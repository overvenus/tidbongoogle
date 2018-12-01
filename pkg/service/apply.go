package service

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	drive "google.golang.org/api/drive/v3"

	"github.com/overvenus/tidbongoogle/pkg/codec"
	"github.com/overvenus/tidbongoogle/pkg/googleutil"
	"github.com/overvenus/tidbongoogle/pkg/sheet"
	"github.com/pingcap/kvproto/pkg/enginepb"
	"github.com/pingcap/kvproto/pkg/raft_cmdpb"
	"github.com/pingcap/kvproto/pkg/raft_serverpb"
	log "github.com/sirupsen/logrus"
)

const raftInitLogTerm = uint64(5)
const raftInitLogIndex = uint64(5)

type snapshot struct {
	regionID     uint64
	peerID       uint64
	appliedIndex uint64
	appliedTerm  uint64
	// root folder id of a region
	regionFolderID string
}

type regionStore struct {
	// region id
	id uint64
	// peer id
	peerID uint64
	// root folder id of a region
	regionFolderID string
	// snapshot folder id of a region
	snapFolderID string

	// applied (saved) index
	appiledIndex uint64
	appliedTerm  uint64
}

func (store *regionStore) advance(appliedIndex, appliedTerm uint64) {
	log.Infof("[region %d] advance applied term to %d, applied index to %d",
		store.id, appliedTerm, appliedIndex)
	if store.appiledIndex > appliedIndex {
		log.Errorf("[region %d] advance backward, term from %d to %d, index from %d to %d",
			store.id, store.appliedTerm, appliedTerm, store.appiledIndex, appliedIndex)
	}
	store.appiledIndex = appliedIndex
	store.appliedTerm = appliedTerm
}

func (store *regionStore) makeResponse() *enginepb.CommandResponse {
	log.Infof("[region %d] make response with applied term %d, applied index %d",
		store.id, store.appliedTerm, store.appiledIndex)

	return &enginepb.CommandResponse{
		Header: &enginepb.CommandResponseHeader{
			RegionId: store.id,
		},
		ApplyState: &raft_serverpb.RaftApplyState{
			AppliedIndex: store.appiledIndex,
			// TOOD: for now, we just ignore truncate state.
		},
		AppliedTerm: store.appliedTerm,
	}
}

// Applier save raft log to google drive.
type Applier struct {
	ctx context.Context
	cfg *Config

	// region id -> region store
	regions map[uint64]*regionStore

	snapCh    chan *snapshot
	cmdReqCh  chan *enginepb.CommandRequestBatch
	cmdRespCh chan *enginepb.CommandResponseBatch

	decoder *sheet.Decoder
}

// NewApplier creates an Applier.
func NewApplier(ctx context.Context, cfg *Config) *Applier {
	app := new(Applier)
	app.ctx = ctx
	app.cfg = cfg
	app.regions = make(map[uint64]*regionStore)
	app.snapCh = make(chan *snapshot, 10)
	app.cmdReqCh = make(chan *enginepb.CommandRequestBatch, 10)
	app.cmdRespCh = make(chan *enginepb.CommandResponseBatch, 10)
	app.decoder = sheet.NewDecoder()
	app.start()
	return app
}

// ResponseBatchChannel returns a channel that outputs apply response.
func (app *Applier) ResponseBatchChannel() <-chan *enginepb.CommandResponseBatch {
	return app.cmdRespCh
}

// RequestBatchChannel returns a channel that accepts apply request.
func (app *Applier) RequestBatchChannel() chan<- *enginepb.CommandRequestBatch {
	return app.cmdReqCh
}

// start the applier.
func (app *Applier) start() error {
	log.Info("starting applier ...")
	driveCli := googleutil.NewDriveClient(
		&app.cfg.Google, app.cfg.DriveRootID, app.cfg.MaxRetry)
	if err := app.restore(driveCli); err != nil {
		return err
	}
	app.decoder.Do()
	app.decoder.AutoSync()
	go app.apply(driveCli)
	log.Info("starting applier done")
	return nil
}

func (app *Applier) apply(driveCli *googleutil.DriveClient) {
	ticker := time.NewTicker(app.cfg.ReportInterval.Duration)

	for {
		select {
		case <-app.ctx.Done():
			log.Infof("applier quit ...")
			return
		case <-ticker.C:
			log.Infof("time to report applied state")
			if len(app.regions) == 0 {
				continue
			}
			respBatch := &enginepb.CommandResponseBatch{
				Responses: make([]*enginepb.CommandResponse, 0, len(app.regions)),
			}
			for _, store := range app.regions {
				resp := store.makeResponse()
				respBatch.Responses = append(respBatch.Responses, resp)
			}
			app.cmdRespCh <- respBatch

		case snap := <-app.snapCh:
			// We have created a region folder in google drive,
			// then we add the region to regions map.
			store := regionStore{
				// region id
				id: snap.regionID,
				// peer id
				peerID: snap.peerID,
				// root folder id of a region
				regionFolderID: snap.regionFolderID,
			}
			// applied (saved) index
			store.advance(snap.appliedIndex, snap.appliedTerm)
			app.regions[snap.regionID] = &store

		case batch := <-app.cmdReqCh:
			respBatch := &enginepb.CommandResponseBatch{
				Responses: make([]*enginepb.CommandResponse, 0, len(batch.Requests)),
			}
			for _, cmd := range batch.Requests {
				header := cmd.GetHeader()
				regionID := header.RegionId
				term := header.Term
				index := header.Index
				log.Infof(
					"[region %d] try apply raft log at index %d term %d ...",
					regionID, index, term)

				store, ok := app.regions[regionID]
				if !ok {
					log.Errorf("[region %d] region store not found", regionID)
					continue
				}
				// support region split
				if cmd.AdminRequest != nil {
					switch cmd.AdminRequest.CmdType {
					case raft_cmdpb.AdminCmdType_BatchSplit:
						app.handleSplits(driveCli, regionID,
							cmd.AdminRequest.Splits, cmd.AdminResponse.Splits)
					default:
					}
				}

				b, err := proto.Marshal(cmd)
				log.Infof("[region %d] log at term %d index %d size %d",
					regionID, term, index, len(b))
				if err != nil {
					log.Warnf("[region %d] fail to marshal snapshop state", err)
				}

				app.decoder.Decode(b)

				cmdName := codec.EncodeRaftLog(term, index)
				logFile, err := driveCli.CreateFile(
					cmdName, store.regionFolderID, b)
				if err != nil {
					log.Errorf(
						"[region %d] fail to save raft log at index %d term %d",
						regionID, index, term)
				} else {
					log.Infof(
						"[region %d] raft log at index %d term %d applied, ID: %s",
						regionID, index, term, logFile.Id)
				}
				store.advance(index, term)

				resp := store.makeResponse()
				respBatch.Responses = append(respBatch.Responses, resp)

			}
			app.cmdRespCh <- respBatch
		}
	}
}

func (app *Applier) handleSplits(
	driveCli *googleutil.DriveClient,
	originalRegionID uint64,
	req *raft_cmdpb.BatchSplitRequest, resp *raft_cmdpb.BatchSplitResponse,
) error {
	// TODO: create folders then update mem state.
	log.Infof(
		"[region %d] exec %v, new regions %+v",
		originalRegionID, req, resp.Regions,
	)
	for _, region := range resp.Regions {
		if region.Id == originalRegionID {
			log.Infof(
				"[region %d] split skip original region %+v",
				originalRegionID, region,
			)
			continue
		}
		var peerID uint64
		for _, pr := range region.Peers {
			// HACK: all peers in the this store are learners.
			if pr.IsLearner {
				peerID = pr.Id
			}
		}
		// Create a region folder.
		regionFolder, snapFolder, err := maybeCreateRegionFolder(
			driveCli, driveCli.Root, region.Id, peerID)
		if err != nil {
			log.Errorf("[region %d] fail to create snap folder", region.Id)
			return err
		}
		store := new(regionStore)
		store.id = region.Id
		store.peerID = peerID
		store.regionFolderID = regionFolder.Id
		store.snapFolderID = snapFolder.Id
		store.advance(raftInitLogIndex, raftInitLogTerm)
		app.regions[region.Id] = store
	}
	return nil
}

// Layout of a region folder:
// ```
//   region_2_3
//   ├── [f]  log_6_7
//   └── [d]  snap_2_3
//       └── [d]  25_25_7
//           ├── [f]  state
//           └── [f]  chunk_1
// ```
func (app *Applier) restore(driveCli *googleutil.DriveClient) error {
	log.Info("restore region apply state ...")
	flist, err := driveCli.ListFolder(app.cfg.DriveRootID, 0)
	if err != nil {
		return err
	}
	for _, f := range flist.Files {
		regionID, peerID, err := codec.DecodeRegionFolder(f.Name)
		if err != nil {
			log.Errorf("fail to restore folder %s", err)
			continue
		}
		store := regionStore{
			id:             regionID,
			peerID:         peerID,
			regionFolderID: f.Id,
		}
		// Restore applied index.
		// Order by name desc, the first one is the snap folder and the second
		// is the latest applied index.
		fl, err := driveCli.ListFileByNameDesc(store.regionFolderID, 2)
		if err != nil {
			log.Errorf("[region %d] fail to list root folder %v", regionID, err)
			return err
		}
		flen := len(fl.Files)
		if flen == 2 {
			// There is a snap folder and some raft logs.
			store.snapFolderID = fl.Files[0].Id
			latestLog := fl.Files[1].Name
			term, index, err := codec.DecodeRaftLog(latestLog)
			if err != nil {
				log.Errorf("[region %d] fail to decode raft log %v", regionID, err)
				return err
			}
			store.advance(index, term)
		} else if flen == 1 {
			// It must be a snap folder.
			store.snapFolderID = fl.Files[0].Id
			// FIXME: sometime we need to restore applied index/term from snapshot.
		} else {
			// Nothing? skip.
			log.Infof("[region %d] empty region folder", regionID)
			continue
		}
		log.Infof("[region %d] restore store %+v", regionID, store)
		app.regions[regionID] = &store
	}
	log.Info("restore region apply state done")
	return nil
}

// Snapper handles snapshot.
type Snapper struct {
	driveRootID string
	driveCli    *googleutil.DriveClient
	snapCh      chan<- *snapshot

	// snapshot data chunk sequence number
	chunkSeq int

	regionID         uint64
	peerID           uint64
	appliedIndex     uint64
	appliedTerm      uint64
	regionFolderID   string
	snapFolderID     string
	snapshotFolderID string // actual snapshot data go here.
}

// NewSnapper create a snapper which handles a snapshot.
func (app *Applier) NewSnapper() *Snapper {
	snapper := new(Snapper)
	snapper.driveRootID = app.cfg.DriveRootID
	snapper.snapCh = app.snapCh
	snapper.driveCli = googleutil.NewDriveClient(
		&app.cfg.Google, app.cfg.DriveRootID, app.cfg.MaxRetry)
	return snapper
}

// HandleSnapshotState handle snapshot state message.
func (snap *Snapper) HandleSnapshotState(state *enginepb.SnapshotState) error {
	snap.regionID = state.Region.Id
	snap.peerID = state.Peer.Id
	snap.appliedIndex = state.ApplyState.TruncatedState.Index
	snap.appliedTerm = state.ApplyState.TruncatedState.Term

	// Create a region folder.
	regionFolder, snapFolder, err := maybeCreateRegionFolder(
		snap.driveCli, snap.driveRootID, snap.regionID, snap.peerID)
	if err != nil {
		log.Errorf("[region %d] fail to create snap folder", snap.regionID)
		return err
	}
	snap.regionFolderID = regionFolder.Id
	snap.snapFolderID = snapFolder.Id

	// 3rd we create a snapshot folder to store the snapshot.
	snapshotFolderName := codec.EncodeSnapshotFolder(
		state.ApplyState.AppliedIndex,
		state.ApplyState.TruncatedState.Index,
		state.ApplyState.TruncatedState.Term)
	snapshotFolder, _, err := snap.driveCli.MaybeCreateFolder(
		snapshotFolderName,
		snapFolder.Id,
	)
	if err != nil {
		log.Errorf("[region %d] fail to create snapshot folder", snap.regionID)
		return err
	}
	snap.snapshotFolderID = snapshotFolder.Id

	// Then we store the snapshot state.
	b, err := proto.Marshal(state)
	if err != nil {
		log.Errorf("[region %d] fail to marshal snapshop state", err)
		return err
	}
	_, err = snap.driveCli.CreateFile(
		"state",
		snap.snapshotFolderID, // Put snapshot state in its snapshot folder.
		b,
	)
	if err != nil {
		log.Errorf("[region %d] fail to save snapshop state", err)
		return err
	}
	return nil
}

// HandleSnapshotData handles snapshot data message.
func (snap *Snapper) HandleSnapshotData(state *enginepb.SnapshotData) error {
	if snap.snapshotFolderID == "" {
		panic(fmt.Sprintf("[region %d] expect a snapshot folder", snap.regionID))
	}
	// Store the snapshot data chunk.
	b, err := proto.Marshal(state)
	if err != nil {
		log.Errorf("[region %d] fail to marshal snapshop data", err)
		return err
	}
	chunkName := fmt.Sprintf("chunk_%d", snap.chunkSeq) // TODO: put it to codec.
	_, err = snap.driveCli.CreateFile(
		chunkName,
		snap.snapshotFolderID, // Put snapshot data in its snapshot folder.
		b,
	)
	if err != nil {
		log.Errorf("[region %d] fail to save snapshop state", err)
		return err
	}
	snap.chunkSeq++
	return nil
}

// Done finish the snapshot.
func (snap *Snapper) Done() *enginepb.SnapshotDone {
	s := snapshot{
		regionID:       snap.regionID,
		peerID:         snap.peerID,
		appliedIndex:   snap.appliedIndex,
		appliedTerm:    snap.appliedTerm,
		regionFolderID: snap.regionFolderID,
	}
	snap.snapCh <- &s
	log.Infof("[region %d] snapshot done, %+v", snap.regionID, snap)
	return new(enginepb.SnapshotDone)
}

// maybeCreateRegionFolder creates a region folder.
func maybeCreateRegionFolder(
	driveCli *googleutil.DriveClient, driveRootID string, regionID uint64, peerID uint64,
) (*drive.File, *drive.File, error) {
	log.Infof("[region %d] start create region folder ...", regionID)

	// First we try to create a new folder for the region.
	regionFolderName := codec.EncodeRegionFolder(regionID, peerID)
	// See if it is already exists.
	regionFolder, _, err := driveCli.MaybeCreateFolder(regionFolderName, driveRootID)
	if err != nil {
		log.Errorf("[region %d] fail to create region folder", regionID)
		return nil, nil, err
	}

	// Second we try to create a folder for save all snapshots.
	snapFolderName := codec.EncodeSnapFolder(regionID, peerID)
	snapFolder, _, err := driveCli.MaybeCreateFolder(
		snapFolderName, regionFolder.Id) // Put snap folder in region folder.
	if err != nil {
		log.Errorf("[region %d] fail to create snap folder", regionID)
		return nil, nil, err
	}

	return regionFolder, snapFolder, nil
}
