package service

import (
	"context"
	"io"

	"github.com/overvenus/tidbongoogle/pkg/googleutil"
	"github.com/overvenus/tidbongoogle/pkg/util"
	"github.com/pingcap/kvproto/pkg/enginepb"
	log "github.com/sirupsen/logrus"
)

// Config is Config of the engine service.
type Config struct {
	// Report interval
	ReportInterval util.Duration `toml:"report-interval"`

	// The ID of a drive fold that store all region data.
	DriveRootID string `toml:"drive-root-id"`
	// Google config
	Google googleutil.Config `toml:"google"`
}

type srv struct {
	ctx       context.Context
	app       *Applier
	cmdReqCh  chan<- *enginepb.CommandRequestBatch
	cmdRespCh <-chan *enginepb.CommandResponseBatch
}

// CreateEngineService creats an engine service.
func CreateEngineService(ctx context.Context, applier *Applier) enginepb.EngineServer {
	s := new(srv)
	s.ctx = ctx
	s.app = applier
	s.cmdReqCh = s.app.RequestBatchChannel()
	s.cmdRespCh = s.app.ResponseBatchChannel()
	return s
}

func (s *srv) ApplyCommandBatch(cmds enginepb.Engine_ApplyCommandBatchServer) error {
	actx, cancel := context.WithCancel(s.ctx)
	defer cancel()

	go func() {
		for {
			select {
			case resp := <-s.cmdRespCh:
				// Forward response from applier to gRPC peer.
				log.Infof("sending apply responses")
				cmds.Send(resp)
				log.Infof("sent apply responses")
			case <-actx.Done():
				log.Infof("close server send half")
				return
			}
		}
	}()
	for {
		batch, err := cmds.Recv()
		if err != nil {
			if err == io.EOF {
				log.Infof("ApplyCommandBatch RPC finish")
				return nil
			}
			log.Errorf("recv commands batch error: %v", err)
			return err
		}
		// Send request batch to applier.
		log.Infof("scheduling apply requests")
		s.cmdReqCh <- batch
		log.Infof("scheduled apply requests")
	}
}

func (s *srv) ApplySnapshot(snaps enginepb.Engine_ApplySnapshotServer) error {
	snap := s.app.NewSnapper()
	var state *enginepb.SnapshotState
	var regionID uint64
	for {
		chunk, err := snaps.Recv()
		if err != nil {
			if err == io.EOF {
				// Okay, we have all snapshot data.
				break
			}
			log.Fatalf("recv snapshot chunk error: %v", err)
		}
		if state == nil {
			state = chunk.GetState()
			regionID = state.Region.Id
			err = snap.HandleSnapshotState(state)
			if err != nil {
				log.Errorf("[region %d] fail to apply snapshot state", regionID)
			}
		} else {
			err = snap.HandleSnapshotData(chunk.GetData())
			if err != nil {
				log.Errorf("[region %d] fail to apply snapshot data", regionID)
			}
		}
	}

	done := snap.Done()
	snaps.SendAndClose(done)
	return nil
}
