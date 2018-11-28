package service

import (
	"context"
	"io"

	"github.com/overvenus/tidbongoogle/pkg/googleutil"
	"github.com/pingcap/kvproto/pkg/enginepb"
	log "github.com/sirupsen/logrus"
)

// Config is Config of the engine service.
type Config struct {
	// The ID of a drive fold that store all region data.
	DriveRootID string `toml:"drive-root-id"`
	// Google config
	Google googleutil.Config `toml:"google"`
}

type srv struct {
	ctx        context.Context
	cfg        *Config
	regionCh   chan uint64
	cmdBatchCh chan *enginepb.CommandRequestBatch
}

// CreateEngineService creats an engine service.
func CreateEngineService(ctx context.Context, cfg *Config) enginepb.EngineServer {
	s := new(srv)
	s.ctx = ctx
	s.cfg = cfg
	s.regionCh = make(chan uint64, 8)
	s.cmdBatchCh = make(chan *enginepb.CommandRequestBatch, 8)
	s.start()
	return s
}

func (s *srv) start() error {
	go func() {
		for {
			select {
			case regionID := <-s.regionCh:
				// TODO: craete a dir in google drive
				_ = regionID
			case batch := <-s.cmdBatchCh:
				for _, cmd := range batch.Requests {
					header := cmd.GetHeader()
					regionID := header.RegionId
					_ = regionID
					// TODO: post cmd to its dir.
				}
			}
		}
	}()
	return nil
}

func (s *srv) ApplyCommandBatch(cmds enginepb.Engine_ApplyCommandBatchServer) error {
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
		s.cmdBatchCh <- batch
	}
}

func (s *srv) ApplySnapshot(snaps enginepb.Engine_ApplySnapshotServer) error {
	var state *enginepb.SnapshotState
	data := make([]*enginepb.SnapshotData, 0, 32)
	for {
		chunk, err := snaps.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Fatalf("recv snapshot chunk error: %v", err)
		}
		if state == nil {
			state = chunk.GetState()
		} else {
			data = append(data, chunk.GetData())
		}
	}

	// TODO: Save state and data to google drive.

	s.regionCh <- state.GetRegion().GetId()
	snaps.SendAndClose(new(enginepb.SnapshotDone))
	return nil
}
