package main

import (
	"context"
	"flag"
	"math/rand"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/overvenus/tidbongoogle/pkg/googleutil"

	"github.com/BurntSushi/toml"
	"github.com/overvenus/tidbongoogle/pkg/service"
	"github.com/pingcap/kvproto/pkg/enginepb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	addr      = flag.String("addr", "0.0.0.0:3930", "Address of the server")
	pprofAddr = flag.String("pprof", "0.0.0.0:6060", "Pprof address")
	cfgFile   = flag.String("cfg", "conf/config.toml", "config file")
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func main() {
	flag.Parse()

	cfg := new(service.Config)
	if _, err := toml.DecodeFile(*cfgFile, cfg); err != nil {
		log.Fatalf("fail to parse config file %v", err)
	}
	log.Infof("Config: %#v", *cfg)

	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	googleutil.Initsheet(&cfg.Google)
	googleutil.InitGClient(googleutil.NewDriveClient(&cfg.Google, cfg.DriveRootID, 5))

	go func() {
		s := <-sigs
		log.Infof("capture a signal %s, quit ...", s)
		cancel()
	}()

	go func() {
		log.Infof("pprof at %s", *pprofAddr)
		http.ListenAndServe(*pprofAddr, nil)
	}()

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	go func() {
		sctx, cancel := context.WithCancel(ctx)
		defer cancel()

		applier := service.NewApplier(sctx, cfg)
		svc := service.CreateEngineService(sctx, applier)
		enginepb.RegisterEngineServer(s, svc)

		log.Infof("create gRPC server, listens on %s", *addr)
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	<-ctx.Done()
	log.Infof("shutdown grpc server ...")
	log.Infof("Bye")
}
