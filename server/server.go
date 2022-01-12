package server

import (
	"github.com/fengleng/flightmq/common"
	"github.com/fengleng/flightmq/config"
	"github.com/fengleng/flightmq/log"
	"github.com/fengleng/flightmq/mq_errors"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
)

type Server struct {
	version  string
	running  int32
	exitChan chan struct{}
	wg       common.WaitGroupWrapper
	cfg      *config.Config

	Logger log.Logger
	//ctx      *Context
	//etcd     etcd
}

func NewServer(cfg *config.Config) *Server {
	return &Server{
		cfg:      cfg,
		version:  "2.0",
		exitChan: make(chan struct{}),
	}
}

func (s *Server) Run() error {
	if atomic.LoadInt32(&s.running) == 1 {
		return mq_errors.ErrMqIsRunning
	}

	if !atomic.CompareAndSwapInt32(&s.running, 0, 1) {
		return mq_errors.ErrMqLoadingRunning
	}

	dispatcher := NewDispatcher(s.cfg)
	dispatcher.srv = s
	s.wg.Wrap(dispatcher.Run)

	tcpServer := NewTcpServ(s.cfg)
	tcpServer.dispatcher = dispatcher
	tcpServer.srv = s
	s.wg.Wrap(tcpServer.Run)

	return nil
}

func (s *Server) Exit() {
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT)
	<-sc
	close(s.exitChan)
	s.wg.Wait()
}
