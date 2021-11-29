package server

import (
	"bufio"
	"fmt"
	"github.com/fengleng/flightmq/common"
	"github.com/fengleng/flightmq/config"
	"github.com/fengleng/flightmq/log"
	"net"
	"sync"
)

type TcpServ struct {
	wg       common.WaitGroupWrapper
	mux      sync.RWMutex
	exitChan chan struct{}

	cfg    *config.Config
	logger log.Logger

	srv        *Server
	dispatcher *Dispatcher
}

func NewTcpServ(cfg *config.Config) *TcpServ {
	return &TcpServ{
		exitChan: make(chan struct{}),
		cfg:      cfg,
	}
}

func (s *TcpServ) Run() {
	defer func() {
		s.wg.Wait()
		s.logger.Info("tcp server exit.")
	}()

	addr := s.cfg.TcpServAddr
	s.logger.Info(fmt.Sprintf("tcp server(%s) is running.", addr))

	listen, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal("%v", err)
	}

	go func() {
		select {
		case <-s.srv.exitChan:
			close(s.exitChan)
		case <-s.exitChan:
		}

		listen.Close()
	}()

	for {
		conn, err := listen.Accept()
		if err != nil {
			s.logger.Error("%v", err)
			break
		}

		tcpConn := &TcpConn{
			conn:     conn,
			serv:     s,
			exitChan: make(chan struct{}),
			reader:   bufio.NewReaderSize(conn, 16*1024),
			writer:   bufio.NewWriterSize(conn, 16*1024),
		}

		s.wg.Wrap(tcpConn.Handle)
	}
}
