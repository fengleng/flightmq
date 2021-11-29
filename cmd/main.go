package main

import (
	"github.com/fengleng/flightmq/config"
	"github.com/fengleng/flightmq/log"
	"github.com/fengleng/flightmq/server"
)

func main() {

	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatal("init cfg err %v", err)
		return
	}

	srv := server.NewServer(cfg)

	err = srv.Run()
	if err != nil {
		log.Fatal("init cfg err %v", err)
		return
	}

}
