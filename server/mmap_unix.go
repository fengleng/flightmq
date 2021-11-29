//go:build linux
// +build linux

package server

import (
	"github.com/fengleng/flightmq/log"
	"github.com/pingcap/errors"
	"syscall"
)

func mMap(fd uintptr, size int64) ([]byte, error) {
	data, err := syscall.Mmap(int(fd), 0, defaultMemMapSize, syscall.PROT_WRITE|syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		log.Error("err:%v", err)
		return nil, err
	}
	return data, nil
}

func munMap(data []byte) error {
	err := syscall.Munmap(data)
	if err != nil {
		return errors.Annotatef(err, "unmap  failed.")
	} else {
		return nil
	}
}
