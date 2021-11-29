package log

import (
	"os"
	"syscall"
	"testing"
)

func TestT1(t *testing.T) {
	m := syscall.Umask(0)

	defer syscall.Umask(m)
	err := os.MkdirAll("/etc/fengleng/log", os.ModePerm)
	t.Log(err)
}
