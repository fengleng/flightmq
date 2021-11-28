package log

import (
	"testing"
	"time"
)

func TestT1(t *testing.T) {
	StdLog = NewConsoleLogger(CfgOptionSkip(3), LogLevelCfgOption(DEBUG))
	log.Debug("fsdfsd")
	log.Info("fsdfsd")
	log.Warn("fsdfsd")
	log.Error("fsdfsd")
	log.Fatal("fsdfsd")
	time.Sleep(10 * time.Second)
}
