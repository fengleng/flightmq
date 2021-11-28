package log

import (
	"github.com/fengleng/go-common/fileutil"
	"os"
)

func NewFileLogger(opts ...CfgOption) Logger {
	cfg := defaultLogCfg

	//cfg := &dcfg
	//fmt.Println(dir)

	for _, f := range opts {
		f(cfg)
	}
	cfg.logOutputType = OutputTypeFile

	if !fileutil.FileExists(cfg.dir) {
		err := os.MkdirAll(cfg.dir, 0666)
		if err != nil {
			logger.Error("%v", err)
			panic(err)
		}
		panic(err)
	}

	return newXLog(cfg)
}
