package log

import (
	"os"
	"runtime"

	//"github.com/fengleng/go-common/file_cfg"
	"io"
)

type OutputType int

const (
	OutputTypeStd   = iota + 1
	OutputTypeFile  = iota + 1
	OutputTypeQueue = iota + 1
)

type logCfg struct {
	logOutputType OutputType
	logLevel      Level

	wc             io.WriteCloser
	logChannelSize int

	dir        string
	filePrefix string
	seq        string

	service string
	skip    int
}

var (
	StdLog Logger
	Log    Logger
)

var (
	dir           string
	seq           string
	defaultLogCfg *logCfg
)

func init() {
	if runtime.GOOS == "windows" {
		dir = `c:\fengleng\log`
		seq = `\`
	} else {
		dir = `/etc/fengleng/log`
		seq = `/`
	}
	defaultLogCfg = &logCfg{
		logOutputType:  OutputTypeStd,
		wc:             os.Stdout,
		dir:            dir,
		filePrefix:     "",
		service:        "XLOG",
		logLevel:       INFO,
		logChannelSize: 50000,
		seq:            seq,
		skip:           defaultSkip,
	}
	StdLog = NewConsoleLogger(CfgOptionSkip(3))
	Log = NewFileLogger(CfgOptionSkip(3))
	//StdLog = NewConsoleLogger()
	//Log = NewFileLogger()
}

type CfgOption func(opt *logCfg)

func OutPutCfgOption(t OutputType) CfgOption {
	return func(opt *logCfg) {
		opt.logOutputType = t
	}
}

func LogLevelCfgOption(l Level) CfgOption {
	return func(opt *logCfg) {
		opt.logLevel = l
	}
}

func CfgOptionWC(wc io.WriteCloser) CfgOption {
	return func(opt *logCfg) {
		opt.wc = wc
	}
}

func CfgOptionDir(dir string) CfgOption {
	return func(opt *logCfg) {
		opt.dir = dir
	}
}

func CfgOptionFilePrefix(filePrefix string) CfgOption {
	return func(opt *logCfg) {
		opt.filePrefix = filePrefix
	}
}

func CfgOptionService(service string) CfgOption {
	return func(opt *logCfg) {
		opt.service = service
	}
}

func CfgOptionChannelSize(logChannelSize int) CfgOption {
	return func(opt *logCfg) {
		opt.logChannelSize = logChannelSize
	}
}

func CfgOptionSkip(skip int) CfgOption {
	return func(opt *logCfg) {
		opt.skip = skip
	}
}
