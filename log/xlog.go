package log

import (
	"bytes"
	"fmt"
	"github.com/fengleng/go-common/core/hack"
	log2 "log"
	"sync"

	"io"
	"os"
	"path"
	"time"
)

const (
	defaultSkip = 4
)

var (
	defaultSep string
)

type XLog struct {
	level Level

	skip     int
	hostname string
	service  string

	logTextChan chan string
	outputType  OutputType
	wc          io.WriteCloser
	dir         string
	sep         string
	fileName    string
	curHour     int
	curHourTime string

	openLock sync.Mutex
}

func newXLog(cfg *logCfg) *XLog {
	if cfg == nil {
		cfg = defaultLogCfg
	}

	hostname, _ := os.Hostname()

	x := &XLog{
		level:       cfg.logLevel,
		skip:        cfg.skip,
		service:     cfg.service,
		hostname:    hostname,
		logTextChan: make(chan string, cfg.logChannelSize),
		outputType:  cfg.logOutputType,
		dir:         cfg.dir,
		fileName:    cfg.filePrefix,
		sep:         cfg.seq,
		wc:          cfg.wc,
	}
	go x.Flush()
	return x
}

func (x *XLog) Flush() {
	for {
		select {
		case logText := <-x.logTextChan:
			x.write(logText)
		}
	}
}

func (x *XLog) write(logText string) {
	if x.needReopen() {
		x.reopen()
	}
	_, err := x.wc.Write(hack.Slice(logText))
	if err != nil {
		log2.Println(err)
		return
	}
}

func (x *XLog) needReopen() bool {
	return x.outputType == OutputTypeFile && x.curHour != time.Now().Hour()
}

func (x *XLog) reopen() {
	x.openLock.Lock()
	defer x.openLock.Unlock()
	if !x.needReopen() {
		return
	}
	x.curHour = time.Now().Hour()
	x.curHourTime = time.Now().Format("2006010203")
	fName := fmt.Sprintf("%s%s%s%s", x.dir, x.sep, x.fileName, x.curHourTime)
	file, err := os.OpenFile(fName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
	if err != nil {
		log2.Println(err)
		file, err = os.OpenFile(fName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0666)
		if err != nil {
			log2.Println(err)
			panic(err.Error())
		}
	}
	x.wc = file
}

func (x *XLog) Debug(txt string, a ...interface{}) {
	if x.level > DEBUG {
		return
	}
	x.formatLogAndWriteChan(formatValue(txt, a...), DEBUG)
}

func (x *XLog) Info(txt string, a ...interface{}) {
	if x.level > INFO {
		return
	}
	x.formatLogAndWriteChan(formatValue(txt, a...), INFO)
}

func (x *XLog) Warn(txt string, a ...interface{}) {
	if x.level > WARN {
		return
	}
	x.formatLogAndWriteChan(formatValue(txt, a...), WARN)
}

func (x *XLog) Error(txt string, a ...interface{}) {
	if x.level > ERROR {
		return
	}
	x.formatLogAndWriteChan(formatValue(txt, a...), ERROR)
}

func (x *XLog) Fatal(txt string, a ...interface{}) {
	if x.level > FATAL {
		return
	}
	x.formatLogAndWriteChan(formatValue(txt, a...), FATAL)
}

func (x *XLog) formatLogAndWriteChan(body *string, level Level) {
	var buffer bytes.Buffer

	buffer.WriteString("[")
	buffer.WriteString(time.Now().Format("2006-01-02 15-04-05.0000"))
	buffer.WriteString("] ")

	buffer.WriteString("[")
	buffer.WriteString(level.String())
	buffer.WriteString("] ")

	buffer.WriteString("[")
	buffer.WriteString(x.hostname)
	buffer.WriteString("] ")

	buffer.WriteString("[")
	buffer.WriteString(x.service)
	buffer.WriteString("] ")

	f, filename, lineno := getRuntimeInfo(x.skip)
	buffer.WriteString("[")
	buffer.WriteString(fmt.Sprintf("%s:%d%s", path.Base(filename), lineno, f))
	buffer.WriteString("] ")

	buffer.WriteString(*body)
	buffer.WriteString("\n")

	x.logTextChan <- colors[level](buffer.String())
}
