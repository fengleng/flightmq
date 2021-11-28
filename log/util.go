package log

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

func getRuntimeInfo(skip int) (function, filename string, lineno int) {
	function = "???"
	pc, filename, lineno, ok := runtime.Caller(skip)
	if ok {
		function = runtime.FuncForPC(pc).Name()
		index := strings.LastIndex(function, ".")
		if index >= 0 {
			function = function[index:]
		}
	}
	return
}

func formatValue(format string, a ...interface{}) (result *string) {
	if len(a) == 0 {
		result = &format
		return
	}

	result1 := fmt.Sprintf(format, a...)
	return &result1
	//return
}

func formatLineInfo(runtime bool, functionName, filename, logText string, lineno int) string {
	var buffer bytes.Buffer
	if runtime {
		buffer.WriteString("[")
		buffer.WriteString(functionName)
		buffer.WriteString(":")

		buffer.WriteString(filename)
		buffer.WriteString(":")

		buffer.WriteString(strconv.FormatInt(int64(lineno), 10))
		buffer.WriteString("] ")
	}
	buffer.WriteString(logText)

	return buffer.String()
}

func newError(format string, a ...interface{}) error {
	err := fmt.Sprintf(format, a...)
	pc, file, line, ok := runtime.Caller(1)
	if !ok {
		return errors.New(err)
	}

	function := runtime.FuncForPC(pc).Name()
	msg := fmt.Sprintf("%s func:%s file:%s line:%d",
		err, function, file, line)
	return errors.New(msg)
}
