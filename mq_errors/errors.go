package mq_errors

import (
	"github.com/pingcap/errors"
)

var (
	ErrTtrExceed           = errors.New("msgTTR can't greater than 60.")
	ErrNodeIdExceed        = errors.New("nodeId must be between 1 and 1024.")
	ErrLogLevelExceed      = errors.New("logLevel must be between 0 and 4.")
	ErrMsgMaxPushNumExceed = errors.New("MsgMaxPushNum must be between 1 and 1000.")
	ErrMqIsRunning         = errors.New("mq is running")
	ErrMqLoadingRunning    = errors.New("mq loading running fail")
	ErrQueueClosed         = errors.New("queue has been closed.")
)
