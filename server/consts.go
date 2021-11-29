package server

const (
	defaultMemMapSize = 1 << 28
)

const (
	DEFAULT_KEY           = "default"
	DEAD_QUEUE_FLAG       = "dead"
	ROUTE_KEY_MATCH_FULL  = 1
	ROUTE_KEY_MATCH_FUZZY = 2
)

// flag(1-byte) + status(2-bytes) + msg_len(4-bytes) + msg(n-bytes)
const MSG_FIX_LENGTH = 1 + 2 + 4
const GROW_SIZE = 10 * 1024 * 1024
const REWRITE_SIZE = 100 * 1024 * 1024
