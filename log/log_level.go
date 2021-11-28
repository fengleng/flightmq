package log

type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
	FATAL
)

var levelMap = map[string]Level{
	"DEBUG": DEBUG,
	"INFO":  INFO,
	"WARN":  WARN,
	"ERROR": ERROR,
	"FATAL": FATAL,
}

func GetLogLevel(s string) Level {
	level, ok := levelMap[s]
	if ok {
		return level
	}
	return INFO
}

func (l Level) String() string {
	levelStr := "INFO"
	switch l {
	case DEBUG:
		levelStr = "DBG"
	case INFO:
		levelStr = "INF"
	case WARN:
		levelStr = "WAR"
	case ERROR:
		levelStr = "ERR"
	case FATAL:
		levelStr = "FAT"
	}
	return levelStr
}
