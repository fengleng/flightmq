package log

func NewConsoleLogger(opts ...CfgOption) Logger {
	cfg := defaultLogCfg
	for _, f := range opts {
		f(cfg)
	}
	cfg.logOutputType = OutputTypeStd
	return newXLog(cfg)
}
