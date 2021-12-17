package config

import (
	"flag"
	"fmt"
	"github.com/fengleng/flightmq/common"
	"github.com/fengleng/flightmq/log"
	"github.com/fengleng/flightmq/mq_errors"
	"gopkg.in/ini.v1"
	"os"
	"strings"
)

type Config struct {
	// node
	NodeId            int
	NodeWeight        int
	MsgTTR            int
	MsgMaxRetry       int
	MsgMaxPushNum     int
	DataSavePath      string
	EnableCluster     bool
	HeartbeatInterval int

	// RegisterAddr
	RegisterAddr string

	// etcd
	EtcdEndPoints []string

	// http server
	HttpServAddr      string
	HttpServEnableTls bool
	HttpServCertFile  string
	HttpServKeyFile   string

	// tcp server
	TcpServAddr      string
	TcpServEnableTls bool
	TcpServCertFile  string
	TcpServKeyFile   string

	// report addr
	ReportHttpAddr string
	ReportTcpAddr  string

	// log
	LogTargetType string
	LogFilename   string
	LogLevel      int
	LogMaxSize    int
	LogRotate     bool
}

func NewConfig() (*Config, error) {
	var err error
	var cfg *Config

	// specify config file
	cfgFile := flag.String("config_file", "", "config file")
	if len(*cfgFile) > 0 {
		cfg, err = LoadConfigFromFile(*cfgFile)
		if err != nil {
			log.Fatal("load config file %v error, %v\n", *cfgFile, err)
			return nil, err
		}
	} else {
		cfg = new(Config)
	}

	// command options
	var endpoints string
	flag.StringVar(&endpoints, "etcd_endpoints", cfg.TcpServAddr, "etcd endpoints")
	flag.StringVar(&cfg.TcpServAddr, "tcp_addr", cfg.TcpServAddr, "tcp address")
	flag.StringVar(&cfg.RegisterAddr, "register_addr", cfg.RegisterAddr, "register address")
	flag.StringVar(&cfg.HttpServAddr, "http_addr", cfg.HttpServAddr, "http address")
	flag.StringVar(&cfg.ReportTcpAddr, "report_tcp_addr", cfg.ReportTcpAddr, "report tcp address")
	flag.StringVar(&cfg.ReportHttpAddr, "report_http_addr", cfg.ReportHttpAddr, "report http address")
	flag.IntVar(&cfg.NodeId, "node_id", cfg.NodeId, "node unique id")
	flag.IntVar(&cfg.NodeWeight, "node_weight", cfg.NodeWeight, "node weight")
	flag.IntVar(&cfg.MsgTTR, "msg_ttr", cfg.MsgTTR, "msg ttr")
	flag.IntVar(&cfg.MsgMaxRetry, "msg_max_retry", cfg.MsgMaxRetry, "msg max retry")
	flag.StringVar(&cfg.DataSavePath, "data_save_path", cfg.DataSavePath, "data save path")
	flag.IntVar(&cfg.LogLevel, "log_level", cfg.LogLevel, "log level,such as: 0,error 1,warn 2,info 3,trace 4,debug")
	flag.Parse()

	// parse etcd endpoints
	if len(endpoints) > 0 {
		cfg.EtcdEndPoints = strings.Split(endpoints, ",")
	}

	cfg.SetDefault()

	if err := cfg.Validate(); err != nil {
		log.Fatal("config file %v error, %v\n", *cfgFile, err)
		return nil, err
	}

	exists, err := common.PathExists(cfg.DataSavePath)
	if err != nil {
		log.Fatal("err:%v", err)
		return nil, err
	}
	if !exists {
		if err := os.MkdirAll(cfg.DataSavePath, os.ModePerm); err != nil {
			log.Fatal("err:%v", err)
			return nil, err
		}
	}

	return cfg, nil
}

func LoadConfigFromFile(cfgFile string) (*Config, error) {
	if res, err := common.PathExists(cfgFile); !res {
		if err != nil {
			return nil, fmt.Errorf("config file %s is error, %s \n", cfgFile, err)
		} else {
			return nil, fmt.Errorf("config file %s is not exists \n", cfgFile)
		}
	}

	c, err := ini.Load(cfgFile)
	if err != nil {
		return nil, fmt.Errorf("load config file %v failed, %v \n", cfgFile, err)
	}

	cfg := new(Config)

	// node
	cfg.NodeId, _ = c.Section("node").Key("id").Int()
	cfg.NodeWeight, _ = c.Section("node").Key("weight").Int()
	cfg.MsgTTR, _ = c.Section("node").Key("msgTTR").Int()
	cfg.MsgMaxRetry, _ = c.Section("node").Key("msgMaxRetry").Int()
	cfg.ReportTcpAddr = c.Section("node").Key("reportTcpaddr").String()
	cfg.ReportHttpAddr = c.Section("node").Key("reportHttpaddr").String()
	cfg.DataSavePath = c.Section("node").Key("dataSavePath").String()

	// log config
	cfg.LogFilename = c.Section("log").Key("filename").String()
	cfg.LogLevel, _ = c.Section("log").Key("level").Int()
	cfg.LogRotate, _ = c.Section("log").Key("rotate").Bool()
	cfg.LogMaxSize, _ = c.Section("log").Key("max_size").Int()
	cfg.LogTargetType = c.Section("log").Key("target_type").String()

	// http server config
	cfg.HttpServAddr = c.Section("http_server").Key("addr").String()
	cfg.HttpServCertFile = c.Section("http_server").Key("certFile").String()
	cfg.HttpServKeyFile = c.Section("http_server").Key("keyFile").String()
	cfg.HttpServEnableTls, _ = c.Section("http_server").Key("enableTls").Bool()

	// tcp server config
	cfg.TcpServAddr = c.Section("tcp_server").Key("addr").String()
	cfg.TcpServCertFile = c.Section("tcp_server").Key("certFile").String()
	cfg.TcpServKeyFile = c.Section("tcp_server").Key("keyFile").String()
	cfg.TcpServEnableTls, _ = c.Section("tcp_server").Key("enableTls").Bool()

	// register config
	cfg.RegisterAddr = c.Section("gregister").Key("addr").String()

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.MsgTTR > 60 {
		return mq_errors.ErrTtrExceed
	}
	if c.NodeId > 1024 || c.NodeId < 0 {
		return mq_errors.ErrNodeIdExceed
	}
	if c.LogLevel > 4 || c.LogLevel < 0 {
		return mq_errors.ErrLogLevelExceed
	}
	if c.MsgMaxPushNum > 1000 || c.MsgMaxPushNum <= 0 {
		return mq_errors.ErrMsgMaxPushNumExceed
	}
	exists, err := common.PathExists(c.DataSavePath)
	if err != nil {
		log.Fatal("err:%v", err)
		return err
	}
	if !exists {
		if err := os.MkdirAll(c.DataSavePath, os.ModePerm); err != nil {
			log.Fatal("err:%v", err)
			return err
		}
	}
	return nil
}

func (c *Config) SetDefault() {
	if c.NodeId == 0 {
		c.NodeId = 1
	}
	if c.NodeWeight == 0 {
		c.NodeWeight = 1
	}
	if c.MsgTTR == 0 {
		c.MsgTTR = 30
	}
	if c.MsgMaxRetry == 0 {
		c.MsgMaxRetry = 5
	}
	if c.MsgMaxPushNum == 0 {
		c.MsgMaxPushNum = 1000
	}
	if c.HeartbeatInterval <= 0 {
		c.HeartbeatInterval = 5
	}

	// etcd
	if len(c.EtcdEndPoints) == 0 {
		c.EtcdEndPoints = append(c.EtcdEndPoints, "127.0.0.1:2379")
	}

	// 数据存储目录,相对于命令执行所在目录,例如在/home执行启动命令,将会生成/home/data目录
	if len(c.DataSavePath) == 0 {
		c.DataSavePath = "data/flightMq"
	}

	// log default config
	if len(c.LogFilename) == 0 {
		c.LogFilename = "flightMq.log"
	}
	if c.LogLevel <= 0 {
		c.LogLevel = 4
	}
	if c.LogMaxSize <= 5000000 {
		c.LogMaxSize = 5000000
	}
	if len(c.LogTargetType) == 0 {
		c.LogTargetType = "console"
	}

	// server default config
	if len(c.HttpServAddr) == 0 {
		c.HttpServAddr = "127.0.0.1:9504"
	}
	if len(c.TcpServAddr) == 0 {
		c.TcpServAddr = "127.0.0.1:9503"
	}

	// gresiger default config
	if len(c.RegisterAddr) == 0 {
		c.RegisterAddr = "http://127.0.0.1:9595"
	}

	if len(c.ReportHttpAddr) == 0 {
		c.ReportHttpAddr = c.HttpServAddr
	}
	if len(c.ReportTcpAddr) == 0 {
		c.ReportTcpAddr = c.TcpServAddr
	}
}
