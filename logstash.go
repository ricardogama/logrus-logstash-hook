package logrus_logstash

import (
	"errors"
	"io"
	"net"

	"github.com/Sirupsen/logrus"
)

// Hook represents a Logstash hook
type Hook struct {
	conn      io.Writer
	ctx       logrus.Fields
	formatter logrus.Formatter
}

// Options represents multiple options to setup the Logstash hook
type Options struct {
	Conn     io.Writer
	Protocol string
	Address  string

	AppName string

	Fields logrus.Fields

	Formatter logrus.Formatter
}

func validOptions(opts Options) error {
	if opts.Conn == nil && (opts.Address == "" || opts.Protocol == "") {
		return errors.New("Missing connection details")
	}
	if opts.Conn != nil && (opts.Address != "" || opts.Protocol != "") {
		return errors.New("Specify Conn or Address and Protocol")
	}
	if opts.AppName != "" && opts.Formatter != nil {
		return errors.New("Specify AppName or Formatter")
	}
	return nil
}

// New sets a new Logrus-Logstash hook from a given options.
// Using Options you can specify the way the hook behaves
//
// For example to use a different socket implementation:
//
// conn, _ := mySocket.Dial("tcp://logstash.host.com")
// opts := logrus_logstash.Options{Conn: conn}
// hook, _ := logrus_logstash.New(opts)
//
// Another example is to set the way the entry is formatted before it is sent to logstash:
//
// type myFormatter struct {
// }
//
// func (f *myFormatter) Format(e *logrus.Entry) ([]byte, error) {
// ...
// }
//
// opts := logrus_logstash.Options{Address: "localhost:9090", Protocol: "tcp", Formatter: &myFormatter{}}
// hook, _ := logrus_logstash.New(opts)
func New(opts Options) (*Hook, error) {
	if err := validOptions(opts); err != nil {
		return nil, err
	}

	conn := opts.Conn
	ctx := opts.Fields
	frmtr := opts.Formatter
	appName := opts.AppName

	if opts.Conn == nil {
		var err error
		conn, err = net.Dial(opts.Protocol, opts.Address)
		if err != nil {
			return nil, err
		}
	}

	if appName != "" {
		frmtr = defaultFormatter(appName, "1")
	}

	if ctx == nil {
		ctx = logrus.Fields{}
	}

	return &Hook{
		conn:      conn,
		ctx:       ctx,
		formatter: frmtr,
	}, nil
}

func (h *Hook) Fire(entry *logrus.Entry) error {
	// add missing context into entry's data
	for k, v := range h.ctx {
		if _, ok := entry.Data[k]; !ok {
			entry.Data[k] = v
		}
	}

	var (
		dataBytes []byte
		err       error
	)
	dataBytes, err = h.formatter.Format(entry)
	if err != nil {
		return err
	}
	if _, err = h.conn.Write(dataBytes); err != nil {
		return err
	}
	return nil
}

func (h *Hook) Levels() []logrus.Level {
	return []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
}
