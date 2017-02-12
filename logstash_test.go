package logrus_logstash

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/Sirupsen/logrus"
)

func TestValidOptions(t *testing.T) {
	tt := []struct {
		input Options
		err   error
	}{
		{Options{}, errors.New("Missing connection details")},
		{Options{Conn: &bytes.Buffer{}}, nil},
		{Options{Address: "localhost:8989", Protocol: "udp"}, nil},
		{Options{Address: "localhost:8989"}, errors.New("Missing connection details")},
		{Options{Protocol: "tcp"}, errors.New("Missing connection details")},
		{Options{Conn: &bytes.Buffer{}, Protocol: "tcp"}, errors.New("Specify Conn or Address and Protocol")},
		{Options{Conn: &bytes.Buffer{}, Address: "localhost"}, errors.New("Specify Conn or Address and Protocol")},
		{Options{Conn: &bytes.Buffer{}, AppName: "bla", Formatter: &logrus.JSONFormatter{}}, errors.New("Specify AppName or Formatter")},
	}

	for _, te := range tt {
		if err := validOptions(te.input); !reflect.DeepEqual(err, te.err) {
			t.Errorf("Expected error to be '%s' but got '%s'", te.err, err)
		}
	}
}

func TestNew(t *testing.T) {
	udpConn, err := net.Dial("udp", "localhost:9090")
	if err != nil {
		t.Errorf("Expected udp connection won't fail: %s", err)
	}

	tt := []struct {
		in  Options
		out *Hook
	}{
		{Options{Conn: udpConn, Formatter: &logrus.JSONFormatter{}},
			&Hook{
				conn:      udpConn,
				ctx:       logrus.Fields{},
				formatter: &logrus.JSONFormatter{},
			}},
		{Options{Conn: udpConn, Formatter: &logrus.JSONFormatter{}, Fields: logrus.Fields{"id": "12"}},
			&Hook{
				conn: udpConn,
				ctx: logrus.Fields{
					"id": "12",
				},
				formatter: &logrus.JSONFormatter{},
			},
		},
	}

	for _, te := range tt {
		if hook, err := New(te.in); err != nil || !reflect.DeepEqual(te.out, hook) {
			if err != nil {
				t.Errorf("Expected New to not fail")
			} else {
				t.Errorf("Expected to receive '%#v' but got '%#v'", te.out, hook)
			}
		}
	}
}

func TestNewAddressProtocol(t *testing.T) {
	opts := Options{
		Address:  "172.17.1.1:9090",
		Protocol: "udp",
	}
	h, err := New(opts)
	if err != nil {
		t.Errorf("Expected New not to fail: %s", err)
	}
	v, k := h.conn.(net.Conn)
	if !k {
		t.Error("Expected conn to be net.Conn interface too")
	}
	if v.RemoteAddr().String() != opts.Address {
		t.Errorf("Expected connection to be '%s' but got '%s'", opts.Address, v.RemoteAddr().String())
	}
}

func TestNewErrors(t *testing.T) {
	tt := []struct {
		errMsg string
		opts   Options
	}{
		{"dial tcp 172.17.11.1:9090: getsockopt: no route to host", Options{Address: "172.17.11.1:9090", Protocol: "tcp"}},
		{"Missing connection details", Options{}},
		{"Specify Conn or Address and Protocol", Options{Conn: &bytes.Buffer{}, Protocol: "udp"}},
	}
	for _, te := range tt {
		_, err := New(te.opts)
		if err == nil {
			t.Errorf("Expected to fail")
		}
		if err.Error() != te.errMsg {
			t.Errorf("Expected error message to be '%s' but got '%s'", te.errMsg, err.Error())
		}
	}
}

func TestHookLevels(t *testing.T) {
	h := &Hook{}
	expected := []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	}
	if !reflect.DeepEqual(h.Levels(), expected) {
		t.Errorf("Expected h.Levels return '%v' but got '%v'", expected, h.Levels())
	}
}

func TestHookFire(t *testing.T) {
	tt := []struct {
		buffer io.ReadWriter
		fields logrus.Fields
		app    string

		level   logrus.Level
		message string
		data    logrus.Fields

		expected map[string]interface{}
	}{
		{&bytes.Buffer{}, logrus.Fields{"f1": 1}, "test1", logrus.PanicLevel,
			"hello world", logrus.Fields{"id": "a1"},
			map[string]interface{}{"id": "a1", "f1": 1.0, "@message": "hello world",
				"@level": "panic", "@timestamp": time.Time{}.Format(time.RFC3339),
				"type": "test1", "@version": "1"}},
		{&bytes.Buffer{}, logrus.Fields{"f1": 1}, "test2", logrus.InfoLevel,
			"world", logrus.Fields{"f1": "a2"},
			map[string]interface{}{"f1": "a2", "@message": "world",
				"@level": "info", "@timestamp": time.Time{}.Format(time.RFC3339),
				"type": "test2", "@version": "1"}},
	}

	for _, te := range tt {
		entry := &logrus.Entry{
			Level:   te.level,
			Message: te.message,
			Time:    time.Time{},
			Data:    te.data,
		}
		h, err := New(Options{Conn: te.buffer, Fields: te.fields, AppName: te.app})
		if err != nil {
			t.Errorf("Expected hook creation won't fail: %s", err)
		}
		if err := h.Fire(entry); err != nil {
			t.Errorf("Expected Fire to not fail: %s", err)
		}
		var res map[string]interface{}
		resBytes, err := ioutil.ReadAll(te.buffer)
		if err != nil {
			t.Errorf("Failed reading buffer: %s", err)
		}
		if err := json.Unmarshal(resBytes, &res); err != nil {
			t.Errorf("Failed unmarshalling read buffer: %s", err)
		}
		if !reflect.DeepEqual(te.expected, res) {
			t.Errorf("Expected to get '%s' but got '%s'", te.expected, res)
		}
	}
}

type failer struct {
	errMsg string
	wfunc  func() error
	ffunc  func() error
}

func (f failer) Format(entry *logrus.Entry) ([]byte, error) {
	return nil, f.ffunc()
}

func (f failer) Write(p []byte) (int, error) {
	return 0, f.wfunc()
}

func TestFireErrors(t *testing.T) {
	tt := []failer{
		{"format error", func() error { return nil }, func() error { return errors.New("format error") }},
		{"write error", func() error { return errors.New("write error") }, func() error { return nil }},
	}

	for _, te := range tt {
		h, err := New(Options{Conn: te, Formatter: te})
		if err != nil {
			t.Errorf("Failed creating hook: %s", err)
		}
		err = h.Fire(&logrus.Entry{Data: logrus.Fields{}})
		if err == nil {
			t.Errorf("Expected Fire to fail but it didn't")
		}
		if err.Error() != te.errMsg {
			t.Errorf("Expected error message to be '%s' but got '%s'", te.errMsg, err)
		}
	}
}
