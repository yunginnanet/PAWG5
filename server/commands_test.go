package server

import (
	"net"
	"strconv"
	"testing"

	"github.com/tidwall/finn"
	"github.com/tidwall/redcon"
)

type DummyConn struct {
	Result string
	t      *testing.T
}

func (dc *DummyConn) Warningf(format string, args ...interface{}) {
	dc.t.Logf("WARN: "+format, args...)
}

func (dc *DummyConn) Printf(format string, args ...interface{}) {
	dc.t.Logf("LOG: "+format, args...)
}

func (dc *DummyConn) Verbosef(format string, args ...interface{}) {
	dc.t.Logf("VERBOSE: "+format, args...)
}

func (dc *DummyConn) Noticef(format string, args ...interface{}) {
	dc.t.Logf("NOTICE: "+format, args...)
}

func (dc *DummyConn) Debugf(format string, args ...interface{}) {
	dc.t.Logf("DEBUG: "+format, args...)
}

func (dc *DummyConn) Apply(conn redcon.Conn, cmd redcon.Command, mutate func() (interface{}, error), respond func(interface{}) (interface{}, error)) (interface{}, error) {
	// mutate()
	return respond(cmd)
}

func (dc *DummyConn) Log() finn.Logger {
	return dc
}

func (dc *DummyConn) RemoteAddr() string {
	return ""
}
func (dc *DummyConn) Close() error {
	return nil
}
func (dc *DummyConn) WriteError(msg string)  {}
func (dc *DummyConn) WriteString(str string) {}
func (dc *DummyConn) WriteBulk(bulk []byte) {
	dc.Result += "," + string(bulk)
}
func (dc *DummyConn) WriteBulkString(bulk string) {}
func (dc *DummyConn) WriteInt(num int)            {}
func (dc *DummyConn) WriteInt64(num int64)        {}
func (dc *DummyConn) WriteUint64(num uint64)      {}
func (dc *DummyConn) WriteArray(count int) {
	dc.Result = strconv.Itoa(count)
}
func (dc *DummyConn) WriteNull()               {}
func (dc *DummyConn) WriteRaw(data []byte)     {}
func (dc *DummyConn) WriteAny(any interface{}) {}
func (dc *DummyConn) Context() interface{} {
	return nil
}
func (dc *DummyConn) SetContext(v interface{}) {}
func (dc *DummyConn) SetReadBuffer(bytes int)  {}
func (dc *DummyConn) Detach() redcon.DetachedConn {
	return nil
}
func (dc *DummyConn) ReadPipeline() []redcon.Command {
	return nil
}
func (dc *DummyConn) PeekPipeline() []redcon.Command {
	return nil
}
func (dc *DummyConn) NetConn() net.Conn {
	return nil
}

func TestCmdKeys(t *testing.T) {
	var kvm *StateMachine
	var err error
	if kvm, err = NewStateMachine(t.TempDir()); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	dconn := &DummyConn{}

	cmd := redcon.Command{Raw: []byte("KEYS *"), Args: [][]byte{[]byte("KEYS"), []byte("*")}}

	expectedKeys := [][]byte{[]byte("key.1"), []byte("cc.1"), []byte("key.2")}

	doput := func(k, v string) {
		if err := kvm.db.Put([]byte(k), []byte(v)); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	doput("key.1", "value.1")
	doput("cc.1", "value.2")
	doput("key.2", "value.3")

	if _, err = kvm.cmdKeys(dconn, dconn, cmd); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if dconn.Result != "3,key.1,cc.1,key.2" {
		t.Errorf("Expected keys %v, but got %v", expectedKeys, dconn.Result)
	}

	cmd = redcon.Command{Raw: []byte("KEYS c*"), Args: [][]byte{[]byte("KEYS"), []byte("c*")}}
	_, err = kvm.cmdKeys(dconn, dconn, cmd)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if dconn.Result != "1,cc.1" {
		t.Errorf("Expected keys %v, but got %v", expectedKeys, dconn.Result)
	}
}
