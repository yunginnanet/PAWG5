package server

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"io"
	"os"
	"strconv"

	"github.com/akrylysov/pogreb"
)

// Restore attempts to restore a database from rd, which implements an io.Reader.
// This is meant to restore data exported by the Snapshot function.
func (kvm *StateMachine) Restore(rd io.Reader) error {
	kvm.mu.Lock()
	defer kvm.mu.Unlock()
	var err error
	if err := kvm.db.Close(); err != nil {
		return err
	}
	if err := os.RemoveAll(kvm.dbPath); err != nil {
		return err
	}
	kvm.db = nil
	kvm.db, err = pogreb.Open(kvm.dir, &pogreb.Options{})
	if err != nil {
		return err
	}
	num := make([]byte, 8)
	gzr, err := gzip.NewReader(rd)
	if err != nil {
		return err
	}
	r := bufio.NewReader(gzr)
	for {
		if _, err := io.ReadFull(r, num); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		key := make([]byte, int(binary.LittleEndian.Uint64(num)))
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, num); err != nil {
			return err
		}
		value := make([]byte, int(binary.LittleEndian.Uint64(num)))
		if _, err := io.ReadFull(r, value); err != nil {
			return err
		}
		if err := kvm.db.Put(key, value); err != nil {
			return err
		}
	}
	return gzr.Close()
}

// WriteRedisCommandsFromSnapshot will read a snapshot and write all the
// Redis SET commands needed to rebuild the entire database.
// The commands are written to wr.
func WriteRedisCommandsFromSnapshot(wr io.Writer, snapshotPath string) error {
	f, err := os.Open(snapshotPath)
	if err != nil {
		return err
	}
	defer f.Close()
	var cmd []byte
	num := make([]byte, 8)
	var gzclosed bool
	gzr, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	defer func() {
		if !gzclosed {
			gzr.Close()
		}
	}()
	r := bufio.NewReader(gzr)
	for {
		if _, err := io.ReadFull(r, num); err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		key := make([]byte, int(binary.LittleEndian.Uint64(num)))
		if _, err := io.ReadFull(r, key); err != nil {
			return err
		}
		if _, err := io.ReadFull(r, num); err != nil {
			return err
		}
		value := make([]byte, int(binary.LittleEndian.Uint64(num)))
		if _, err := io.ReadFull(r, value); err != nil {
			return err
		}
		if len(key) == 0 || key[0] != 'k' {
			// do not accept keys that do not start with 'k'
			continue
		}
		key = key[1:]
		cmd = cmd[:0]
		cmd = append(cmd, "*3\r\n$3\r\nSET\r\n$"...)
		cmd = strconv.AppendInt(cmd, int64(len(key)), 10)
		cmd = append(cmd, '\r', '\n')
		cmd = append(cmd, key...)
		cmd = append(cmd, '\r', '\n', '$')
		cmd = strconv.AppendInt(cmd, int64(len(value)), 10)
		cmd = append(cmd, '\r', '\n')
		cmd = append(cmd, value...)
		cmd = append(cmd, '\r', '\n')
		if _, err := wr.Write(cmd); err != nil {
			return err
		}
	}
	err = gzr.Close()
	gzclosed = true
	return err
}

// Snapshot writes a snapshot of the database to wr, which implements io.Writer.
func (kvm *StateMachine) Snapshot(wr io.Writer) error {
	kvm.mu.RLock()
	defer kvm.mu.RUnlock()
	gzw := gzip.NewWriter(wr)

	iter := kvm.db.Items()
	var (
		key     []byte
		value   []byte
		iterErr error
	)

	for key, value, iterErr = iter.Next(); iterErr == nil; key, value, iterErr = iter.Next() {
		if err := binary.Write(gzw, binary.LittleEndian, uint64(len(key))); err != nil {
			return err
		}
		if _, err := gzw.Write(key); err != nil {
			return err
		}
		if err := binary.Write(gzw, binary.LittleEndian, uint64(len(value))); err != nil {
			return err
		}
		if _, err := gzw.Write(value); err != nil {
			return err
		}
	}

	return gzw.Close()
}
