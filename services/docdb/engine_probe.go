package docdb

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
)

const (
	opMsgCode      = 2013
	wireHeaderLen  = 16
	maxProbeReply  = 1 << 20
	bsonDoubleSize = 8
	bsonInt32      = 0x10
	bsonString     = 0x02
)

var (
	errShellFailed = errors.New("mongo shell failed")
	errExecFailed  = errors.New("exec mongo shell failed")
	errBadReply    = errors.New("unexpected mongod reply")
)

// isMasterMessage builds an OP_MSG of {isMaster: 1, $db: "admin"}, which mongod 3.6+ answers unauthenticated.
func isMasterMessage() []byte {
	doc := []byte{0, 0, 0, 0, bsonInt32}
	doc = append(doc, "isMaster\x00"...)
	doc = binary.LittleEndian.AppendUint32(doc, 1)
	doc = append(doc, bsonString)
	doc = append(doc, "$db\x00"...)
	doc = binary.LittleEndian.AppendUint32(doc, uint32(len("admin")+1))
	doc = append(doc, "admin\x00"...)
	doc = append(doc, 0)
	binary.LittleEndian.PutUint32(doc, uint32(len(doc))) //nolint:gosec // small constant-sized document

	msg := make([]byte, 0, wireHeaderLen+5+len(doc))
	msg = binary.LittleEndian.AppendUint32(msg, uint32(wireHeaderLen+5+len(doc))) //nolint:gosec // small message
	msg = binary.LittleEndian.AppendUint32(msg, 1)
	msg = binary.LittleEndian.AppendUint32(msg, 0)
	msg = binary.LittleEndian.AppendUint32(msg, opMsgCode)
	msg = binary.LittleEndian.AppendUint32(msg, 0)
	msg = append(msg, 0)

	return append(msg, doc...)
}

// probeMongo proves a mongod answers on addr: a published docker port accepts TCP before the server is up,
// so only a real command reply counts.
func probeMongo(ctx context.Context, addr string) error { return probeMongoConn(ctx, addr, nil) }

func probeMongoConn(ctx context.Context, addr string, tlsCfg *tls.Config) error {
	var (
		conn net.Conn
		err  error
	)

	if tlsCfg != nil {
		conn, err = (&tls.Dialer{Config: tlsCfg}).DialContext(ctx, "tcp", addr)
	} else {
		conn, err = (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	}

	if err != nil {
		return fmt.Errorf("dial mongod: %w", err)
	}

	defer func() { _ = conn.Close() }()

	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}

	if _, err = conn.Write(isMasterMessage()); err != nil {
		return fmt.Errorf("write isMaster: %w", err)
	}

	hdr := make([]byte, wireHeaderLen)
	if _, err = io.ReadFull(conn, hdr); err != nil {
		return fmt.Errorf("read reply header: %w", err)
	}

	length := binary.LittleEndian.Uint32(hdr[0:4])
	if binary.LittleEndian.Uint32(hdr[12:16]) != opMsgCode || length <= wireHeaderLen || length > maxProbeReply {
		return errBadReply
	}

	body := make([]byte, length-wireHeaderLen)
	if _, err = io.ReadFull(conn, body); err != nil {
		return fmt.Errorf("read reply body: %w", err)
	}

	return checkOK(body)
}

func checkOK(body []byte) error {
	marker := []byte("\x01ok\x00")

	i := bytes.Index(body, marker)
	if i < 0 || len(body) < i+len(marker)+bsonDoubleSize {
		return errBadReply
	}

	bits := binary.LittleEndian.Uint64(body[i+len(marker):])
	if math.Float64frombits(bits) != 1 {
		return errBadReply
	}

	return nil
}
