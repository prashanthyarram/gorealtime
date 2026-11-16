package ws

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	opCont   = 0x0
	opText   = 0x1
	opBinary = 0x2
	opClose  = 0x8
	opPing   = 0x9
	opPong   = 0xA
)

var (
	errProtocol = errors.New("websocket: protocol error")
	errTooBig   = errors.New("websocket: message too big")
	errClosed   = errors.New("websocket: closed")
)

// wire is one RFC 6455 connection.
// Read must not run concurrently with itself. Writes are serialized.
// Control frames are handled inside Read, so Ping waits for a pong that
// only an active Read can observe.
type wire struct {
	nc        net.Conn
	br        io.Reader
	server    bool
	readLimit int64

	writeMu sync.Mutex
	readMu  sync.Mutex

	pongMu   sync.Mutex
	pongWait chan struct{}

	closeOnce sync.Once
	shut      atomic.Bool
	pmce      *pmce
}

func (w *wire) Read(ctx context.Context) (binary bool, data []byte, err error) {
	w.readMu.Lock()
	defer func() {
		if w.shut.Load() && w.pmce != nil {
			w.pmce.releaseIn()
		}
		w.readMu.Unlock()
	}()
	stop := w.armDeadline(ctx, false)
	defer stop()

	var (
		buf        []byte
		gotOp      byte
		compressed bool
	)
	for {
		fin, op, rsv1, payload, err := w.readFrame()
		if err != nil {
			return false, nil, err
		}
		switch op {
		case opPing:
			if err := w.writeFrame(context.Background(), opPong, payload); err != nil {
				return false, nil, err
			}
		case opPong:
			w.notePong()
		case opClose:
			_ = w.writeFrame(context.Background(), opClose, payload)
			return false, nil, closeError(payload)
		case opText, opBinary:
			if gotOp != 0 {
				w.CloseNow()
				return false, nil, errProtocol
			}
			gotOp = op
			if rsv1 {
				compressed = true
			}
			buf = append(buf, payload...)
			if w.messageTooBig(len(buf), compressed) {
				w.CloseNow()
				return false, nil, errTooBig
			}
			if fin {
				return w.finishMessage(ctx, gotOp, buf, compressed)
			}
		case opCont:
			if gotOp == 0 || rsv1 {
				w.CloseNow()
				return false, nil, errProtocol
			}
			buf = append(buf, payload...)
			if w.messageTooBig(len(buf), compressed) {
				w.CloseNow()
				return false, nil, errTooBig
			}
			if fin {
				return w.finishMessage(ctx, gotOp, buf, compressed)
			}
		default:
			w.CloseNow()
			return false, nil, errProtocol
		}
	}
}

func (w *wire) finishMessage(ctx context.Context, op byte, buf []byte, compressed bool) (bool, []byte, error) {
	if compressed {
		out, err := w.pmce.decompress(ctx, buf)
		if err != nil {
			w.CloseNow()
			return false, nil, err
		}
		buf = out
	}
	return op == opBinary, buf, nil
}

func (w *wire) Write(ctx context.Context, p []byte) error {
	return w.writeFrame(ctx, opText, p)
}

func (w *wire) writeMessage(ctx context.Context, binary bool, p []byte) error {
	op := byte(opText)
	if binary {
		op = opBinary
	}
	w.writeMu.Lock()
	defer w.unlockWrite()
	payload := p
	rsv1 := false
	if w.pmce != nil && w.pmce.on {
		out, compressed, err := w.pmce.compress(p)
		if err != nil {
			return err
		}
		payload = out
		rsv1 = compressed
	}
	return w.writeFrameLocked(ctx, op, payload, true, rsv1)
}

func (w *wire) Ping(ctx context.Context) error {
	ch := make(chan struct{})
	w.pongMu.Lock()
	w.pongWait = ch
	w.pongMu.Unlock()
	if err := w.writeFrame(ctx, opPing, nil); err != nil {
		w.pongMu.Lock()
		if w.pongWait == ch {
			w.pongWait = nil
		}
		w.pongMu.Unlock()
		return err
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *wire) Close(code int, reason string) error {
	if code == 0 {
		code = StatusNormalClosure
	}
	if len(reason) > 123 {
		reason = reason[:123]
	}
	payload := make([]byte, 2+len(reason))
	binaryPutUint16(payload[:2], uint16(code))
	copy(payload[2:], reason)
	return w.writeFrame(context.Background(), opClose, payload)
}

func (w *wire) CloseNow() error {
	if w == nil || w.nc == nil {
		return nil
	}
	w.shut.Store(true)
	w.closeOnce.Do(func() {
		_ = w.nc.Close()
	})
	// Read or Write may hold these locks. TryLock drops the flate windows
	// when the caller is outside those calls. The in-call path drops them
	// on the way out, after it notices shut.
	if w.readMu.TryLock() {
		if w.pmce != nil {
			w.pmce.releaseIn()
		}
		w.readMu.Unlock()
	}
	if w.writeMu.TryLock() {
		if w.pmce != nil {
			w.pmce.releaseOut()
		}
		w.writeMu.Unlock()
	}
	return nil
}

func (w *wire) notePong() {
	w.pongMu.Lock()
	ch := w.pongWait
	w.pongWait = nil
	w.pongMu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (w *wire) readFrame() (fin bool, op byte, rsv1 bool, payload []byte, err error) {
	var hdr [2]byte
	if _, err = io.ReadFull(w.br, hdr[:]); err != nil {
		return false, 0, false, nil, err
	}
	fin = hdr[0]&0x80 != 0
	rsv1 = hdr[0]&0x40 != 0
	op = hdr[0] & 0x0f
	// RSV2 and RSV3 are unused. RSV1 is the permessage-deflate bit and
	// is legal only on the first frame of a data message after negotiation.
	if hdr[0]&0x30 != 0 || (rsv1 && (op&0x8 != 0 || op == opCont || w.pmce == nil || !w.pmce.on)) {
		w.CloseNow()
		return false, 0, false, nil, errProtocol
	}
	masked := hdr[1]&0x80 != 0
	if w.server != masked {
		w.CloseNow()
		return false, 0, false, nil, errProtocol
	}
	n := int64(hdr[1] & 0x7f)
	switch n {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(w.br, ext[:]); err != nil {
			return false, 0, false, nil, err
		}
		n = int64(binary.BigEndian.Uint16(ext[:]))
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(w.br, ext[:]); err != nil {
			return false, 0, false, nil, err
		}
		u := binary.BigEndian.Uint64(ext[:])
		if u > 1<<62 {
			w.CloseNow()
			return false, 0, false, nil, errProtocol
		}
		n = int64(u)
	}
	if n > w.readLimit || (rsv1 && w.pmce != nil && w.pmce.maxIn > 0 && n > w.pmce.maxIn) {
		w.CloseNow()
		return false, 0, false, nil, errTooBig
	}
	control := op&0x8 != 0
	if control && (!fin || n > 125) {
		w.CloseNow()
		return false, 0, false, nil, errProtocol
	}
	var key [4]byte
	if masked {
		if _, err = io.ReadFull(w.br, key[:]); err != nil {
			return false, 0, false, nil, err
		}
	}
	payload = make([]byte, n)
	if _, err = io.ReadFull(w.br, payload); err != nil {
		return false, 0, false, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= key[i%4]
		}
	}
	return fin, op, rsv1, payload, nil
}

func (w *wire) writeFrame(ctx context.Context, op byte, payload []byte) error {
	w.writeMu.Lock()
	defer w.unlockWrite()
	return w.writeFrameLocked(ctx, op, payload, true, false)
}

func (w *wire) unlockWrite() {
	if w.shut.Load() && w.pmce != nil {
		w.pmce.releaseOut()
	}
	w.writeMu.Unlock()
}

// writeFrameFlags sends one frame without compressing it.
// Tests use it to build fragmented and RSV1 frames directly.
func (w *wire) writeFrameFlags(ctx context.Context, fin bool, op byte, rsv1 bool, payload []byte) error {
	w.writeMu.Lock()
	defer w.unlockWrite()
	return w.writeFrameLocked(ctx, op, payload, fin, rsv1)
}

func (w *wire) writeFrameLocked(ctx context.Context, op byte, payload []byte, fin, rsv1 bool) error {
	if payload == nil {
		payload = []byte{}
	}
	stop := w.armDeadline(ctx, true)
	defer stop()

	mask := !w.server
	hdrLen := 2
	n := len(payload)
	switch {
	case n > 65535:
		hdrLen += 8
	case n > 125:
		hdrLen += 2
	}
	if mask {
		hdrLen += 4
	}
	buf := make([]byte, hdrLen+n)
	if fin {
		buf[0] |= 0x80
	}
	if rsv1 {
		buf[0] |= 0x40
	}
	buf[0] |= op
	switch {
	case n > 65535:
		buf[1] = 127
		binary.BigEndian.PutUint64(buf[2:10], uint64(n))
	case n > 125:
		buf[1] = 126
		binary.BigEndian.PutUint16(buf[2:4], uint16(n))
	default:
		buf[1] = byte(n)
	}
	off := hdrLen
	if mask {
		buf[1] |= 0x80
		off -= 4
		if _, err := rand.Read(buf[off : off+4]); err != nil {
			return err
		}
		key := buf[off : off+4]
		dst := buf[hdrLen:]
		for i, b := range payload {
			dst[i] = b ^ key[i%4]
		}
	} else {
		copy(buf[hdrLen:], payload)
	}
	_, err := w.nc.Write(buf)
	if err != nil {
		return err
	}
	return nil
}

func (w *wire) armDeadline(ctx context.Context, write bool) func() {
	if ctx == nil || ctx.Done() == nil {
		return func() {}
	}
	var once sync.Once
	stop := context.AfterFunc(ctx, func() {
		once.Do(func() {
			if write {
				_ = w.nc.SetWriteDeadline(time.Now())
			} else {
				_ = w.nc.SetReadDeadline(time.Now())
			}
		})
	})
	return func() {
		stop()
		if write {
			_ = w.nc.SetWriteDeadline(time.Time{})
		} else {
			_ = w.nc.SetReadDeadline(time.Time{})
		}
	}
}

func (w *wire) messageTooBig(n int, compressed bool) bool {
	if int64(n) > w.readLimit {
		return true
	}
	return compressed && w.pmce != nil && w.pmce.maxIn > 0 && int64(n) > w.pmce.maxIn
}

func closeError(payload []byte) error {
	if len(payload) == 0 {
		return &CloseError{Code: 1005}
	}
	if len(payload) < 2 {
		return errProtocol
	}
	return &CloseError{
		Code:   int(binary.BigEndian.Uint16(payload[:2])),
		Reason: string(payload[2:]),
	}
}

func binaryPutUint16(b []byte, v uint16) {
	binary.BigEndian.PutUint16(b, v)
}
