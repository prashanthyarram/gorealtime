package ws

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// The Python client is a second implementation. It uses the standard
// library zlib module, not this package's compressor.
func TestPythonPermessageDeflate(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 is not installed")
	}
	script := t.TempDir() + "/peer.py"
	if err := os.WriteFile(script, []byte(pythonPeer), 0o644); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		mode string
		cfg  Compression
	}{
		{mode: "round", cfg: Compression{Enabled: true, MinSize: 1}},
		{mode: "context", cfg: Compression{Enabled: true, MinSize: 1, ServerContextTakeover: true, ClientContextTakeover: true}},
		{mode: "fragment", cfg: Compression{Enabled: true, MinSize: 1}},
		{mode: "bad", cfg: Compression{Enabled: true}},
		{mode: "fallback", cfg: Compression{Enabled: true}},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				sess, err := Upgrade(w, r, Config{
					ReadLimit:   1 << 20,
					Compression: tc.cfg,
				})
				if err != nil {
					return
				}
				defer sess.CloseNow()
				if tc.mode == "bad" {
					_, _, _ = sess.Read(context.Background())
					return
				}
				for {
					binary, data, err := sess.Read(context.Background())
					if err != nil {
						return
					}
					msg := append([]byte(nil), data...)
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					err = sess.w.writeMessage(ctx, binary, msg)
					cancel()
					if err != nil {
						return
					}
				}
			}))
			defer s.Close()
			host, port, err := net.SplitHostPort(s.Listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, python, script, host, port, tc.mode)
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("python: %v\n%s", err, out)
			}
			if !strings.Contains(string(out), "ok") && !strings.Contains(string(out), "closed") {
				t.Fatalf("python output %q", out)
			}
			if tc.mode == "bad" && !strings.Contains(string(out), "closed") {
				t.Fatalf("bad stream stayed open: %s", out)
			}
		})
	}
}

const pythonPeer = `
import base64, hashlib, os, socket, struct, sys, zlib

GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

def main():
    host, port, mode = sys.argv[1], int(sys.argv[2]), sys.argv[3]
    sock = socket.create_connection((host, port), timeout=3)
    sock.settimeout(3)
    key = base64.b64encode(os.urandom(16)).decode()
    if mode == "fallback":
        ext = "permessage-deflate; server_max_window_bits=8, permessage-deflate; client_max_window_bits"
    elif mode == "context":
        ext = "permessage-deflate"
    else:
        ext = "permessage-deflate; server_no_context_takeover; client_no_context_takeover"
    req = (
        "GET / HTTP/1.1\r\n"
        "Host: %s:%s\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "Sec-WebSocket-Extensions: %s\r\n"
        "\r\n"
    ) % (host, port, key, ext)
    sock.sendall(req.encode())
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            raise SystemExit("handshake eof")
        data += chunk
    head, rest = data.split(b"\r\n\r\n", 1)
    text = head.decode()
    if "101" not in text.split("\r\n", 1)[0]:
        raise SystemExit("status " + text.split("\r\n", 1)[0])
    accept = base64.b64encode(hashlib.sha1((key + GUID).encode()).digest()).decode()
    if accept not in text:
        raise SystemExit("bad accept")
    agreed = ""
    for line in text.split("\r\n"):
        if line.lower().startswith("sec-websocket-extensions:"):
            agreed = line.split(":", 1)[1].strip()
    buf = bytearray(rest)

    def send_frame(op, payload, fin=True, rsv1=False):
        b0 = op & 0x0F
        if fin:
            b0 |= 0x80
        if rsv1:
            b0 |= 0x40
        mask = os.urandom(4)
        n = len(payload)
        hdr = bytearray([b0])
        if n < 126:
            hdr.append(0x80 | n)
        elif n < 65536:
            hdr.append(0x80 | 126)
            hdr += struct.pack("!H", n)
        else:
            hdr.append(0x80 | 127)
            hdr += struct.pack("!Q", n)
        hdr += mask
        masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        sock.sendall(bytes(hdr) + masked)

    def recv_exact(n):
        while len(buf) < n:
            chunk = sock.recv(4096)
            if not chunk:
                raise SystemExit("eof")
            buf.extend(chunk)
        out = bytes(buf[:n])
        del buf[:n]
        return out

    def recv_frame():
        h = recv_exact(2)
        rsv1 = h[0] & 0x40
        op = h[0] & 0x0F
        ln = h[1] & 0x7F
        if ln == 126:
            ln = struct.unpack("!H", recv_exact(2))[0]
        elif ln == 127:
            ln = struct.unpack("!Q", recv_exact(8))[0]
        if h[1] & 0x80:
            raise SystemExit("server masked")
        payload = recv_exact(ln) if ln else b""
        return op, bool(rsv1), payload

    def inflate(payload, dec):
        return dec.decompress(payload + b"\x00\x00\xff\xff")

    def deflate(comp, msg):
        raw = comp.compress(msg) + comp.flush(zlib.Z_SYNC_FLUSH)
        if not raw.endswith(b"\x00\x00\xff\xff"):
            raise SystemExit("missing tail")
        return raw[:-4]

    no_server_ctx = "server_no_context_takeover" in agreed
    no_client_ctx = "client_no_context_takeover" in agreed
    if mode == "fallback":
        if "server_max_window_bits" in agreed:
            raise SystemExit("accepted small window: " + agreed)
        if agreed != "permessage-deflate; server_no_context_takeover; client_no_context_takeover":
            raise SystemExit("fallback response " + agreed)
    elif mode == "context":
        if agreed != "permessage-deflate":
            raise SystemExit("context response " + agreed)
    elif "permessage-deflate" not in agreed:
        raise SystemExit("no deflate " + agreed)

    comp = {"c": zlib.compressobj(level=1, wbits=-15)}
    dec = {"d": zlib.decompressobj(wbits=-15)}

    def send_msg(msg, compress=True):
        if not compress:
            send_frame(1, msg, rsv1=False)
            return
        if no_client_ctx:
            comp["c"] = zlib.compressobj(level=1, wbits=-15)
        send_frame(1, deflate(comp["c"], msg), rsv1=True)

    def read_msg():
        op, rsv1, payload = recv_frame()
        if op != 1:
            raise SystemExit("op %d" % op)
        if not rsv1:
            return payload
        if no_server_ctx:
            dec["d"] = zlib.decompressobj(wbits=-15)
        return inflate(payload, dec["d"])

    if mode == "bad":
        send_frame(1, b"\xff\x01", rsv1=True)
        try:
            sock.settimeout(2)
            chunk = sock.recv(64)
        except OSError:
            print("closed")
            return
        if chunk == b"":
            print("closed")
            return
        raise SystemExit("still open")

    if mode == "fragment":
        comp["c"] = zlib.compressobj(level=1, wbits=-15)
        raw = deflate(comp["c"], b"Hello")
        send_frame(1, raw[:3], fin=False, rsv1=True)
        send_frame(0, raw[3:], fin=True, rsv1=False)
        if read_msg() != b"Hello":
            raise SystemExit("fragment mismatch")
        print("ok")
        return

    if mode == "context":
        send_msg(b"Hello")
        if read_msg() != b"Hello":
            raise SystemExit("echo1")
        send_msg(b"ping", compress=False)
        if read_msg() != b"ping":
            raise SystemExit("plain")
        send_msg(b"Hello")
        if read_msg() != b"Hello":
            raise SystemExit("echo2")
        print("ok")
        return

    msg = b"hello " * 200
    send_msg(msg)
    if read_msg() != msg:
        raise SystemExit("round mismatch")
    print("ok")

if __name__ == "__main__":
    main()
`
