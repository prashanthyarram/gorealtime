package gorealtime

import (
	"compress/gzip"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// acceptsGzip reports whether the client asked for gzip with a non-zero
// quality. An explicit gzip;q=0 loses. A bare * allows gzip. An empty
// header does not, so compression is never assumed.
func acceptsGzip(header string) bool {
	if strings.TrimSpace(header) == "" {
		return false
	}
	gzipQ := -1.0
	starQ := -1.0
	for _, part := range strings.Split(header, ",") {
		name, q, ok := codingQuality(part)
		if !ok {
			continue
		}
		switch name {
		case "gzip", "x-gzip":
			gzipQ = q
		case "*":
			starQ = q
		}
	}
	if gzipQ >= 0 {
		return gzipQ > 0
	}
	return starQ > 0
}

func codingQuality(part string) (name string, q float64, ok bool) {
	part = strings.TrimSpace(part)
	if part == "" {
		return "", 0, false
	}
	name = part
	q = 1
	if i := strings.Index(part, ";"); i >= 0 {
		name = strings.TrimSpace(part[:i])
		for _, param := range strings.Split(part[i+1:], ";") {
			param = strings.TrimSpace(param)
			if len(param) < 3 || !strings.EqualFold(param[:2], "q=") {
				continue
			}
			v, err := strconv.ParseFloat(param[2:], 64)
			if err != nil || v < 0 {
				q = 0
				continue
			}
			q = v
		}
	}
	if name == "" {
		return "", 0, false
	}
	return strings.ToLower(name), q, true
}

func addVary(w http.ResponseWriter, value string) {
	existing := w.Header().Get("Vary")
	if existing == "" {
		w.Header().Set("Vary", value)
		return
	}
	for _, part := range strings.Split(existing, ",") {
		if strings.EqualFold(strings.TrimSpace(part), value) {
			return
		}
	}
	w.Header().Set("Vary", existing+", "+value)
}

// gzipStream compresses one HTTP response and flushes after each write so
// a server-sent event is not held in the gzip buffer.
type gzipStream struct {
	http.ResponseWriter
	gz *gzip.Writer
	f  http.Flusher
}

func (g *gzipStream) Write(p []byte) (int, error) {
	n, err := g.gz.Write(p)
	if err != nil {
		return n, err
	}
	if err := g.gz.Flush(); err != nil {
		return n, err
	}
	g.f.Flush()
	return n, nil
}

func (g *gzipStream) Flush() {
	_ = g.gz.Flush()
	g.f.Flush()
}

// wrapGzip compresses w when the client accepts gzip and nothing else has
// already set Content-Encoding. The returned closer finishes the gzip
// trailer. ok is false when the response stays uncompressed.
func wrapGzip(w http.ResponseWriter, r *http.Request) (out http.ResponseWriter, flush http.Flusher, closeFn func() error, ok bool) {
	base, isFlush := w.(http.Flusher)
	if !isFlush || w.Header().Get("Content-Encoding") != "" || !acceptsGzip(r.Header.Get("Accept-Encoding")) {
		return w, base, func() error { return nil }, false
	}
	addVary(w, "Accept-Encoding")
	w.Header().Set("Content-Encoding", "gzip")
	gz := gzip.NewWriter(w)
	stream := &gzipStream{ResponseWriter: w, gz: gz, f: base}
	return stream, stream, gz.Close, true
}

// gzipWriter wraps dst for one buffered HTTP body, such as a long poll.
// ok is false when the body should be written as identity.
func gzipWriter(w http.ResponseWriter, r *http.Request) (dst io.Writer, closeFn func() error, ok bool) {
	if w.Header().Get("Content-Encoding") != "" || !acceptsGzip(r.Header.Get("Accept-Encoding")) {
		return w, func() error { return nil }, false
	}
	addVary(w, "Accept-Encoding")
	w.Header().Set("Content-Encoding", "gzip")
	gz := gzip.NewWriter(w)
	return gz, gz.Close, true
}
