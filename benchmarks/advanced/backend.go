// Deterministic fixture; no application dependencies, cache, or disk I/O.
package main

import (
    "bytes"
    "crypto/sha256"
    "encoding/binary"
    "encoding/hex"
    "fmt"
    "io"
    "log"
    "net/http"
    "os"
    "strconv"
    "strings"
    "sync/atomic"
    "time"
)

var block = []byte(strings.Repeat("0123456789abcdef", 1024))
var uploadHash = sha256.Sum256(bytes.Repeat(block, 64))
var backendLabel string
var backendDelay time.Duration
var backendRequests atomic.Uint64

func serve(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("x-upstream-http-version", r.Proto)
    if value := r.Header.Get("x-test-plugin"); value != "" { w.Header().Set("x-seen-plugin", value) }
    if r.URL.Path == "/ready" { w.Write([]byte("ready")); return }
    if r.URL.Path == "/stats" { w.Header().Set("content-type", "application/json"); fmt.Fprintf(w, "{\"label\":%q,\"requests\":%d}", backendLabel, backendRequests.Load()); return }
    if backendLabel != "" { w.Header().Set("x-backend", backendLabel); backendRequests.Add(1) }
    if backendDelay > 0 { time.Sleep(backendDelay) }
    if r.URL.Path == "/upload" {
        hash := sha256.New()
        n, err := io.Copy(hash, io.LimitReader(r.Body, 16*1024*1024+1))
        if err != nil || n != 1048576 || !bytes.Equal(hash.Sum(nil), uploadHash[:]) { http.Error(w, "invalid upload", 400); return }
        w.Header().Set("Content-Type", "text/plain")
        fmt.Fprintf(w, "%d:%s", n, hex.EncodeToString(hash.Sum(nil)))
        return
    }
    if r.URL.Path == "/grpc" {
        input, err := io.ReadAll(io.LimitReader(r.Body, 6))
        if err != nil || r.Method != "POST" || !bytes.Equal(input, []byte{0,0,0,0,0}) { http.Error(w, "invalid grpc request", 400); return }
        w.Header().Set("Content-Type", "application/grpc")
        w.Header().Set("Trailer", "grpc-status")
        w.WriteHeader(200)
        for i := 0; i < 16; i++ {
            prefix := make([]byte, 5)
            binary.BigEndian.PutUint32(prefix[1:], uint32(len(block)))
            if _, err := w.Write(prefix); err != nil { return }
            if _, err := w.Write(block); err != nil { return }
            w.(http.Flusher).Flush()
            time.Sleep(time.Millisecond)
        }
        w.Header().Set("grpc-status", "0")
        return
    }
    size := 65536
    if strings.HasSuffix(r.URL.Path, "-small") { size = 34 }
    if strings.HasPrefix(r.URL.Path, "/bytes/") {
        n, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/bytes/"))
        if err != nil || n < 0 || n > 16*1024*1024 { http.Error(w, "invalid size", 400); return }
        size = n
    }
    stream := r.URL.Path == "/stream"
    if stream { size = 256*1024 }
    w.Header().Set("Content-Type", "application/octet-stream")
    w.Header().Set("Content-Length", strconv.Itoa(size))
    for size > 0 {
        n := min(size, len(block))
        if _, err := w.Write(block[:n]); err != nil { return }
        size -= n
        if stream { w.(http.Flusher).Flush(); time.Sleep(2*time.Millisecond) }
    }
}

func main() {
    backendLabel = os.Getenv("BACKEND_LABEL")
    delay, err := strconv.Atoi(os.Getenv("REQUEST_DELAY_MS"))
    if err == nil && delay > 0 { backendDelay = time.Duration(delay) * time.Millisecond }
    handler := http.HandlerFunc(serve)
    go func() {
        server := &http.Server{Addr: ":8080", Handler: handler, ReadHeaderTimeout: 10*time.Second}
        log.Fatal(server.ListenAndServe())
    }()
    server := &http.Server{Addr: ":8443", Handler: handler, ReadHeaderTimeout: 10*time.Second}
    log.Fatal(server.ListenAndServeTLS("/certs/cert.pem", "/certs/key.pem"))
}
