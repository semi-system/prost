// Isolated load fixture. Implements mail protocol transport, not authentication,
// persistence or delivery. All interfaces are confined to its benchmark network.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var proxyCount, localCount, invalidCount, smtpCount, imapCount, starttlsCount, reloads atomic.Uint64
var active atomic.Int64
var cached atomic.Pointer[tls.Certificate]
var capacity = make(chan struct{}, 8192)
var payload = bytes.Repeat([]byte("x"), 16384)

func loadCertificate() (string, error) {
	version, err := filepath.EvalSymlinks("/certificates/edge/current")
	if err != nil {
		return "", err
	}
	certificate, err := tls.LoadX509KeyPair(filepath.Join(version, "fullchain.pem"), filepath.Join(version, "privkey.pem"))
	if err != nil {
		return "", err
	}
	cached.Store(&certificate)
	digest := sha256.Sum256(certificate.Certificate[0])
	return hex.EncodeToString(digest[:]), nil
}
func serverTLS() *tls.Config {
	return &tls.Config{MinVersion: tls.VersionTLS12, SessionTicketsDisabled: true, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
		if certificate := cached.Load(); certificate != nil {
			return certificate, nil
		}
		_, err := loadCertificate()
		return cached.Load(), err
	}}
}
func proxy(c net.Conn) (string, error) {
	c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var header [16]byte
	if _, err := io.ReadFull(c, header[:]); err != nil {
		return "", err
	}
	if !bytes.Equal(header[:12], []byte("\r\n\r\n\x00\r\nQUIT\n")) {
		invalidCount.Add(1)
		return "", fmt.Errorf("PROXY signature")
	}
	if bytes.Equal(header[12:], []byte{0x20, 0, 0, 0}) {
		localCount.Add(1)
		c.SetReadDeadline(time.Time{})
		return "LOCAL", nil
	}
	size := int(binary.BigEndian.Uint16(header[14:]))
	if header[12] != 0x21 || !((header[13] == 0x11 && size == 12) || (header[13] == 0x21 && size == 36)) {
		invalidCount.Add(1)
		return "", fmt.Errorf("PROXY command/family/length/TLV")
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(c, data); err != nil {
		return "", err
	}
	length := 4
	if size == 36 {
		length = 16
	}
	source := net.IP(data[:length]).String()
	proxyCount.Add(1)
	c.SetReadDeadline(time.Time{})
	return source, nil
}

type addressConn struct {
	net.Conn
	source string
}

func (c *addressConn) RemoteAddr() net.Addr { return &net.TCPAddr{IP: net.ParseIP(c.source)} }

type proxyListener struct {
	net.Listener
	accepted chan net.Conn
	failures chan error
}

func newProxyListener(listener net.Listener) net.Listener {
	result := &proxyListener{Listener: listener, accepted: make(chan net.Conn, 1024), failures: make(chan error, 1)}
	go func() {
		for {
			c, err := listener.Accept()
			if err != nil {
				result.failures <- err
				return
			}
			select {
			case capacity <- struct{}{}:
			default:
				c.Close()
				continue
			}
			// A canceled client may leave an incomplete PROXY prefix. Parsing
			// it in Accept would block every unrelated connection/health probe.
			go func(c net.Conn) {
				defer func() { <-capacity }()
				source, err := proxy(c)
				if err != nil {
					c.Close()
					return
				}
				select {
				case result.accepted <- &addressConn{c, source}:
				case <-time.After(time.Second):
					c.Close()
				}
			}(c)
		}
	}()
	return result
}

func (l *proxyListener) Accept() (net.Conn, error) {
	select {
	case c := <-l.accepted:
		return c, nil
	case err := <-l.failures:
		return nil, err
	}
}
func listen(address string) net.Listener {
	// Private test-network listener; no public application deployment.
	// nosemgrep: go.lang.security.audit.net.bind_all.avoid-bind-to-all-interfaces
	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatal(err)
	}
	return listener
}
func write(c net.Conn, value string) bool {
	c.SetWriteDeadline(time.Now().Add(30 * time.Second))
	_, err := io.WriteString(c, value)
	return err == nil
}
func mail(c net.Conn, imap bool) {
	defer func() { c.Close() }()
	select {
	case capacity <- struct{}{}:
		defer func() { <-capacity }()
	default:
		return
	}
	active.Add(1)
	defer active.Add(-1)
	source, err := proxy(c)
	if err != nil {
		return
	}
	r := bufio.NewReaderSize(c, 32768)
	encrypted := false
	if imap {
		if !write(c, "* OK IMAP4rev1 fixture\r\n") {
			return
		}
	} else {
		if !write(c, "220 mail.test ESMTP fixture\r\n") {
			return
		}
	}
	for {
		if !imap {
			c.SetReadDeadline(time.Now().Add(5 * time.Minute))
		}
		raw, err := r.ReadString('\n')
		if err != nil || len(raw) > 32768 {
			return
		}
		fields := strings.Fields(raw)
		if len(fields) == 0 {
			continue
		}
		command := strings.ToUpper(fields[0])
		tag := ""
		if imap {
			if len(fields) < 2 {
				return
			}
			tag = fields[0]
			command = strings.ToUpper(fields[1])
		}
		if command == "STARTTLS" && !encrypted {
			reply := "220 Ready to start TLS\r\n"
			if imap {
				reply = tag + " OK Begin TLS\r\n"
			}
			if !write(c, reply) {
				return
			}
			if r.Buffered() != 0 {
				return
			} // STARTTLS must not be pipelined.
			secured := tls.Server(c, serverTLS())
			if secured.Handshake() != nil {
				return
			}
			c = secured
			r = bufio.NewReaderSize(c, 32768)
			encrypted = true
			starttlsCount.Add(1)
			continue
		}
		if imap {
			imapCount.Add(1)
			if command == "IDLE" {
				if !write(c, "+ idling\r\n") {
					return
				}
				done, err := r.ReadString('\n')
				if err != nil || strings.TrimSpace(done) != "DONE" {
					return
				}
				if !write(c, tag+" OK IDLE terminated\r\n") {
					return
				}
				continue
			}
			if command == "CAPABILITY" {
				if !write(c, "* CAPABILITY IMAP4rev1 STARTTLS IDLE\r\n"+tag+" OK CAPABILITY\r\n") {
					return
				}
				continue
			}
			if !write(c, tag+" OK "+source+"\r\n") {
				return
			}
			if command == "LOGOUT" {
				return
			}
		} else {
			smtpCount.Add(1)
			switch command {
			case "EHLO":
				if !write(c, "250-mail.test\r\n250-X-CLIENT-IP "+source+"\r\n250 STARTTLS\r\n") {
					return
				}
			case "DATA":
				if !write(c, "354 End with dot\r\n") {
					return
				}
				total := 0
				for {
					line, err := r.ReadString('\n')
					if err != nil {
						return
					}
					total += len(line)
					if total > 1048576 {
						return
					}
					if line == ".\r\n" {
						break
					}
				}
				if !write(c, "250 Queued fixture\r\n") {
					return
				}
			case "QUIT":
				write(c, "221 Bye\r\n")
				return
			default:
				if !write(c, "250 OK\r\n") {
					return
				}
			}
		}
	}
}
func web(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/stats":
		json.NewEncoder(w).Encode(map[string]any{"proxy": proxyCount.Load(), "local": localCount.Load(), "invalid": invalidCount.Load(), "smtp": smtpCount.Load(), "imap": imapCount.Load(), "starttls": starttlsCount.Load(), "reloads": reloads.Load(), "active": active.Load()})
		return
	case "/upload":
		n, err := io.Copy(io.Discard, http.MaxBytesReader(w, r.Body, 16*1024*1024))
		if err != nil {
			http.Error(w, "body limit", 413)
			return
		}
		json.NewEncoder(w).Encode(map[string]int64{"bytes": n})
		return
	}
	if strings.HasPrefix(r.URL.Path, "/bytes/") || strings.HasPrefix(r.URL.Path, "/stream/") {
		parts := strings.Split(r.URL.Path, "/")
		size, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil || size < 0 || size > 16*1024*1024 {
			http.Error(w, "size", 400)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Length", strconv.Itoa(size))
		for size > 0 {
			count := min(size, len(payload))
			// Fixed 'x' bytes, no request data or HTML; binary MIME with nosniff.
			// nosemgrep: go.lang.security.audit.xss.no-direct-write-to-responsewriter.no-direct-write-to-responsewriter
			if _, err = w.Write(payload[:count]); err != nil {
				return
			}
			size -= count
			if strings.HasPrefix(r.URL.Path, "/stream/") {
				http.NewResponseController(w).Flush()
			}
		}
		return
	}
	source := r.RemoteAddr
	if host, _, err := net.SplitHostPort(source); err == nil {
		source = host
	}
	json.NewEncoder(w).Encode(map[string]string{"source": source, "backend": "mail-fixture", "path": r.URL.Path})
}
func main() {
	for port, imap := range map[string]bool{":1025": false, ":1587": false, ":1143": true} {
		listener := listen(port)
		go func(imap bool) {
			for {
				c, err := listener.Accept()
				if err != nil {
					return
				}
				if tcp, ok := c.(*net.TCPConn); ok {
					tcp.SetNoDelay(true)
				}
				go mail(c, imap)
			}
		}(imap)
	}
	for _, port := range []string{":8080", ":8081"} {
		listener := listen(port)
		if port == ":8080" {
			listener = newProxyListener(listener)
		}
		server := &http.Server{ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, WriteTimeout: 30 * time.Second, Handler: http.HandlerFunc(web)}
		go server.Serve(listener)
	}
	token, err := os.ReadFile("/cases/reload-token")
	if err != nil {
		log.Fatal(err)
	}
	server := &http.Server{ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != "POST" || r.URL.Path != "/reload" || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+strings.TrimSpace(string(token)))) != 1 {
			http.Error(w, "denied", 403)
			return
		}
		var request map[string]any
		if json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&request) != nil {
			http.Error(w, "invalid JSON", 400)
			return
		}
		// Fault injection: a successful notification is deliberately not an
		// acknowledgment until the subsequent TLS fingerprint probe succeeds.
		if _, err := os.Stat("/cases/reload-suppressed"); err == nil {
			json.NewEncoder(w).Encode(map[string]bool{"accepted": true})
			return
		}
		digest, err := loadCertificate()
		if err != nil {
			http.Error(w, "certificate unavailable", 503)
			return
		}
		reloads.Add(1)
		json.NewEncoder(w).Encode(map[string]string{"fingerprint_sha256": digest})
	})}
	log.Fatal(server.ServeTLS(listen(":8777"), "/cases/api.pem", "/cases/api.key"))
}
