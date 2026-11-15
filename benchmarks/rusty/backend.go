// Correctness fixture: binary PROXY v2 must precede a verified mTLS handshake.
package main

import (
    "bytes"
    "crypto/tls"
    "crypto/x509"
    "encoding/binary"
    "fmt"
    "io"
    "log"
    "net"
    "net/http"
    "os"
    "time"
)

type clientConn struct { net.Conn; source *net.TCPAddr }
func (c *clientConn) RemoteAddr() net.Addr { return c.source }
type proxyListener struct { net.Listener }
func (l proxyListener) Accept() (net.Conn,error) {
    for {
        c,err:=l.Listener.Accept(); if err!=nil {return nil,err}
        c.SetReadDeadline(time.Now().Add(3*time.Second))
        var header [16]byte
        if _,err=io.ReadFull(c,header[:]);err!=nil {c.Close();continue}
        length:=int(binary.BigEndian.Uint16(header[14:16]))
        if !bytes.Equal(header[:12],[]byte("\r\n\r\n\x00\r\nQUIT\n")) || header[12]!=0x21 || length>36 {c.Close();continue}
        payload:=make([]byte,length); if _,err=io.ReadFull(c,payload);err!=nil {c.Close();continue}
        var source *net.TCPAddr
        switch {
        case header[13]==0x11 && length==12:
            source=&net.TCPAddr{IP:net.IP(payload[:4]),Port:int(binary.BigEndian.Uint16(payload[8:10]))}
        case header[13]==0x21 && length==36:
            source=&net.TCPAddr{IP:net.IP(payload[:16]),Port:int(binary.BigEndian.Uint16(payload[32:34]))}
        default:c.Close();continue
        }
        c.SetReadDeadline(time.Time{})
        return &clientConn{c,source},nil
    }
}
func main() {
    cert,err:=tls.LoadX509KeyPair("/certs/cert.pem","/certs/key.pem");if err!=nil {log.Fatal(err)}
    ca,err:=os.ReadFile("/certs/ca.pem");if err!=nil {log.Fatal(err)}
    roots:=x509.NewCertPool();if !roots.AppendCertsFromPEM(ca) {log.Fatal("invalid CA")}
    config:=&tls.Config{Certificates:[]tls.Certificate{cert},ClientCAs:roots,ClientAuth:tls.RequireAndVerifyClientCert,MinVersion:tls.VersionTLS12,NextProtos:[]string{"http/1.1"}}
    // Private benchmark fixture: declared JSON/plain text or fixed binary data; all-interface listener is confined to its Docker test network.
    // nosemgrep: go.lang.security.audit.net.bind_all.avoid-bind-to-all-interfaces
    listener,err:=net.Listen("tcp",":8443");if err!=nil {log.Fatal(err)}
    server:=&http.Server{ReadHeaderTimeout:3*time.Second,IdleTimeout:30*time.Second,Handler:http.HandlerFunc(func(w http.ResponseWriter,r *http.Request) {
        if len(r.TLS.VerifiedChains)==0 {http.Error(w,"mTLS required",403);return}
        w.Header().Set("content-type","text/plain")
        // Private benchmark fixture: declared JSON/plain text or fixed binary data; all-interface listener is confined to its Docker test network.
        // nosemgrep: go.lang.security.audit.xss.no-fprintf-to-responsewriter.no-fprintf-to-responsewriter
        fmt.Fprint(w,r.RemoteAddr)
    })}
    log.Fatal(server.Serve(tls.NewListener(proxyListener{listener},config)))
}
