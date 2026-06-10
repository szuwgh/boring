package core

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/szuwgh/boring/core/config"
	"github.com/szuwgh/boring/core/consumer/sshforward"
	"github.com/szuwgh/boring/core/producer/tcp"
	"github.com/szuwgh/boring/core/stream"
	"golang.org/x/crypto/ssh"
)

func TestStreamPipelineForwardsTCPThroughSSH(t *testing.T) {
	echoAddr, stopEcho := startEchoServer(t)
	defer stopEcho()

	sshAddr, stopSSH := startSSHServer(t, "unvdb", "secret")
	defer stopSSH()

	reg := NewRegister()
	reg.RegisterStreamProducer("tcp_listener", stream.MakeProducerBuilder(tcp.ListenerBuilder))
	reg.RegisterStreamConsumer("ssh_forward", stream.MakeConsumerBuilder(sshforward.Builder))

	engine, err := NewEngineFromConfig(&config.Config{Boring: []config.BoringConfig{{
		Name:     "dev-ssh",
		Mode:     "stream",
		Producer: map[string]any{"type": "tcp_listener", "listen": "127.0.0.1:0"},
		Consumers: []map[string]any{{
			"type":     "ssh_forward",
			"ssh_addr": sshAddr,
			"user":     "unvdb",
			"password": "secret",
			"target":   echoAddr,
		}},
	}}}, reg)
	if err != nil {
		t.Fatalf("NewEngineFromConfig returned error: %v", err)
	}

	engine.Run()
	defer engine.Stop()

	addrProvider, ok := engine.stream["dev-ssh"].Producer.(interface{ Addr() net.Addr })
	if !ok {
		t.Fatalf("stream producer does not expose Addr")
	}

	conn, err := net.Dial("tcp", addrProvider.Addr().String())
	if err != nil {
		t.Fatalf("dial tunnel: %v", err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}

	payload := []byte("ping through stream pipeline")
	if _, err := conn.Write(payload); err != nil {
		t.Fatalf("write tunnel: %v", err)
	}

	got := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read echoed payload: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("expected %q, got %q", payload, got)
	}
}

func startEchoServer(t *testing.T) (string, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen echo server: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				_, _ = io.Copy(c, c)
			}(conn)
		}
	}()
	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

func startSSHServer(t *testing.T, user, password string) (string, func()) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate host key: %v", err)
	}
	signer, err := ssh.NewSignerFromKey(key)
	if err != nil {
		t.Fatalf("create host signer: %v", err)
	}
	serverConfig := &ssh.ServerConfig{
		PasswordCallback: func(conn ssh.ConnMetadata, pass []byte) (*ssh.Permissions, error) {
			if conn.User() == user && string(pass) == password {
				return nil, nil
			}
			return nil, fmt.Errorf("password rejected")
		},
	}
	serverConfig.AddHostKey(signer)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ssh server: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveSSHConn(conn, serverConfig)
		}
	}()

	return ln.Addr().String(), func() {
		_ = ln.Close()
		<-done
	}
}

type directTCPIPRequest struct {
	DestAddr   string
	DestPort   uint32
	OriginAddr string
	OriginPort uint32
}

func serveSSHConn(raw net.Conn, config *ssh.ServerConfig) {
	serverConn, chans, reqs, err := ssh.NewServerConn(raw, config)
	if err != nil {
		_ = raw.Close()
		return
	}
	defer serverConn.Close()
	go ssh.DiscardRequests(reqs)

	for ch := range chans {
		if ch.ChannelType() != "direct-tcpip" {
			_ = ch.Reject(ssh.UnknownChannelType, "unsupported channel type")
			continue
		}

		var req directTCPIPRequest
		if err := ssh.Unmarshal(ch.ExtraData(), &req); err != nil {
			_ = ch.Reject(ssh.ConnectionFailed, "bad direct-tcpip request")
			continue
		}
		targetAddr := net.JoinHostPort(req.DestAddr, strconv.Itoa(int(req.DestPort)))
		target, err := net.Dial("tcp", targetAddr)
		if err != nil {
			_ = ch.Reject(ssh.ConnectionFailed, err.Error())
			continue
		}

		channel, requests, err := ch.Accept()
		if err != nil {
			_ = target.Close()
			continue
		}
		go ssh.DiscardRequests(requests)
		go proxyTestConn(channel, target)
	}
}

func proxyTestConn(a, b io.ReadWriteCloser) {
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(a, b)
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(b, a)
		done <- struct{}{}
	}()
	<-done
	_ = a.Close()
	_ = b.Close()
}
