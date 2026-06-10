package sshforward

import (
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/szuwgh/boring/core/stream"
	"golang.org/x/crypto/ssh"
)

type Consumer struct {
	config *Config
}

var _ stream.Consumer = (*Consumer)(nil)

func Builder(config *Config) stream.Consumer {
	return &Consumer{config: config}
}

func (c *Consumer) ConsumeConn(local net.Conn) error {
	defer local.Close()
	if err := c.validate(); err != nil {
		return err
	}

	sshClient, err := ssh.Dial("tcp", c.config.SSHAddr, c.clientConfig())
	if err != nil {
		log.Printf("tcp addr:%s,error:%s", c.config.SSHAddr, err)
		return fmt.Errorf("ssh_forward: ssh dial: %w", err)
	}
	defer sshClient.Close()

	remote, err := sshClient.Dial("tcp", c.config.Target)
	if err != nil {
		log.Printf("target dial addr:%s,error:%s", c.config.SSHAddr, err)
		return fmt.Errorf("ssh_forward: target dial: %w", err)
	}
	defer remote.Close()

	pipe(local, remote)
	return nil
}

func (c *Consumer) validate() error {
	if c.config == nil {
		return fmt.Errorf("ssh_forward: config is required")
	}
	if c.config.SSHAddr == "" {
		return fmt.Errorf("ssh_forward: ssh_addr is required")
	}
	if c.config.User == "" {
		return fmt.Errorf("ssh_forward: user is required")
	}
	if c.config.Password == "" {
		return fmt.Errorf("ssh_forward: password is required")
	}
	if c.config.Target == "" {
		return fmt.Errorf("ssh_forward: target is required")
	}
	return nil
}

func (c *Consumer) clientConfig() *ssh.ClientConfig {
	return &ssh.ClientConfig{
		User:            c.config.User,
		Auth:            []ssh.AuthMethod{ssh.Password(c.config.Password)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(),
		Timeout:         30 * time.Second,
	}
}

func pipe(a, b net.Conn) {
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
