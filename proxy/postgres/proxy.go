package postgres

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/proto-tap/sql-tap/proxy"
)

var _ proxy.Proxy = (*Proxy)(nil)

// Proxy is a TCP proxy that sits between a PostgreSQL client and server,
// capturing query events from the wire protocol.
type Proxy struct {
	listenAddr   string
	upstreamAddr string
	events       chan proxy.Event
	listener     net.Listener
	wg           sync.WaitGroup
}

// New creates a new PostgreSQL proxy.
func New(listenAddr, upstreamAddr string) *Proxy {
	return &Proxy{
		listenAddr:   listenAddr,
		upstreamAddr: upstreamAddr,
		events:       make(chan proxy.Event, 256),
	}
}

// Events returns the channel of captured events.
func (p *Proxy) Events() <-chan proxy.Event {
	return p.events
}

// ListenAndServe starts accepting client connections and relaying them to PostgreSQL.
func (p *Proxy) ListenAndServe(ctx context.Context) error {
	var lc net.ListenConfig
	lis, err := lc.Listen(ctx, "tcp", p.listenAddr)
	if err != nil {
		return fmt.Errorf("postgres: listen: %w", err)
	}
	p.listener = lis

	go func() {
		<-ctx.Done()
		_ = lis.Close()
	}()

	for {
		clientConn, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("postgres: accept: %w", ctx.Err())
			}
			return fmt.Errorf("postgres: accept: %w", err)
		}

		p.wg.Go(func() {
			p.handleConn(ctx, clientConn)
		})
	}
}

// Close stops the proxy and waits for all connections to finish.
func (p *Proxy) Close() error {
	if p.listener != nil {
		if err := p.listener.Close(); err != nil {
			return fmt.Errorf("postgres: close listener: %w", err)
		}
	}
	p.wg.Wait()
	return nil
}

func (p *Proxy) handleConn(ctx context.Context, clientConn net.Conn) {
	defer func() { _ = clientConn.Close() }()

	var d net.Dialer
	upstreamConn, err := d.DialContext(ctx, "tcp", p.upstreamAddr)
	if err != nil {
		log.Printf("postgres: dial upstream %s: %v", p.upstreamAddr, err)
		return
	}
	defer func() { _ = upstreamConn.Close() }()

	c := newConn(clientConn, upstreamConn, p.events)
	if err := c.relay(ctx); err != nil {
		log.Printf("postgres: relay %s: %v", clientConn.RemoteAddr(), err)
	}
}
