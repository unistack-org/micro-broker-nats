// Package nats provides a NATS broker
package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	nats "github.com/nats-io/nats.go"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/metadata"
	"golang.org/x/sync/errgroup"
)

var pPool = sync.Pool{
	New: func() interface{} {
		return &publication{msg: broker.NewMessage("")}
	},
}

type natsBroker struct {
	sync.Once
	sync.RWMutex

	// indicate if we're connected
	connected bool
	init      bool
	addrs     []string
	conn      *nats.Conn
	opts      broker.Options
	nopts     nats.Options

	// should we drain the connection
	drain   bool
	closeCh chan (error)
}

type publication struct {
	topic string
	err   error
	msg   *broker.Message
	ctx   context.Context
}

func (p *publication) Topic() string {
	return p.topic
}

func (p *publication) Message() *broker.Message {
	return p.msg
}

func (p *publication) Ack() error {
	// nats does not support acking
	return nil
}

func (p *publication) SetError(err error) {
	p.err = err
}

func (p *publication) Error() error {
	return p.err
}

func (p *publication) Context() context.Context {
	return p.ctx
}

type subscriber struct {
	s    *nats.Subscription
	opts broker.SubscribeOptions
}

func (s *subscriber) Options() broker.SubscribeOptions {
	return s.opts
}

func (s *subscriber) Topic() string {
	return s.s.Subject
}

func (s *subscriber) Unsubscribe(ctx context.Context) error {
	return s.s.Unsubscribe()
}

func (n *natsBroker) Address() string {
	if n.conn != nil && n.conn.IsConnected() {
		return n.conn.ConnectedUrl()
	}

	return strings.Join(n.addrs, ",")
}

func (n *natsBroker) setAddrs(addrs []string) []string {
	//nolint:prealloc
	var cAddrs []string
	for _, addr := range addrs {
		if len(addr) == 0 {
			continue
		}
		if !strings.HasPrefix(addr, "nats://") {
			addr = "nats://" + addr
		}
		cAddrs = append(cAddrs, addr)
	}
	if len(cAddrs) == 0 {
		cAddrs = []string{nats.DefaultURL}
	}
	return cAddrs
}

func (n *natsBroker) Connect(ctx context.Context) error {
	n.RLock()
	if n.connected {
		n.RUnlock()
		return nil
	}
	n.RUnlock()

	n.Lock()
	defer n.Unlock()
	status := nats.CLOSED
	if n.conn != nil {
		status = n.conn.Status()
	}

	switch status {
	case nats.CONNECTED, nats.RECONNECTING, nats.CONNECTING:
		n.connected = true
		return nil
	default: // DISCONNECTED or CLOSED or DRAINING
		opts := n.nopts
		opts.Servers = n.addrs
		opts.TLSConfig = n.opts.TLSConfig

		c, err := opts.Connect()
		if err != nil {
			if n.opts.Logger.V(logger.WarnLevel) {
				n.opts.Logger.Error(n.opts.Context, "error connecting to broker", err)
			}

			return err
		}
		n.conn = c
		n.connected = true
		return nil
	}
}

func (n *natsBroker) Disconnect(ctx context.Context) error {
	n.RLock()
	if !n.connected {
		n.RUnlock()
		return nil
	}
	n.RUnlock()

	n.Lock()
	defer n.Unlock()
	// drain the connection if specified
	if n.drain {
		if err := n.conn.Drain(); err != nil {
			return err
		}
		n.closeCh <- nil
	}

	// close the client connection
	n.conn.Close()

	// set not connected
	n.connected = false

	return nil
}

func (n *natsBroker) Init(opts ...broker.Option) error {
	if len(opts) == 0 && n.init {
		return nil
	}

	if err := n.opts.Register.Init(); err != nil {
		return err
	}
	if err := n.opts.Tracer.Init(); err != nil {
		return err
	}
	if err := n.opts.Logger.Init(); err != nil {
		return err
	}
	if err := n.opts.Meter.Init(); err != nil {
		return err
	}

	n.setOption(opts...)
	if n.opts.Codec == nil {
		return fmt.Errorf("codec is nil")
	}
	return nil
}

func (n *natsBroker) Options() broker.Options {
	return n.opts
}

func (n *natsBroker) BatchPublish(ctx context.Context, p []*broker.Message, opts ...broker.PublishOption) error {
	var err error
	msgs := make([]*nats.Msg, 0, len(p))
	var wg sync.WaitGroup

	wg.Add(len(p))

	options := broker.NewPublishOptions(opts...)

	for _, m := range p {
		rec := &nats.Msg{}
		rec.Subject, _ = m.Header.Get(metadata.HeaderTopic)
		if options.BodyOnly {
			rec.Data = m.Body
		} else if n.opts.Codec.String() == "noop" {
			rec.Data = m.Body
			rec.Header = make(nats.Header, len(m.Header))
			for k, v := range m.Header {
				rec.Header.Add(k, v)
			}
		} else {
			rec.Data, err = n.opts.Codec.Marshal(m)
			if err != nil {
				return err
			}
		}
		msgs = append(msgs, rec)
	}

	n.RLock()
	defer n.RUnlock()

	g := errgroup.Group{}

	for _, ms := range msgs {
		m := ms
		g.Go(func() error {
			return n.conn.PublishMsg(m)
		})
	}

	return g.Wait()
}

func (n *natsBroker) Publish(ctx context.Context, topic string, msg *broker.Message, opts ...broker.PublishOption) error {
	var err error

	n.RLock()
	if n.conn == nil {
		n.RUnlock()
		return errors.New("not connected")
	}
	n.RUnlock()

	options := broker.NewPublishOptions(opts...)

	rec := &nats.Msg{}
	rec.Subject, _ = msg.Header.Get(metadata.HeaderTopic)
	if options.BodyOnly {
		rec.Data = msg.Body
	} else if n.opts.Codec.String() == "noop" {
		rec.Data = msg.Body
		rec.Header = make(nats.Header, len(msg.Header))
		for k, v := range msg.Header {
			rec.Header.Add(k, v)
		}
	} else {
		rec.Data, err = n.opts.Codec.Marshal(msg)
		if err != nil {
			return err
		}
	}

	n.RLock()
	defer n.RUnlock()

	return n.conn.PublishMsg(rec)
}

func (n *natsBroker) BatchSubscribe(ctx context.Context, topic string, handler broker.BatchHandler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	return nil, nil
}

func (n *natsBroker) Subscribe(ctx context.Context, topic string, handler broker.Handler, opts ...broker.SubscribeOption) (broker.Subscriber, error) {
	n.RLock()
	if n.conn == nil {
		n.RUnlock()
		return nil, errors.New("not connected")
	}
	n.RUnlock()

	options := broker.NewSubscribeOptions(opts...)

	eh := n.opts.ErrorHandler
	if options.ErrorHandler != nil {
		eh = options.ErrorHandler
	}

	fn := func(msg *nats.Msg) {
		pub := pPool.Get().(*publication)
		pub.msg.Header = nil
		pub.msg.Body = nil
		pub.topic = msg.Subject
		pub.err = nil
		pub.ctx = ctx

		if options.BodyOnly {
			pub.msg.Body = msg.Data
		} else if n.opts.Codec.String() == "noop" {
			pub.msg.Body = msg.Data
			pub.msg.Header = metadata.New(len(msg.Header))
			for k, v := range msg.Header {
				pub.msg.Header.Set(k, strings.Join(v, ","))
			}
		} else {
			err := n.opts.Codec.Unmarshal(msg.Data, pub.msg)
			pub.err = err
			if err != nil {
				pub.msg.Body = msg.Data
				if eh != nil {
					eh(pub)
				} else {
					if n.opts.Logger.V(logger.ErrorLevel) {
						n.opts.Logger.Error(n.opts.Context, "handler error", err)
					}
				}
				return
			}
		}
		if err := handler(pub); err != nil {
			pub.err = err
			if eh != nil {
				eh(pub)
			} else {
				if n.opts.Logger.V(logger.ErrorLevel) {
					n.opts.Logger.Error(n.opts.Context, "handler error", err)
				}
			}
		}
	}

	var sub *nats.Subscription
	var err error

	n.RLock()
	if len(options.Group) > 0 {
		sub, err = n.conn.QueueSubscribe(topic, options.Group, fn)
	} else {
		sub, err = n.conn.Subscribe(topic, fn)
	}
	n.RUnlock()
	if err != nil {
		return nil, err
	}
	return &subscriber{s: sub, opts: options}, nil
}

func (n *natsBroker) String() string {
	return "nats"
}

func (n *natsBroker) Name() string {
	return n.opts.Name
}

func (n *natsBroker) setOption(opts ...broker.Option) {
	for _, o := range opts {
		o(&n.opts)
	}

	n.Once.Do(func() {
		n.nopts = nats.GetDefaultOptions()
	})

	if nopts, ok := n.opts.Context.Value(optionsKey{}).(nats.Options); ok {
		n.nopts = nopts
	}

	// broker.Options have higher priority than nats.Options
	// only if Addrs, Secure or TLSConfig were not set through a broker.Option
	// we read them from nats.Option
	if len(n.opts.Addrs) == 0 {
		n.opts.Addrs = n.nopts.Servers
	}

	if n.opts.TLSConfig == nil {
		n.opts.TLSConfig = n.nopts.TLSConfig
	}
	n.addrs = n.setAddrs(n.opts.Addrs)

	if n.opts.Context.Value(drainConnectionKey{}) != nil {
		n.drain = true
		n.closeCh = make(chan error)
		n.nopts.ClosedCB = n.onClose
		n.nopts.AsyncErrorCB = n.onAsyncError
		n.nopts.DisconnectedErrCB = n.onDisconnectedError
	}
}

func (n *natsBroker) onClose(conn *nats.Conn) {
	n.closeCh <- nil
}

func (n *natsBroker) onAsyncError(conn *nats.Conn, sub *nats.Subscription, err error) {
	// There are kinds of different async error nats might callback, but we are interested
	// in ErrDrainTimeout only here.
	if err == nats.ErrDrainTimeout {
		n.closeCh <- err
	}
}

func (n *natsBroker) onDisconnectedError(conn *nats.Conn, err error) {
	n.closeCh <- err
}

func NewBroker(opts ...broker.Option) *natsBroker {
	options := broker.NewOptions(opts...)

	if options.Codec.String() != "noop" {
		options.Logger.Info(options.Context, "broker codec not noop, disable plain nats headers usage")
	}

	n := &natsBroker{
		opts: options,
	}

	n.setOption(opts...)

	return n
}
