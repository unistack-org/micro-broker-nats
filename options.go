package nats

import (
	"golang.org/x/net/context"

	"github.com/micro/go-micro/broker"
	"github.com/nats-io/nats"
)

type optionsKey struct{}

// Options allow to inject a nats.Options struct for configuring
// the nats connection
func Options(nopts nats.Options) broker.Option {
	return func(o *broker.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		o.Context = context.WithValue(o.Context, optionsKey{}, nopts)
	}
}
