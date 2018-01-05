package nats

import (
	"github.com/micro/go-micro/broker"
	"github.com/nats-io/nats"
)

var (
	DefaultNatsOptions = nats.GetDefaultOptions()

	optionsKey = optionsKeyType{}
)

type optionsKeyType struct{}

type brokerOptions struct {
	natsOptions nats.Options
}

// NatsOptions allow to inject a nats.Options struct for configuring
// the nats connection
func NatsOptions(nopts nats.Options) broker.Option {
	return func(o *broker.Options) {
		no := o.Context.Value(optionsKey).(*brokerOptions)
		no.natsOptions = nopts
	}
}
