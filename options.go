package nats

import (
	nats "github.com/nats-io/nats.go"
	"go.unistack.org/micro/v3/broker"
)

type optionsKey struct{}
type drainConnectionKey struct{}

// Options accepts nats.Options
func Options(opts nats.Options) broker.Option {
	return broker.SetOption(optionsKey{}, opts)
}

// DrainConnection will drain subscription on close
func DrainConnection() broker.Option {
	return broker.SetOption(drainConnectionKey{}, struct{}{})
}
