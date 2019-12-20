package examples

import (
	"context"
	"log"
	"testing"
	"time"

	"github.com/contextgg/go-es/builder"
	"github.com/contextgg/go-es/es"
)

type LoggedIn struct {
	Username string
}
type LoggedOut struct{}

// Auth aggregate
type Auth struct {
	es.BaseAggregate

	Username string `bson:"username"`
	LoggedIn bool   `bson:"logged_in"`
}

func (a *Auth) HandleCommand(ctx context.Context, cmd es.Command) error {
	switch c := cmd.(type) {
	case *Login:
		// store some events!.
		a.StoreEvent(&LoggedIn{
			Username: c.Username,
		})
	case *Logout:
		// store some events!
		a.StoreEvent(&LoggedOut{})
	}
	return nil
}
func (a *Auth) ApplyEvent(ctx context.Context, event interface{}) error {
	switch e := event.(type) {
	case *LoggedIn:
		// store some events!.
		a.Username = e.Username
		a.LoggedIn = true
	case *LoggedOut:
		a.LoggedIn = false
	}
	return nil
}

// Login is a command
type Login struct {
	es.BaseCommand

	Username string
}

// Logout is a command too
type Logout struct {
	es.BaseCommand
}

func TestStuff(t *testing.T) {
	// store := builder.Mongo("mongodb://localhost:27017", "test", 0)
	// eventbus := builder.Nats("nats://localhost:4222", "identity-auth")

	b := builder.NewClientBuilder()
	b.RegisterEvent(
		builder.Event(&LoggedIn{}, false),
		builder.Event(&LoggedOut{}, false),
	)
	b.WireAggregate(
		builder.Aggregate(&Auth{}),
		builder.Command(&Login{}),
		builder.Command(&Logout{}),
	)
	b.SetEventStore(
		builder.LocalStore(),
	)
	// b.SetEventStore(
	// 	builder.Mongo("mongodb://localhost:27017", "test", 0),
	// )
	// b.AddPublisher(
	// 	builder.Nats("nats://localhost:4222", "test"),
	// )

	cli, err := b.Build()
	if err != nil {
		t.Error(err)
		return
	}
	defer cli.Close()

	bus := cli.CommandBus

	ctx := context.Background()

	cmd1 := &Login{
		BaseCommand: es.BaseCommand{AggregateID: "132"},
		Username:    "demouser",
	}
	if err := bus.HandleCommand(ctx, cmd1); err != nil {
		log.Fatal(err)
	}

	cmd2 := &Logout{
		BaseCommand: es.BaseCommand{AggregateID: "132"},
	}
	if err := bus.HandleCommand(ctx, cmd2); err != nil {
		log.Fatal(err)
	}

	time.Sleep(1)
}
