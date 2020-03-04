package mongo

import (
	"context"
	"testing"

	"github.com/contextgg/go-es/es"
)

// SmashggEvent for testing
type SmashggEvent struct {
	es.BaseAggregateSourced
}

func TestLoadAggregate(t *testing.T) {
	db, err := Create("mongodb://localhost:27017", "smashgg", "", "", false)
	if err != nil {
		t.Error(err)
		return
	}

	store, err := NewStore(db, nil)
	if err != nil {
		t.Error(err)
		return
	}

	factory := es.NewAggregateFactory(&SmashggEvent{})
	aggregateStore := es.NewAggregateStore(factory, store, nil)
	aggregate, err := aggregateStore.LoadAggregate(context.TODO(), "384824")
	if err != nil {
		t.Error(err)
		return
	}

	if aggregate.GetID() != "384824" && aggregate.GetTypeName() != "SmashggEvent" {
		t.Error("Something isn't right")
		return
	}
}
