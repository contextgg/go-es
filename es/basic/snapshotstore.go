package basic

import (
	"context"

	"github.com/contextgg/go-es/es"
)

// NewSnapshotStore create boring snapshot
func NewSnapshotStore() es.SnapshotStore {
	return &snapshotStore{}
}

type snapshotStore struct {
}

func (b *snapshotStore) Save(ctx context.Context, previousVersion int, aggregate es.Aggregate) error {
	return nil
}

func (b *snapshotStore) Load(ctx context.Context, aggregate es.Aggregate) error {
	return nil
}

// Close underlying connection
func (b *snapshotStore) Close() {
}
