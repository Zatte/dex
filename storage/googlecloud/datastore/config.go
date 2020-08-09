package datastore

import (
	"context"

	googleDatastore "cloud.google.com/go/datastore"
	"github.com/dexidp/dex/pkg/log"
	"github.com/dexidp/dex/storage"
)

const (
	DatastoreHost    = "DATASTORE_EMULATOR_HOST"
	DatastoreProject = "DATASTORE_PROJECT_ID"
)

// Config is an implementation of a storage configuration.
type Config struct {
	ctx        context.Context
	projectID  string
	kindPrefix string
}

// Open always returns a new in memory storage.
func (c *Config) Open(logger log.Logger) (storage.Storage, error) {
	ctx, cancelFn := context.WithCancel(c.ctx)
	client, err := googleDatastore.NewClient(ctx, c.projectID)
	if err != nil {
		return nil, err
	}
	return &datastore{
		ctx:        ctx,
		kindPrefix: c.kindPrefix,
		logger:     logger,
		client:     client,
		cancelFn:   cancelFn,
	}, nil
}
