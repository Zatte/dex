package datastore

import (
	"context"

	googleDatastore "cloud.google.com/go/datastore"
	"github.com/zatte/dex/pkg/log"
	"github.com/zatte/dex/storage"
)

const (
	DatastoreHost    = "DATASTORE_EMULATOR_HOST"
	DatastoreProject = "DATASTORE_PROJECT_ID"
)

// Config is an implementation of a storage configuration.
type Config struct {
	ProjectID  string `json:"projectId" yaml:"projectId"`
	KindPrefix string `json:"kindPrefix" yaml:"kindPrefix"`
}

// Open always returns a new in memory storage.
func (c *Config) Open(logger log.Logger) (storage.Storage, error) {
	ctx, cancelFn := context.WithCancel(context.Background())
	client, err := googleDatastore.NewClient(ctx, c.ProjectID)
	if err != nil {
		cancelFn()
		return nil, err
	}
	return &datastore{
		ctx:        ctx,
		kindPrefix: c.KindPrefix,
		logger:     logger,
		client:     client,
		cancelFn:   cancelFn,
	}, nil
}
