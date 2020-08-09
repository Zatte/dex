package datastore

import (
	"encoding/json"

	"github.com/dexidp/dex/storage"
)

// Keys replaces the strcuture of Keys since *jose.JSONWebKey uses interface{}
// which datastore doesn't handle well but does implement a good
// JSON marshaler we will piggy back on it.
type Keys struct {
	JSON []byte `datastore:",noindex"`
}

func NewKeys(keys storage.Keys) (*Keys, error) {
	js, err := json.Marshal(keys)
	if err != nil {
		return nil, err
	}
	return &Keys{
		JSON: js,
	}, nil
}

func (k *Keys) ToStorageModel() (res storage.Keys, err error) {
	return res, json.Unmarshal(k.JSON, &res)
}

// OfflineSessions uses a map[string]Storage.Refreshtoken which datastore objects to
// as suche we also turn it into a json string.
type OfflineSessions struct {
	JSON []byte `datastore:",noindex"`
}

func NewOfflineSessions(sessions storage.OfflineSessions) (*OfflineSessions, error) {
	js, err := json.Marshal(sessions)
	if err != nil {
		return nil, err
	}
	return &OfflineSessions{
		JSON: js,
	}, nil
}

func (k *OfflineSessions) ToStorageModel() (res storage.OfflineSessions, err error) {
	return res, json.Unmarshal(k.JSON, &res)
}
