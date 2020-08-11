package datastore

import (
	"encoding/json"

	gds "cloud.google.com/go/datastore"
	"github.com/dexidp/dex/storage"
)

// KeysJSONWrapper is a utility which strips all the structure of the saved object
// and stores it as an opaque JSON blog. Some fields used by dex have map[string]....
// which isn't supported by datastore; dex do however only do CRUD + list operations
// no seaches so an opaque blob would do for all our use cases.
type KeysJSONWrapper struct {
	storage.Keys
}

// Load implements datastore PropertyLoadSaver Interface
func (x *KeysJSONWrapper) Load(ps []gds.Property) error {
	b := ps[0].Value.([]byte)
	err := json.Unmarshal(b, &x.Keys)
	return err
}

// Save implements datastore PropertyLoadSaver Interface
func (x *KeysJSONWrapper) Save() ([]gds.Property, error) {
	b, err := json.Marshal(&x.Keys)
	if err != nil {
		return nil, err
	}
	return []gds.Property{
		{
			Name:    "JSON",
			Value:   b,
			NoIndex: true,
		},
	}, nil
}

// OfflineSessionsJSONWrapper is a utility which strips all the structure of the saved object
// and stores it as an opaque JSON blog. Some fields used by dex have map[string]....
// which isn't supported by datastore; dex do however only do CRUD + list operations
// no seaches so an opaque blob would do for all our use cases.
type OfflineSessionsJSONWrapper struct {
	storage.OfflineSessions
}

// Load implements datastore PropertyLoadSaver Interface
func (x *OfflineSessionsJSONWrapper) Load(ps []gds.Property) error {
	b := ps[0].Value.([]byte)
	err := json.Unmarshal(b, &x.OfflineSessions)
	return err
}

// Save implements datastore PropertyLoadSaver Interface
func (x *OfflineSessionsJSONWrapper) Save() ([]gds.Property, error) {
	b, err := json.Marshal(x.OfflineSessions)
	if err != nil {
		return nil, err
	}
	return []gds.Property{
		{
			Name:    "JSON",
			Value:   b,
			NoIndex: true,
		},
	}, nil
}
