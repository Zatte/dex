// Package datastore provides an in datastore implementation of the storage interface.
package datastore

import (
	"context"
	"strings"
	"time"

	gds "cloud.google.com/go/datastore"
	"github.com/dexidp/dex/pkg/log"
	"github.com/dexidp/dex/storage"
)

type datastore struct {
	ctx        context.Context
	logger     log.Logger
	kindPrefix string
	client     *gds.Client
	cancelFn   context.CancelFunc
}

type offlineSessionID struct {
	userID string
	connID string
}

const (
	keysId = "allKeys"
)

func (s *datastore) Close() error {
	s.cancelFn()
	return nil
}

func (s *datastore) GarbageCollect(now time.Time) (result storage.GCResult, err error) {

	var keysToDelete []*gds.Key

	var authCodes []storage.AuthCode
	if _, err := s.client.GetAll(s.ctx, gds.NewQuery(s.kindPrefix+"AuthCode"), &authCodes); err != nil {
		return result, err
	}
	for _, ac := range authCodes {
		if now.After(ac.Expiry) {
			keysToDelete = append(keysToDelete, gds.NameKey(s.kindPrefix+"AuthCode", ac.ID, nil))
			result.AuthCodes++
		}
	}

	var authReq []storage.AuthRequest
	if _, err := s.client.GetAll(s.ctx, gds.NewQuery(s.kindPrefix+"AuthRequest"), &authReq); err != nil {
		return result, err
	}
	for _, ac := range authReq {
		if now.After(ac.Expiry) {
			keysToDelete = append(keysToDelete, gds.NameKey(s.kindPrefix+"AuthRequest", ac.ID, nil))
			result.AuthRequests++
		}
	}

	return result, s.client.DeleteMulti(s.ctx, keysToDelete)
}

func (s *datastore) create(key *gds.Key, entity interface{}, maybeExisting interface{}) (err error) {
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		return s.createTx(tx, key, entity, maybeExisting)
	})
	return err
}

func (s *datastore) createTx(tx *gds.Transaction, key *gds.Key, entity interface{}, maybeExisting interface{}) (err error) {
	if err := tx.Get(key, maybeExisting); err == nil {
		return storage.ErrAlreadyExists
	} else if err != gds.ErrNoSuchEntity {
		return err
	}

	if _, err := tx.Put(key, entity); err != nil {
		return err
	}
	return err
}

func (s *datastore) update(key *gds.Key, entity interface{}) (err error) {
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		return s.updateTx(tx, key, entity)
	})
	return err
}
func (s *datastore) updateTx(tx *gds.Transaction, key *gds.Key, entity interface{}) (err error) {
	if _, err := tx.Put(key, entity); err != nil {
		if err == gds.ErrNoSuchEntity {
			return storage.ErrNotFound
		}
		return err
	}
	return nil
}

func (s *datastore) get(key *gds.Key, dst interface{}) (err error) {
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		return s.getTx(tx, key, dst)
	})
	return err
}
func (s *datastore) getTx(tx *gds.Transaction, key *gds.Key, dst interface{}) (err error) {
	if err := tx.Get(key, dst); err != nil {
		if err == gds.ErrNoSuchEntity {
			return storage.ErrNotFound
		}
	}
	return err
}

func (s *datastore) query(entityKey string, dst interface{}) (err error) {
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		return s.queryTx(tx, entityKey, dst)
	})
	return err
}
func (s *datastore) queryTx(tx *gds.Transaction, entityKey string, dst interface{}) (err error) {
	if _, err := s.client.GetAll(s.ctx, gds.NewQuery(entityKey), dst); err != nil {
		return err
	}
	return nil
}

func (s *datastore) delete(key *gds.Key, old interface{}) (err error) {
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		return s.deleteTx(tx, key, old)
	})
	return err
}
func (s *datastore) deleteTx(tx *gds.Transaction, key *gds.Key, old interface{}) (err error) {
	if err := s.getTx(tx, key, old); err != nil {
		return err
	}
	if err := tx.Delete(key); err != nil {
		if err == gds.ErrNoSuchEntity {
			return storage.ErrNotFound
		}
		return err
	}
	return nil
}

func (s *datastore) CreateClient(c storage.Client) (err error) {
	return s.create(
		gds.NameKey(s.kindPrefix+"Client", c.ID, nil),
		&c,
		&storage.Client{},
	)
}

func (s *datastore) CreateAuthCode(ac storage.AuthCode) (err error) {
	return s.create(
		gds.NameKey(s.kindPrefix+"AuthCode", ac.ID, nil),
		&ac,
		&storage.AuthCode{},
	)
}

func (s *datastore) CreateRefresh(r storage.RefreshToken) (err error) {
	return s.create(
		gds.NameKey(s.kindPrefix+"RefreshToken", r.ID, nil),
		&r,
		&storage.RefreshToken{},
	)
}

func (s *datastore) CreateAuthRequest(a storage.AuthRequest) (err error) {
	return s.create(
		gds.NameKey(s.kindPrefix+"AuthRequest", a.ID, nil),
		&a,
		&storage.AuthRequest{},
	)
}

func (s *datastore) CreatePassword(p storage.Password) (err error) {
	email := strings.ToLower(p.Email)
	return s.create(
		gds.NameKey(s.kindPrefix+"Password", email, nil),
		&p,
		&storage.Password{},
	)
}

func (s *datastore) CreateOfflineSessions(o storage.OfflineSessions) (err error) {
	id := o.UserID + "|" + o.ConnID // TODO: maybe fix into something smarter
	jsonContainer, err := NewOfflineSessions(o)
	if err != nil {
		return err
	}
	return s.create(
		gds.NameKey(s.kindPrefix+"OfflineSessions", id, nil),
		jsonContainer,
		&OfflineSessions{},
	)
}

func (s *datastore) CreateConnector(connector storage.Connector) (err error) {
	return s.create(
		gds.NameKey(s.kindPrefix+"Connector", connector.ID, nil),
		&connector,
		&storage.Connector{},
	)
}

func (s *datastore) GetAuthCode(id string) (c storage.AuthCode, err error) {
	return c, s.get(
		gds.NameKey(s.kindPrefix+"AuthCode", id, nil),
		&c,
	)
}

func (s *datastore) GetPassword(email string) (p storage.Password, err error) {
	email = strings.ToLower(email)
	return p, s.get(
		gds.NameKey(s.kindPrefix+"Password", email, nil),
		&p,
	)
}

func (s *datastore) GetClient(id string) (client storage.Client, err error) {
	return client, s.get(
		gds.NameKey(s.kindPrefix+"Client", id, nil),
		&client,
	)
}

func (s *datastore) GetKeys() (keys storage.Keys, err error) {
	dsKeys := Keys{}
	err = s.get(
		gds.NameKey(s.kindPrefix+"Keys", keysId, nil),
		&dsKeys,
	)
	if err != nil {
		return keys, err
	}
	return dsKeys.ToStorageModel()
}

func (s *datastore) GetRefresh(id string) (tok storage.RefreshToken, err error) {
	return tok, s.get(
		gds.NameKey(s.kindPrefix+"RefreshToken", id, nil),
		&tok,
	)
}

func (s *datastore) GetAuthRequest(id string) (req storage.AuthRequest, err error) {
	return req, s.get(
		gds.NameKey(s.kindPrefix+"AuthRequest", id, nil),
		&req,
	)
}

func (s *datastore) GetOfflineSessions(userID string, connID string) (o storage.OfflineSessions, err error) {
	id := userID + "|" + connID // TODO: maybe fix into something smarter
	LocalSession := OfflineSessions{}
	err = s.get(
		gds.NameKey(s.kindPrefix+"OfflineSessions", id, nil),
		&LocalSession,
	)
	if err != nil {
		return o, err
	}

	return LocalSession.ToStorageModel()
}

func (s *datastore) GetConnector(id string) (connector storage.Connector, err error) {
	return connector, s.get(
		gds.NameKey(s.kindPrefix+"Connector", id, nil),
		&connector,
	)
}

func (s *datastore) ListClients() (clients []storage.Client, err error) {
	return clients, s.query(
		s.kindPrefix+"Client",
		&clients,
	)
}

func (s *datastore) ListRefreshTokens() (tokens []storage.RefreshToken, err error) {
	return tokens, s.query(
		s.kindPrefix+"RefreshToken",
		&tokens,
	)
}

func (s *datastore) ListPasswords() (passwords []storage.Password, err error) {
	return passwords, s.query(
		s.kindPrefix+"Password",
		&passwords,
	)
}

func (s *datastore) ListConnectors() (conns []storage.Connector, err error) {
	return conns, s.query(
		s.kindPrefix+"Connector",
		&conns,
	)
}

func (s *datastore) DeletePassword(email string) (err error) {
	email = strings.ToLower(email)
	return s.delete(gds.NameKey(s.kindPrefix+"Password", email, nil), storage.Password{})
}

func (s *datastore) DeleteClient(id string) (err error) {
	return s.delete(gds.NameKey(s.kindPrefix+"Client", id, nil), storage.Client{})
}

func (s *datastore) DeleteRefresh(id string) (err error) {
	return s.delete(gds.NameKey(s.kindPrefix+"RefreshToken", id, nil), storage.RefreshToken{})
}

func (s *datastore) DeleteAuthCode(id string) (err error) {
	return s.delete(gds.NameKey(s.kindPrefix+"AuthCode", id, nil), storage.AuthCode{})
}

func (s *datastore) DeleteAuthRequest(id string) (err error) {
	return s.delete(gds.NameKey(s.kindPrefix+"AuthRequest", id, nil), storage.AuthRequest{})
}

func (s *datastore) DeleteOfflineSessions(userID string, connID string) (err error) {
	id := userID + "|" + connID // TODO: maybe fix into something smarter
	return s.delete(gds.NameKey(s.kindPrefix+"OfflineSessions", id, nil), storage.OfflineSessions{})
}

func (s *datastore) DeleteConnector(id string) (err error) {
	return s.delete(gds.NameKey(s.kindPrefix+"Connector", id, nil), storage.Connector{})
}

func (s *datastore) UpdateClient(id string, updater func(old storage.Client) (storage.Client, error)) (err error) {
	key := gds.NameKey(s.kindPrefix+"Client", id, nil)
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		old := storage.Client{}
		if err := tx.Get(key, &old); err != nil && err != gds.ErrNoSuchEntity {
			return storage.ErrNotFound
		}
		if new, err := updater(old); err == nil {
			if _, err := tx.Put(key, &new); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (s *datastore) UpdateKeys(updater func(old storage.Keys) (storage.Keys, error)) (err error) {
	key := gds.NameKey(s.kindPrefix+"Keys", keysId, nil)
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		old := &Keys{}
		if err := tx.Get(key, old); err != nil && err != gds.ErrNoSuchEntity {
			return err
		}
		oldInStorgeFormat := storage.Keys{}
		if len(old.JSON) > 0 {
			oldInStorgeFormat, err = old.ToStorageModel()
			if err != nil {
				return err
			}
		}
		if new, err := updater(oldInStorgeFormat); err == nil {
			toBeSaved, err := NewKeys(new)
			if err != nil {
				return err
			}
			if _, err := tx.Put(key, toBeSaved); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (s *datastore) UpdateAuthRequest(id string, updater func(old storage.AuthRequest) (storage.AuthRequest, error)) (err error) {
	key := gds.NameKey(s.kindPrefix+"AuthRequest", id, nil)
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		old := storage.AuthRequest{}
		if err := tx.Get(key, &old); err != nil && err != gds.ErrNoSuchEntity {
			return storage.ErrNotFound
		}
		if new, err := updater(old); err == nil {
			if _, err := tx.Put(key, &new); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (s *datastore) UpdatePassword(email string, updater func(p storage.Password) (storage.Password, error)) (err error) {
	email = strings.ToLower(email)
	key := gds.NameKey(s.kindPrefix+"Password", email, nil)
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		old := storage.Password{}
		if err := tx.Get(key, &old); err != nil && err != gds.ErrNoSuchEntity {
			return storage.ErrNotFound
		}
		if new, err := updater(old); err == nil {
			if _, err := tx.Put(key, &new); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (s *datastore) UpdateRefreshToken(id string, updater func(p storage.RefreshToken) (storage.RefreshToken, error)) (err error) {
	key := gds.NameKey(s.kindPrefix+"RefreshToken", id, nil)
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		old := storage.RefreshToken{}
		if err := tx.Get(key, &old); err != nil && err != gds.ErrNoSuchEntity {
			return storage.ErrNotFound
		}
		if new, err := updater(old); err == nil {
			if _, err := tx.Put(key, &new); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (s *datastore) UpdateOfflineSessions(userID string, connID string, updater func(o storage.OfflineSessions) (storage.OfflineSessions, error)) (err error) {
	id := userID + "|" + connID // TODO: maybe fix into something smarter
	key := gds.NameKey(s.kindPrefix+"OfflineSessions", id, nil)
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		old := OfflineSessions{}
		if err := tx.Get(key, &old); err != nil && err != gds.ErrNoSuchEntity {
			return storage.ErrNotFound
		}

		oldInStorgeFormat := storage.OfflineSessions{}
		if len(old.JSON) > 0 {
			oldInStorgeFormat, err = old.ToStorageModel()
			if err != nil {
				return err
			}
		}

		if new, err := updater(oldInStorgeFormat); err == nil {
			toBeSaved, err := NewOfflineSessions(new)
			if err != nil {
				return err
			}
			if _, err := tx.Put(key, toBeSaved); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

func (s *datastore) UpdateConnector(id string, updater func(c storage.Connector) (storage.Connector, error)) (err error) {
	key := gds.NameKey(s.kindPrefix+"Connector", id, nil)
	_, err = s.client.RunInTransaction(s.ctx, func(tx *gds.Transaction) error {
		old := storage.Connector{}
		if err := tx.Get(key, &old); err != nil && err != gds.ErrNoSuchEntity {
			return storage.ErrNotFound
		}
		if new, err := updater(old); err == nil {
			if _, err := tx.Put(key, &new); err != nil {
				return err
			}
		}
		return nil
	})
	return err
}
