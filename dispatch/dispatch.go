// Package dispatch identifies dates to reprocess, and feeds them into
// the reprocessing network.
package dispatch

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"time"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/option"
)

// ###############################################################################
//  Ownership related code
// ###############################################################################

const (
	// DSNamespace is the namespace for all gardener related DataStore entities.
	DSNamespace = "Gardener"

	// DSOwnerLeaseName is the name of the single OwnerLease object.
	DSOwnerLeaseName = "OwnerLease"
)

var (
	// TestMode controls datastore retry delays to allow faster testing.
	TestMode = false
)

// Errors for leases
var (
	ErrLostLease          = errors.New("lost ownership lease")
	ErrNotOwner           = errors.New("owner does not match instance")
	ErrOwnershipRequested = errors.New("another instance has requested ownership")
	ErrInvalidState       = errors.New("invalid owner lease state")
	ErrNoSuchLease        = errors.New("lease does not exist")
	ErrNotAvailable       = errors.New("lease not available")

	ErrNoProject = errors.New("PROJECT environment variable not set")
)

// OwnerLease is a DataStore record that controls ownership of the reprocessing task.
// An instance must own this before doing any reprocessing / dedupping operations.
// It should be periodically renewed to avoid another instance taking ownership
// without a handshake.
// TODO - should this have a mutex?
type OwnerLease struct {
	InstanceID      string    // instance ID of the owner.
	LeaseExpiration time.Time // Time that the lease will expire.
	NewInstanceID   string    // ID of instance trying to assume ownership.

	// These are not saved or retrieved from DataStore
	namespace string
	kind      string
	client    *datastore.Client
}

// NewOwnerLease returns a properly initialized OwnerLease object.
// Note this creates a datastore client, which may do network operations.
func NewOwnerLease(instance, namespace, kind string, opts ...option.ClientOption) (*OwnerLease, error) {
	var err error
	var client *datastore.Client
	project, ok := os.LookupEnv("PROJECT")
	if !ok {
		return nil, ErrNoProject
	}
	log.Println(project)
	client, err = datastore.NewClient(context.Background(), project, opts...)
	if err != nil {
		return nil, err
	}

	return &OwnerLease{instance, time.Now().Add(5 * time.Minute), "", namespace, kind, client}, nil
}

// NameKey creates a full key using the Saver settings.
func (ol *OwnerLease) nameKey(name string) *datastore.Key {
	k := datastore.NameKey(ol.kind, name, nil)
	k.Namespace = ol.namespace
	return k
}

// Validate checks that fields have been initialized.
func (ol *OwnerLease) validate() error {
	if ol.NewInstanceID != "" {
		return ErrOwnershipRequested
	}
	if ol.InstanceID == "" {
		log.Printf("%+v\n", ol)
		return ErrInvalidState
	}
	return nil
}

// Renew renews the ownership lease for interval.
// The receiver must have InstanceID already set.
// TODO - should this run on a timer, or in a go routine?
func (ol *OwnerLease) Renew(interval time.Duration) error {
	err := ol.validate()
	if err != nil {
		log.Println(err)
		return err
	}
	k := ol.nameKey(DSOwnerLeaseName)
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err = ol.client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var lease OwnerLease
		err := tx.Get(k, &lease)
		if err != nil {
			return err
		}
		if lease.InstanceID != ol.InstanceID {
			log.Println(ol.InstanceID, "lost lease to", lease.InstanceID)
			return ErrLostLease
		}

		if lease.NewInstanceID != "" {
			log.Println(lease.InstanceID, "relinquishing ownership to", lease.NewInstanceID)
			if lease.LeaseExpiration.After(time.Now()) {
				lease.LeaseExpiration = time.Now()
				_, err = tx.Put(k, &lease)
				if err != nil {
					return err
				}
			}
			return ErrOwnershipRequested
		}

		lease.LeaseExpiration = time.Now().Add(interval)
		_, err = tx.Put(k, &lease)
		return err
	})
	return err
}

// TakeOwnershipIfAvailable assumes ownership if no-one else owns it.
func (ol *OwnerLease) takeOwnershipIfAvailable(interval time.Duration) error {
	k := ol.nameKey(DSOwnerLeaseName)
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err := ol.client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var lease OwnerLease
		err := tx.Get(k, &lease)
		// If lease is expired, or doesn't exist, go ahead and try to take it.
		if err == datastore.ErrNoSuchEntity || lease.LeaseExpiration.Before(time.Now()) {
			ol.LeaseExpiration = time.Now().Add(interval)
			ol.NewInstanceID = ""
			tx.Put(k, ol)
			return nil
		}
		return ErrNotAvailable
	})
	return err
}

// RequestLease sets the NewInstanceID field to indicate that we want ownership.
func (ol *OwnerLease) requestLease() error {
	k := ol.nameKey(DSOwnerLeaseName)
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err := ol.client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var lease OwnerLease
		err := tx.Get(k, &lease)
		if err != nil {
			return err
		}
		if lease.NewInstanceID == "" {
			lease.NewInstanceID = ol.InstanceID
			log.Println(ol.InstanceID, "requesting lease from", lease.InstanceID)
			_, err = tx.Put(k, &lease)
		}
		return err
	})
	return err
}

// WaitForOwnership retries TakeOwnershipIfAvailable until timeout or success.
func (ol *OwnerLease) waitForOwnership(interval time.Duration) error {
	for timeout := time.Now().Add(2 * time.Minute); time.Now().Before(timeout); {
		log.Println("Trying again to get ownership", ol.InstanceID)
		err := ol.takeOwnershipIfAvailable(interval)
		if err != ErrNotAvailable {
			return err
		}
		if TestMode {
			time.Sleep(time.Duration(1) * time.Second)
		} else {
			time.Sleep(time.Duration(5+rand.Intn(10)) * time.Second)
		}
	}

	return ErrNotAvailable
}

// Lease attempts to take ownership of the lease.
// Expected to be attempted at startup, and process should fail health check
// if this fails repeatedly.
func (ol *OwnerLease) Lease(interval time.Duration) error {
	if ol.validate() != nil {
		return ol.validate()
	}
	err := ol.takeOwnershipIfAvailable(interval)
	if err == nil {
		return err
	}
	err = ol.requestLease()
	if err != nil {
		return err
	}
	err = ol.waitForOwnership(interval)
	return err
}

// Delete deletes the lease iff held by ol.
func (ol *OwnerLease) Delete() error {
	k := ol.nameKey(DSOwnerLeaseName)
	ctx, cf := context.WithTimeout(context.Background(), 10*time.Second)
	defer cf()
	_, err := ol.client.RunInTransaction(ctx, func(tx *datastore.Transaction) error {
		var lease OwnerLease
		err := tx.Get(k, &lease)
		if err != nil {
			return err
		}
		if lease.InstanceID == ol.InstanceID {
			err = tx.Delete(k)
			return err
		}
		return ErrNotOwner
	})
	return err
}

// ###############################################################################
//  Dispatch interface and related code
// ###############################################################################
