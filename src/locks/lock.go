package locks

import (
	"Distributed-Lock-Manager/src/key-value"
	"fmt"
	"github.com/google/uuid"
	"time"
)

type Lock struct {
	name       string
	client     *key_value.Client
	retryDelay time.Duration
}

func NewLock(lockName *string, client *key_value.Client, retryDelayMs uint) (*Lock, error) {
	var name string
	if lockName == nil {
		name = uuid.NewString()
	} else {
		name = *lockName
	}

	if _, err := client.SetKeyValue(name, nil); err != nil {
		return nil, fmt.Errorf("failed to create lock: %s", err)
	}

	return &Lock{
		name:       name,
		client:     client,
		retryDelay: time.Duration(retryDelayMs) * time.Millisecond,
	}, nil
}

func (l *Lock) GetName() string {
	return l.name
}

func (l *Lock) Lock(clientId string) error {
	for {
		ok, err := l.nonBlockingLock(clientId)
		if err != nil {
			return err
		}

		if ok {
			break
		}

		time.Sleep(l.retryDelay)
	}

	return nil
}

func (l *Lock) Unlock(clientId string) error {
	oldValue := createLockValue(clientId)

	if _, _, err := l.client.CompareAndSetKeyValue(l.GetName(), oldValue, nil); err != nil {
		return fmt.Errorf("failed to unlock: %s", err)
	}

	return nil
}

func (l *Lock) nonBlockingLock(clientId string) (bool, error) {
	newValue := createLockValue(clientId)

	ok, _, err := l.client.CompareAndSetKeyValue(l.GetName(), nil, newValue)

	if err != nil {
		return false, fmt.Errorf("failed to lock: %s", err)
	}

	return ok, nil
}

func createLockValue(owner any) map[string]any {
	return map[string]any{
		"owner": owner,
	}
}
