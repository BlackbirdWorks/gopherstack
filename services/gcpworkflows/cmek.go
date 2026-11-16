package gcpworkflows

import (
	"errors"
	"strings"

	executionspb "cloud.google.com/go/workflows/executions/apiv1/executionspb"

	"github.com/blackbirdworks/gopherstack/pkgs/cmek"
)

// SetKeyChecker validates crypto_key_name against Cloud KMS; nil disables validation.
func (b *InMemoryBackend) SetKeyChecker(c cmek.Checker) {
	b.mu.Lock("SetKeyChecker")
	defer b.mu.Unlock()

	b.keys = c
}

func (b *InMemoryBackend) keyChecker() cmek.Checker {
	b.mu.RLock("keyChecker")
	defer b.mu.RUnlock()

	return b.keys
}

// checkKey maps a key problem to the gRPC error; disabled uses the message documented on the CMEK page.
func (b *InMemoryBackend) checkKey(key, location string) error {
	return keyError(cmek.Check(b.keyChecker(), key, location), key)
}

// cmekProblem checks key with c (the caller holds the backend lock).
func cmekProblem(c cmek.Checker, key string) error {
	return keyError(cmek.Check(c, key, ""), key)
}

func keyError(err error, key string) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, cmek.ErrNotFound):
		return errInvalid("The Cloud KMS key %s was not found.", key)
	case errors.Is(err, cmek.ErrLocation):
		return errInvalid("The Cloud KMS key ring and the protected workflow must be in the same region.")
	case errors.Is(err, cmek.ErrPurpose):
		return errInvalid("The Cloud KMS key %s must be a symmetric ENCRYPT_DECRYPT key.", key)
	case errors.Is(err, cmek.ErrDisabled):
		return errPrecondition("The provided Cloud KMS key version has been disabled.")
	}

	return err
}

// failIfKeyDisabled fails an active execution whose CMEK key is unusable (workflows/docs/use-cmek).
func (b *InMemoryBackend) failIfKeyDisabled(name string) {
	b.mu.RLock("failIfKeyDisabled")

	rec, ok := b.execs[name]
	key, active := "", false

	if ok {
		key, active = rec.key, rec.pb.GetState() == executionspb.Execution_ACTIVE
	}

	checker := b.keys
	b.mu.RUnlock()

	if !active || key == "" {
		return
	}

	err := cmek.Check(checker, key, "")
	if !errors.Is(err, cmek.ErrDisabled) && !errors.Is(err, cmek.ErrState) {
		return
	}

	b.mu.Lock("failIfKeyDisabled")
	defer b.mu.Unlock()

	rec, ok = b.execs[name]
	if !ok || rec.pb.GetState() != executionspb.Execution_ACTIVE {
		return
	}

	if rec.cancel != nil {
		rec.cancel()
	}

	msg := "The provided Cloud KMS key version has been disabled."
	b.endLocked(name, rec, executionspb.Execution_FAILED, "", &executionspb.Execution_Error{
		Payload: `{"message":"` + msg + `","tags":["SystemError"]}`, Context: msg,
	})
}

func (b *InMemoryBackend) failDisabledKeyExecutions(prefix string) {
	b.mu.RLock("failDisabledKeyExecutions")

	var names []string

	for n, rec := range b.execs {
		if rec.key != "" && rec.pb.GetState() == executionspb.Execution_ACTIVE && strings.HasPrefix(n, prefix) {
			names = append(names, n)
		}
	}

	b.mu.RUnlock()

	for _, n := range names {
		b.failIfKeyDisabled(n)
	}
}
