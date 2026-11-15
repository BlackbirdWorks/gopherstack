package gcpsql_test

import (
	"context"
	"strconv"
)

func (f *fakeEngine) Quiesce(context.Context) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.quiesced++

	return "pos-" + strconv.Itoa(f.port), nil
}

func (f *fakeEngine) Unquiesce(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unquiesced++

	return nil
}

func (f *fakeEngine) Seal(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sealed++

	return nil
}

func (f *fakeEngine) Settle(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.settled++

	return nil
}

func (f *fakeEngine) WaitCaughtUp(_ context.Context, pos string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.waited = append(f.waited, pos)

	return f.waitErr
}
