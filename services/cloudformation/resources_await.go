package cloudformation

import (
	"errors"
	"fmt"
	"time"
)

const (
	resourceSettleTimeout = 30 * time.Second
	resourceSettlePoll    = 20 * time.Millisecond
)

var (
	errResourceSettleTimeout = errors.New("timed out waiting for resource to settle")
	errResourceCreateFailed  = errors.New("resource entered a failed state")
)

// awaitResource polls settled until it reports done, as CloudFormation waits for a
// resource's terminal state before CREATE_COMPLETE or DELETE_COMPLETE.
func awaitResource(what string, settled func() (bool, error)) error {
	deadline := time.Now().Add(resourceSettleTimeout)

	for {
		done, err := settled()
		if err != nil {
			return fmt.Errorf("waiting for %s: %w", what, err)
		}

		if done {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("%w: %s", errResourceSettleTimeout, what)
		}

		time.Sleep(resourceSettlePoll)
	}
}

// awaitGone polls describe until it reports notFound, i.e. the resource is deleted.
func awaitGone(what string, describe func() error, notFound error) error {
	return awaitResource(what, func() (bool, error) {
		err := describe()
		if errors.Is(err, notFound) {
			return true, nil
		}

		return false, err
	})
}
