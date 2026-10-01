package stepfunctions

import (
	"fmt"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions/asl"
)

// splitMockTestCase splits "stateMachineArn#TestCase" into its parts.
func splitMockTestCase(arn string) (string, string, bool) {
	return strings.Cut(arn, "#")
}

// mockRunLocked builds the MockRun for a test case. Caller holds b.mu.
func (b *InMemoryBackend) mockRunLocked(smName, testCase string, hasTestCase bool) (*asl.MockRun, error) {
	if !hasTestCase {
		return nil, nil //nolint:nilnil // no test case means no mocking
	}

	if b.mockConfig == nil {
		return nil, fmt.Errorf(
			"%w: test case %q requested but no mock config is loaded (set SFN_MOCK_CONFIG)",
			ErrInvalidStateMachineArn, testCase,
		)
	}

	run, err := b.mockConfig.TestCase(smName, testCase)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrInvalidStateMachineArn, err)
	}

	return run, nil
}

func (b *InMemoryBackend) integrationsWithMockLocked(run *asl.MockRun) integrationsSnapshot {
	snap := b.snapshotIntegrationsLocked()
	snap.mockRun = run

	return snap
}
