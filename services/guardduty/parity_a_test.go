package guardduty_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// TestParity_DeleteDetectorCleansUpSubResources verifies that DeleteDetector
// removes all sub-resource maps associated with the detector: members,
// publishing destinations, threat entity sets, and trusted entity sets.
// The previous implementation omitted these four delete calls, leaving dangling
// state in long-running processes and test suites that create/delete detectors
// in multiple cycles.
func TestParity_DeleteDetectorCleansUpSubResources(t *testing.T) {
	t.Parallel()

	b := guardduty.NewInMemoryBackend("111111111111", "us-east-1")

	// Create a detector.
	det, err := b.CreateDetector(true, "ALL", nil, nil)
	require.NoError(t, err)
	detID := det.DetectorID

	// Seed a member.
	_, unprocessed := b.CreateMembers(detID, []map[string]any{
		{"accountId": "222222222222", "email": "member@example.com"},
	})
	require.Empty(t, unprocessed, "CreateMembers should not produce unprocessed entries")

	// Seed a publishing destination.
	_, err = b.CreatePublishingDestination(detID, "S3", guardduty.DestinationProperties{
		DestinationArn: "arn:aws:s3:::my-bucket",
	})
	require.NoError(t, err)

	// Verify sub-resources exist before deletion.
	assert.Equal(t, 1, guardduty.MemberCount(b, detID), "member should exist before delete")
	assert.Equal(t, 1, guardduty.PublishingDestinationCount(b, detID),
		"publishing destination should exist before delete")

	// Delete the detector.
	require.NoError(t, b.DeleteDetector(detID))

	// Verify detector is gone.
	assert.Equal(t, 0, guardduty.DetectorCount(b), "detector must be removed")

	// Verify sub-resources are cleaned up.
	assert.Equal(t, 0, guardduty.MemberCount(b, detID),
		"members must be removed when detector is deleted")
	assert.Equal(t, 0, guardduty.PublishingDestinationCount(b, detID),
		"publishing destinations must be removed when detector is deleted")
	assert.Equal(t, 0, guardduty.ThreatEntitySetCount(b, detID),
		"threat entity sets must be removed when detector is deleted")
}
