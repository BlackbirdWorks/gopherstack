package fsx

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewFSxVolumeID_Length is a regression guard: the hashicorp/aws
// provider client-side validates aws_fsx_openzfs_volume.parent_volume_id
// and aws_fsx_openzfs_snapshot.volume_id against a fixed 23-character
// length ("fsvol-" + 17 hex chars, matching real AWS's own ID format), so a
// shorter generated ID here fails in the provider before ever reaching this
// emulator's wire.
func TestNewFSxVolumeID_Length(t *testing.T) {
	t.Parallel()

	id := newFSxVolumeID()
	assert.Len(t, id, len("fsvol-")+fsxVolumeIDHexLen)
	assert.Len(t, id, 23)
}
