package workmail_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

type recordingDirectories struct {
	deleted []string
}

func (r *recordingDirectories) DeleteDirectory(region, directoryID string) error {
	r.deleted = append(r.deleted, region+"/"+directoryID)

	return nil
}

func TestDeleteOrganization_DeleteDirectory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		want            []string
		deleteDirectory bool
	}{
		{name: "deleted", deleteDirectory: true, want: []string{"us-east-1/d-1234567890"}},
		{name: "kept"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := workmail.NewInMemoryBackend("000000000000", "us-east-1")
			dirs := &recordingDirectories{}
			b.SetDirectoryDeleter(dirs)

			org, err := b.CreateOrganizationWithDirectory(t.Context(), "alias", "d-1234567890", nil, false)
			require.NoError(t, err)

			require.NoError(t, b.DeleteOrganization(org.OrgID, tt.deleteDirectory))
			assert.Equal(t, tt.want, dirs.deleted)
		})
	}
}
