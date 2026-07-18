package detective_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

// TestStartInvestigation_DerivesEntityType verifies EntityType is derived
// server-side from the EntityArn resource segment. The real StartInvestigation
// request shape has no EntityType input member at all -- Detective infers it
// from whether the ARN names an IAM role or an IAM user -- so a real SDK
// caller never sends one, and the emulator must not depend on client input to
// populate it.
func TestStartInvestigation_DerivesEntityType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		entityARN  string
		wantType   string
		wantErrLen bool
	}{
		{
			name:      "IAM role ARN",
			entityARN: "arn:aws:iam::123456789012:role/example-role",
			wantType:  "IAM_ROLE",
		},
		{
			name:      "IAM user ARN",
			entityARN: "arn:aws:iam::123456789012:user/example-user",
			wantType:  "IAM_USER",
		},
		{
			name:      "IAM role ARN with path",
			entityARN: "arn:aws:iam::123456789012:role/path/to/example-role",
			wantType:  "IAM_ROLE",
		},
		{
			name:       "non-IAM ARN rejected",
			entityARN:  "arn:aws:s3:::somebucket",
			wantErrLen: true,
		},
		{
			name:       "IAM group ARN rejected",
			entityARN:  "arn:aws:iam::123456789012:group/example-group",
			wantErrLen: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := detective.NewInMemoryBackend("000000000000", "us-east-1")

			g, err := b.CreateGraph(nil)
			require.NoError(t, err)

			id, startErr := b.StartInvestigation(
				g.Arn, tc.entityARN, time.Now().Add(-time.Hour).UTC(), time.Now().UTC(),
			)
			if tc.wantErrLen {
				require.Error(t, startErr)

				return
			}

			require.NoError(t, startErr)

			inv, getErr := b.GetInvestigation(g.Arn, id)
			require.NoError(t, getErr)
			assert.Equal(t, tc.wantType, inv.EntityType)
		})
	}
}
