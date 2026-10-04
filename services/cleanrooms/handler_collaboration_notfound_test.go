package cleanrooms_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cleanroomssdk "github.com/aws/aws-sdk-go-v2/service/cleanrooms"
	"github.com/aws/aws-sdk-go-v2/service/cleanrooms/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cleanrooms"
)

// Get/UpdateCollaboration declare no ResourceNotFoundException (deserializers.go:4695-4705);
// a missing id must surface as the declared ValidationException.
func TestHandler_GetUpdateCollaboration_NonexistentIsDeclaredValidation(t *testing.T) {
	t.Parallel()

	missingID := "00000000-0000-0000-0000-000000000000"

	tests := []struct {
		call func(ctx context.Context, c *cleanroomssdk.Client) error
		name string
	}{
		{
			name: "get",
			call: func(ctx context.Context, c *cleanroomssdk.Client) error {
				_, err := c.GetCollaboration(ctx, &cleanroomssdk.GetCollaborationInput{
					CollaborationIdentifier: aws.String(missingID),
				})

				return err
			},
		},
		{
			name: "update",
			call: func(ctx context.Context, c *cleanroomssdk.Client) error {
				_, err := c.UpdateCollaboration(ctx, &cleanroomssdk.UpdateCollaborationInput{
					CollaborationIdentifier: aws.String(missingID),
					Description:             aws.String("new description"),
				})

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := cleanrooms.NewHandler(cleanrooms.NewInMemoryBackend("000000000000", "us-east-1"))
			client := newTestCleanRoomsClient(t, h)

			err := tt.call(t.Context(), client)
			require.Error(t, err)

			var ve *types.ValidationException

			require.ErrorAs(t, err, &ve)
		})
	}
}
