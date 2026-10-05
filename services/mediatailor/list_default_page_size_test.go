package mediatailor_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediatailorsdk "github.com/aws/aws-sdk-go-v2/service/mediatailor"
	"github.com/aws/aws-sdk-go-v2/service/mediatailor/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediatailor"
)

// "The default value is 100" (api_op_ListSourceLocations.go MaxResults).
func TestListSourceLocations_DefaultPageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		maxRes   *int32
		name     string
		wantLen  int
		wantNext bool
	}{
		{name: "default 100", wantLen: 100, wantNext: true},
		{name: "explicit", maxRes: aws.Int32(25), wantLen: 25, wantNext: true},
		{name: "above total", maxRes: aws.Int32(500), wantLen: 105},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestMediaTailorClient(
				t,
				mediatailor.NewHandler(mediatailor.NewInMemoryBackend("123456789012", "us-east-1")),
			)
			ctx := t.Context()

			for i := range 105 {
				_, err := client.CreateSourceLocation(ctx, &mediatailorsdk.CreateSourceLocationInput{
					SourceLocationName: aws.String(fmt.Sprintf("sl-%03d", i)),
					HttpConfiguration:  &types.HttpConfiguration{BaseUrl: aws.String("https://example.com")},
				})
				require.NoError(t, err)
			}

			out, err := client.ListSourceLocations(ctx, &mediatailorsdk.ListSourceLocationsInput{MaxResults: tt.maxRes})
			require.NoError(t, err)
			assert.Len(t, out.Items, tt.wantLen)
			assert.Equal(t, tt.wantNext, out.NextToken != nil)
		})
	}
}
