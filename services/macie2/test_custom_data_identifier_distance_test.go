package macie2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	macie2sdk "github.com/aws/aws-sdk-go-v2/service/macie2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestCustomDataIdentifier_MaximumMatchDistance_RealClient(t *testing.T) {
	t.Parallel()

	cases := []struct {
		distance  *int32
		name      string
		wantCount int32
		wantErr   bool
	}{
		{name: "default_distance_matches", distance: nil, wantCount: 1},
		{name: "exact_distance_matches", distance: aws.Int32(8), wantCount: 1},
		{name: "too_short_distance", distance: aws.Int32(7), wantCount: 0},
		{name: "below_range", distance: aws.Int32(0), wantErr: true},
		{name: "above_range", distance: aws.Int32(301), wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, client := newMacie2Client(t)

			out, err := client.TestCustomDataIdentifier(t.Context(), &macie2sdk.TestCustomDataIdentifierInput{
				Regex:                aws.String(`\d{7}`),
				SampleText:           aws.String("id: 1234567"),
				Keywords:             []string{"id:"},
				MaximumMatchDistance: tc.distance,
			})
			if tc.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tc.wantCount, aws.ToInt32(out.MatchCount))
		})
	}
}
