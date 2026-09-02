package guardduty_test

import (
	"encoding/base64"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	guarddutysdk "github.com/aws/aws-sdk-go-v2/service/guardduty"
	"github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
)

// TestListFilters_SDKRoundTrip_BoundaryWalkAndStaleToken drives ListFilters
// through the real aws-sdk-go-v2/service/guardduty client to prove
// paginate/decodeToken (services/guardduty/pagination.go), shared by 14
// operations in this package, reproduces the full set across a boundary
// walk and terminates cleanly on an out-of-range token rather than
// panicking or restarting at page one.
func TestListFilters_SDKRoundTrip_BoundaryWalkAndStaleToken(t *testing.T) {
	t.Parallel()

	h := guardduty.NewHandler(guardduty.NewInMemoryBackend("123456789012", "us-east-1"))
	client := newTestGuardDutyClient(t, h)

	det, err := client.CreateDetector(t.Context(), &guarddutysdk.CreateDetectorInput{Enable: aws.Bool(true)})
	require.NoError(t, err)

	detectorID := aws.ToString(det.DetectorId)

	want := make([]string, 0, 5)
	for i := range 5 {
		name := "filter-" + string(rune('a'+i))
		_, cErr := client.CreateFilter(t.Context(), &guarddutysdk.CreateFilterInput{
			DetectorId: aws.String(detectorID),
			Name:       aws.String(name),
			Action:     types.FilterActionNoop,
			FindingCriteria: &types.FindingCriteria{
				Criterion: map[string]types.Condition{"severity": {Gte: aws.Int32(4)}},
			},
		})
		require.NoError(t, cErr)

		want = append(want, name)
	}

	var seen []string

	token := ""
	for {
		in := &guarddutysdk.ListFiltersInput{DetectorId: aws.String(detectorID), MaxResults: aws.Int32(2)}
		if token != "" {
			in.NextToken = aws.String(token)
		}

		out, lErr := client.ListFilters(t.Context(), in)
		require.NoError(t, lErr)

		seen = append(seen, out.FilterNames...)

		if out.NextToken == nil {
			break
		}

		token = aws.ToString(out.NextToken)
	}

	assert.Equal(t, want, seen, "walking every page must reproduce every created filter, in order, no drops or dupes")

	// A token that decodes cleanly to an integer offset far past the
	// current filter count (the collection shrank between calls, or the
	// token was hand-built/replayed).
	staleToken := base64.StdEncoding.EncodeToString([]byte("999999"))

	require.NotPanics(t, func() {
		out, lErr := client.ListFilters(t.Context(), &guarddutysdk.ListFiltersInput{
			DetectorId: aws.String(detectorID),
			NextToken:  aws.String(staleToken),
		})
		require.NoError(t, lErr)
		assert.Empty(t, out.FilterNames)
		assert.Nil(t, out.NextToken)
	})
}
