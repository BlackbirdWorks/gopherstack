package redshiftdata_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	redshiftdatasdk "github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/redshiftdata"
)

// TestListDatabases_SDKRoundTrip_BoundaryWalkNoDrop drives ListDatabases
// through the real aws-sdk-go-v2/service/redshiftdata client, one item per
// page, to prove the paginateStrings fix (services/redshiftdata/
// handler_databases.go): the pre-fix helper decoded a resume cursor as
// "the item after the one named", while the cursor it emitted named the
// first item of the next page -- an off-by-one that silently dropped one
// database at every page boundary, even with no staleness involved at all.
// The existing pagination tests only ever checked "page 2's first item
// differs from page 1's", which is true whether or not an item was dropped
// in between; this walks every page and requires the full set back.
func TestListDatabases_SDKRoundTrip_BoundaryWalkNoDrop(t *testing.T) {
	t.Parallel()

	backend := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(backend)
	client := newTestRedshiftDataSDKClient(t, h)

	var seen []string

	token := ""
	for {
		in := &redshiftdatasdk.ListDatabasesInput{
			Database:   aws.String("dev"),
			MaxResults: 1,
		}
		if token != "" {
			in.NextToken = aws.String(token)
		}

		out, err := client.ListDatabases(t.Context(), in)
		require.NoError(t, err)
		require.Len(t, out.Databases, 1)

		seen = append(seen, out.Databases[0])

		if out.NextToken == nil {
			break
		}

		token = aws.ToString(out.NextToken)
	}

	assert.ElementsMatch(t, []string{"dev", "prod", "staging", "analytics"}, seen,
		"walking one database at a time must not silently drop any at a page boundary")
}

// TestListDatabases_SDKRoundTrip_TamperedTokenTerminates proves a nextToken
// naming no known database returns an empty page rather than resetting to
// page one -- the Class B shape (default-to-zero on a cursor miss) this
// fix also closes.
func TestListDatabases_SDKRoundTrip_TamperedTokenTerminates(t *testing.T) {
	t.Parallel()

	backend := redshiftdata.NewInMemoryBackend(testAccountID, testRegion)
	h := redshiftdata.NewHandler(backend)
	client := newTestRedshiftDataSDKClient(t, h)

	out, err := client.ListDatabases(t.Context(), &redshiftdatasdk.ListDatabasesInput{
		Database:   aws.String("dev"),
		MaxResults: 10,
		NextToken:  aws.String("does-not-name-any-database"),
	})
	require.NoError(t, err)
	assert.Empty(t, out.Databases, "an unmatched cursor must terminate, not restart at page one")
	assert.Nil(t, out.NextToken)
}
