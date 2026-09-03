package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// TestListFindings_Pagination_StaleTokenDoesNotRestart proves that an
// unresolvable nextToken does not restart ListFindings at page one. Findings
// have no delete operation in real Inspector2 (only status/suppression
// changes), so the hostile scenario is a forged token rather than deletion.
func TestListFindings_Pagination_StaleTokenDoesNotRestart(t *testing.T) {
	t.Parallel()

	backend := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := inspector2.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	for range 5 {
		_, err := h.Backend.SeedFinding(inspector2.Finding{Severity: inspector2.FindingSeverity{Label: "MEDIUM"}})
		require.NoError(t, err)
	}

	page1, err := client.ListFindings(ctx, &inspector2sdk.ListFindingsInput{MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, page1.Findings, 2)

	page1ARNs := map[string]bool{}
	for _, f := range page1.Findings {
		page1ARNs[*f.FindingArn] = true
	}

	page2, err := client.ListFindings(ctx, &inspector2sdk.ListFindingsInput{
		MaxResults: aws.Int32(2),
		NextToken:  aws.String("arn:aws:inspector2:us-east-1:123456789012:finding/does-not-exist"),
	})
	require.NoError(t, err)

	for _, f := range page2.Findings {
		assert.False(t, page1ARNs[*f.FindingArn], "an unresolvable nextToken must not restart pagination at page one")
	}
}

// TestListFindings_Pagination_TiedSeverityNoDropOrDuplicate proves that
// paginating with a non-unique SortCriteria field (SEVERITY, which many
// findings can share) visits every finding exactly once. ListFindings builds
// its unsorted candidate list via store.Table.Range, which iterates Go's
// underlying map in randomized order on every call; sortFindings' SEVERITY
// comparator had no tiebreak, so two findings tied on severity could land in
// a different relative order on the page-2 call than they did on page 1,
// letting an item already served on page 1 reappear on page 2 (or letting an
// item slip past the cursor entirely).
func TestListFindings_Pagination_TiedSeverityNoDropOrDuplicate(t *testing.T) {
	t.Parallel()

	backend := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)
	h := inspector2.NewHandler(backend)
	client := newRoundTripClient(t, h)
	ctx := t.Context()

	const total = 24

	want := map[string]bool{}

	for range total {
		f, err := h.Backend.SeedFinding(inspector2.Finding{Severity: inspector2.FindingSeverity{Label: "HIGH"}})
		require.NoError(t, err)
		want[f.FindingArn] = true
	}

	got := map[string]bool{}
	nextToken := (*string)(nil)

	for range total + 1 {
		out, err := client.ListFindings(ctx, &inspector2sdk.ListFindingsInput{
			MaxResults:   aws.Int32(3),
			NextToken:    nextToken,
			SortCriteria: &types.SortCriteria{Field: types.SortFieldSeverity, SortOrder: types.SortOrderAsc},
		})
		require.NoError(t, err)

		for _, f := range out.Findings {
			assert.False(t, got[*f.FindingArn], "finding %s served twice across pages", *f.FindingArn)
			got[*f.FindingArn] = true
		}

		if out.NextToken == nil {
			break
		}

		nextToken = out.NextToken
	}

	assert.Equal(t, want, got, "every tied-severity finding must be visited exactly once")
}

// TestListConnectors_Pagination_DeletedMidPage proves that deleting the
// connector a cursor names does not restart pagination at page one.
func TestListConnectors_Pagination_DeletedMidPage(t *testing.T) {
	t.Parallel()

	b := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)

	for i := range 5 {
		_, err := b.CreateConnector(
			"conn-"+string(rune('a'+i)), "", "AZURE", nil,
			"arn:aws:config::"+rtTestAccountID+":config-connector/cc-"+string(rune('a'+i)),
			[]string{"eastus"}, nil, nil,
		)
		require.NoError(t, err)
	}

	page1, next, err := b.ListConnectors(nil, nil, nil, 2, "")
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, next)

	page1ARNs := map[string]bool{}
	for _, c := range page1 {
		page1ARNs[c.ConnectorArn] = true
	}

	require.NoError(t, b.DeleteConnector(next))

	page2, _, err := b.ListConnectors(nil, nil, nil, 2, next)
	require.NoError(t, err)

	for _, c := range page2 {
		assert.False(
			t, page1ARNs[c.ConnectorArn],
			"cursor must not restart pagination at page one after its item is deleted",
		)
	}
}

// TestListConnectorScanConfigurations_Pagination_StaleTokenDoesNotRestart
// proves that an unresolvable nextToken does not restart
// ListConnectorScanConfigurations at page one. A scan configuration has no
// standalone delete operation (it only exists via
// UpdateConnectorScanConfiguration against a live connector), so the hostile
// scenario is a forged token.
func TestListConnectorScanConfigurations_Pagination_StaleTokenDoesNotRestart(t *testing.T) {
	t.Parallel()

	b := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)

	for i := range 5 {
		awsConfigArn := "arn:aws:config::" + rtTestAccountID + ":config-connector/cc-" + string(rune('a'+i))
		_, err := b.CreateConnector(
			"conn-"+string(rune('a'+i)), "", "AZURE", nil, awsConfigArn, []string{"eastus"}, nil, nil,
		)
		require.NoError(t, err)
		require.NoError(t, b.UpdateConnectorScanConfiguration(awsConfigArn, nil))
	}

	page1, next, err := b.ListConnectorScanConfigurations(nil, 2, "")
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, next)

	page1Arns := map[string]bool{}
	for _, c := range page1 {
		page1Arns[c.AwsConfigConnectorArn] = true
	}

	page2, _, err := b.ListConnectorScanConfigurations(
		nil,
		2,
		"arn:aws:config::"+rtTestAccountID+":config-connector/does-not-exist",
	)
	require.NoError(t, err)

	for _, c := range page2 {
		assert.False(
			t, page1Arns[c.AwsConfigConnectorArn],
			"an unresolvable nextToken must not restart pagination at page one",
		)
	}
}

// TestListCoverage_Pagination_StaleTokenDoesNotRestart proves that an
// unresolvable nextToken does not restart ListCoverage at page one. Coverage
// entries reflect live scan state and have no delete operation, so the
// hostile scenario is a forged token.
func TestListCoverage_Pagination_StaleTokenDoesNotRestart(t *testing.T) {
	t.Parallel()

	b := inspector2.NewInMemoryBackend(rtTestAccountID, rtTestRegion)

	for i := range 5 {
		_, err := b.SeedCoverage(inspector2.CoverageEntry{
			ResourceID:   "i-" + string(rune('a'+i)),
			ResourceType: "AWS_EC2_INSTANCE",
			ScanType:     "PACKAGE",
			AccountID:    rtTestAccountID,
		})
		require.NoError(t, err)
	}

	page1, next, err := b.ListCoverage(nil, 2, "")
	require.NoError(t, err)
	require.Len(t, page1, 2)
	require.NotEmpty(t, next)

	page1Keys := map[string]bool{}
	for _, e := range page1 {
		page1Keys[e.ResourceID+"/"+e.ScanType] = true
	}

	page2, _, err := b.ListCoverage(nil, 2, "i-does-not-exist/PACKAGE")
	require.NoError(t, err)

	for _, e := range page2 {
		key := e.ResourceID + "/" + e.ScanType
		assert.False(t, page1Keys[key], "an unresolvable nextToken must not restart pagination at page one")
	}
}
