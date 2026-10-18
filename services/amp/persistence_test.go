package amp_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ampsdk "github.com/aws/aws-sdk-go-v2/service/amp"
	"github.com/aws/aws-sdk-go-v2/service/amp/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/amp"
)

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	t.Parallel()

	src := newFixture(t)
	ws := src.workspace(t)
	wsARN := aws.String(ws)

	_, err := src.client.CreateRuleGroupsNamespace(t.Context(), &ampsdk.CreateRuleGroupsNamespaceInput{
		WorkspaceId: wsARN, Name: aws.String("ns"), Data: b64(validRules),
	})
	require.NoError(t, err)
	_, err = src.client.CreateAlertManagerDefinition(
		t.Context(),
		&ampsdk.CreateAlertManagerDefinitionInput{WorkspaceId: wsARN, Data: b64(validAlertManager)},
	)
	require.NoError(t, err)
	_, err = src.client.CreateLoggingConfiguration(
		t.Context(),
		&ampsdk.CreateLoggingConfigurationInput{WorkspaceId: wsARN, LogGroupArn: aws.String(logGroup)},
	)
	require.NoError(t, err)
	_, err = src.client.UpdateWorkspaceConfiguration(
		t.Context(),
		&ampsdk.UpdateWorkspaceConfigurationInput{WorkspaceId: wsARN, RetentionPeriodInDays: aws.Int32(7)},
	)
	require.NoError(t, err)
	_, err = src.client.CreateAnomalyDetector(t.Context(), detectorInput(wsARN))
	require.NoError(t, err)
	_, err = src.client.CreateScraper(t.Context(), scraperInput(workspaceARN(t, src)))
	require.NoError(t, err)

	seedMetrics(t, src, ws)
	src.settle()

	snap := src.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	dst := newFixtureFromBackend(t, amp.NewInMemoryBackend(testAccount, "us-east-1"), &fakeClock{t: src.clock.now()})
	require.NoError(t, dst.backend.Restore(t.Context(), snap))

	d, err := dst.client.DescribeWorkspace(t.Context(), &ampsdk.DescribeWorkspaceInput{WorkspaceId: wsARN})
	require.NoError(t, err)
	assert.Equal(t, types.WorkspaceStatusCodeActive, d.Workspace.Status.StatusCode)

	n, err := dst.client.DescribeRuleGroupsNamespace(
		t.Context(),
		&ampsdk.DescribeRuleGroupsNamespaceInput{WorkspaceId: wsARN, Name: aws.String("ns")},
	)
	require.NoError(t, err)
	assert.Equal(t, validRules, string(n.RuleGroupsNamespace.Data))

	am, err := dst.client.DescribeAlertManagerDefinition(
		t.Context(),
		&ampsdk.DescribeAlertManagerDefinitionInput{WorkspaceId: wsARN},
	)
	require.NoError(t, err)
	assert.Equal(t, validAlertManager, string(am.AlertManagerDefinition.Data))

	lg, err := dst.client.DescribeLoggingConfiguration(
		t.Context(),
		&ampsdk.DescribeLoggingConfigurationInput{WorkspaceId: wsARN},
	)
	require.NoError(t, err)
	assert.Equal(t, logGroup, aws.ToString(lg.LoggingConfiguration.LogGroupArn))

	cfg, err := dst.client.DescribeWorkspaceConfiguration(
		t.Context(),
		&ampsdk.DescribeWorkspaceConfigurationInput{WorkspaceId: wsARN},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(7), aws.ToInt32(cfg.WorkspaceConfiguration.RetentionPeriodInDays))

	dl, err := dst.client.ListAnomalyDetectors(t.Context(), &ampsdk.ListAnomalyDetectorsInput{WorkspaceId: wsARN})
	require.NoError(t, err)
	assert.Len(t, dl.AnomalyDetectors, 1)

	sl, err := dst.client.ListScrapers(t.Context(), &ampsdk.ListScrapersInput{})
	require.NoError(t, err)
	assert.Len(t, sl.Scrapers, 1)

	r, _ := dst.query(t, ws, "query", url.Values{"query": {`http_requests_total{code="200"}`}, "time": {"60"}})
	assert.Equal(t, http.StatusOK, r.status)
	assert.Contains(t, string(r.body), `"value":[60,"60"]`)
	assert.Equal(t, src.backend.SampleCount(ws), dst.backend.SampleCount(ws))
}

func TestRestoreDiscardsIncompatibleSnapshot(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.workspace(t)

	require.NoError(t, f.backend.Restore(t.Context(), []byte(`{"version":999}`)))
	assert.Equal(t, 0, f.backend.WorkspaceCount())
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ws := f.workspace(t)
	seedMetrics(t, f, ws)

	f.backend.Reset()
	assert.Equal(t, 0, f.backend.WorkspaceCount())
	assert.Equal(t, 0, f.backend.SampleCount(ws))
}

func TestAccountAndRegionIsolation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	out, err := f.client.CreateWorkspace(t.Context(), &ampsdk.CreateWorkspaceInput{})
	require.NoError(t, err)

	other := f.regionClient(t, "eu-west-1")

	_, err = other.DescribeWorkspace(t.Context(), &ampsdk.DescribeWorkspaceInput{WorkspaceId: out.WorkspaceId})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	l, err := other.ListWorkspaces(t.Context(), &ampsdk.ListWorkspacesInput{})
	require.NoError(t, err)
	assert.Empty(t, l.Workspaces)

	_, err = other.TagResource(
		t.Context(),
		&ampsdk.TagResourceInput{ResourceArn: out.Arn, Tags: map[string]string{"a": "b"}},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

}
