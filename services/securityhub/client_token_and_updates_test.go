package securityhub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	securityhubsdk "github.com/aws/aws-sdk-go-v2/service/securityhub"
	"github.com/aws/aws-sdk-go-v2/service/securityhub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/securityhub"
)

func TestBatchUpdateFindings_MergesAndStampsMembers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		updates map[string]any
		check   func(t *testing.T, f map[string]any)
		name    string
	}{
		{
			name:    "severity_keeps_original",
			updates: map[string]any{"Severity": map[string]any{"Label": "HIGH"}},
			check: func(t *testing.T, f map[string]any) {
				t.Helper()

				sev, _ := f["Severity"].(map[string]any)
				assert.Equal(t, "HIGH", sev["Label"])
				assert.Equal(t, "7.5", sev["Original"])
				assert.InDelta(t, 40.0, sev["Normalized"], 0)
			},
		},
		{
			name:    "note_gets_updated_at",
			updates: map[string]any{"Note": map[string]any{"Text": "looking", "UpdatedBy": "me"}},
			check: func(t *testing.T, f map[string]any) {
				t.Helper()

				note, _ := f["Note"].(map[string]any)
				assert.Equal(t, "looking", note["Text"])
				assert.NotEmpty(t, note["UpdatedAt"])
			},
		},
		{
			name:    "flat_members",
			updates: map[string]any{"Confidence": 80.0, "Criticality": 90.0, "Types": []any{"TTPs/x"}},
			check: func(t *testing.T, f map[string]any) {
				t.Helper()

				assert.InDelta(t, 80.0, f["Confidence"], 0)
				assert.InDelta(t, 90.0, f["Criticality"], 0)
				assert.Equal(t, []any{"TTPs/x"}, f["Types"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := securityhub.NewInMemoryBackend("000000000000", "us-east-1")
			base := securityhub.ValidFinding(map[string]any{
				"Id":       "f1",
				"Severity": map[string]any{"Label": "LOW", "Normalized": 40.0, "Original": "7.5"},
			})
			_, _, _ = b.ImportFindings([]map[string]any{base})

			_, unprocessed := b.BatchUpdateFindings(
				[]map[string]any{{"Id": "f1", "ProductArn": base["ProductArn"]}}, tt.updates,
			)
			require.Empty(t, unprocessed)

			got, _ := b.GetFindings(
				map[string]any{"Id": []any{map[string]any{"Value": "f1", "Comparison": "EQUALS"}}}, nil, "", 100,
			)
			require.Len(t, got, 1)
			tt.check(t, got[0])
		})
	}
}

func TestCreateV2_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, c *securityhubsdk.Client, token string, variant bool) string
		name   string
	}{
		{name: "aggregator_v2", create: func(
			t *testing.T, c *securityhubsdk.Client, token string, variant bool,
		) string {
			t.Helper()

			modes := []string{"SPECIFIED_REGIONS", "ALL_REGIONS"}
			in := &securityhubsdk.CreateAggregatorV2Input{
				ClientToken: aws.String(token), RegionLinkingMode: aws.String(modes[0]),
			}

			if variant {
				in.RegionLinkingMode = aws.String(modes[1])
			}

			out, err := c.CreateAggregatorV2(t.Context(), in)
			if variant {
				require.ErrorContains(t, err, "ConflictException")

				return ""
			}

			require.NoError(t, err)

			return aws.ToString(out.AggregatorV2Arn)
		}},
		{name: "connector_v2", create: func(
			t *testing.T, c *securityhubsdk.Client, token string, variant bool,
		) string {
			t.Helper()

			name := "c1"
			if variant {
				name = "c2"
			}

			out, err := c.CreateConnectorV2(t.Context(), &securityhubsdk.CreateConnectorV2Input{
				ClientToken: aws.String(token), Name: aws.String(name),
				Provider: &types.ProviderConfigurationMemberJiraCloud{
					Value: types.JiraCloudProviderConfiguration{ProjectKey: aws.String("P")},
				},
			})
			if variant {
				require.ErrorContains(t, err, "ConflictException")

				return ""
			}

			require.NoError(t, err)

			return aws.ToString(out.ConnectorId)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newRealClientBackendAndClient(t)

			first := tt.create(t, c, "tok", false)
			require.NotEmpty(t, first)
			assert.Equal(t, first, tt.create(t, c, "tok", false), "same token and parameters replays")
			tt.create(t, c, "tok", true)
		})
	}
}

func TestConnectorsV2_KmsKeyArnAndListFilters(t *testing.T) {
	t.Parallel()

	_, c := newRealClientBackendAndClient(t)

	created, err := c.CreateConnectorV2(t.Context(), &securityhubsdk.CreateConnectorV2Input{
		Name:      aws.String("jira"),
		KmsKeyArn: aws.String("arn:aws:kms:us-east-1:000000000000:key/k1"),
		Provider: &types.ProviderConfigurationMemberJiraCloud{
			Value: types.JiraCloudProviderConfiguration{ProjectKey: aws.String("P")},
		},
	})
	require.NoError(t, err)

	got, err := c.GetConnectorV2(t.Context(), &securityhubsdk.GetConnectorV2Input{ConnectorId: created.ConnectorId})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:kms:us-east-1:000000000000:key/k1", aws.ToString(got.KmsKeyArn))

	tests := []struct {
		name string
		in   securityhubsdk.ListConnectorsV2Input
		want int
	}{
		{name: "no_filter", want: 1},
		{
			name: "provider_match", want: 1,
			in: securityhubsdk.ListConnectorsV2Input{ProviderName: types.ConnectorProviderNameJiraCloud},
		},
		{
			name: "provider_other", want: 0,
			in: securityhubsdk.ListConnectorsV2Input{ProviderName: types.ConnectorProviderNameServicenow},
		},
		{
			name: "status_match", want: 1,
			in: securityhubsdk.ListConnectorsV2Input{ConnectorStatus: types.ConnectorStatusConnected},
		},
		{
			name: "status_other", want: 0,
			in: securityhubsdk.ListConnectorsV2Input{ConnectorStatus: types.ConnectorStatusDegraded},
		},
		{
			name: "enablement_match", want: 1,
			in: securityhubsdk.ListConnectorsV2Input{EnablementStatus: types.EnablementStatusEnabled},
		},
		{
			name: "enablement_other", want: 0,
			in: securityhubsdk.ListConnectorsV2Input{EnablementStatus: types.EnablementStatusPendingDeletion},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, listErr := c.ListConnectorsV2(t.Context(), &tt.in)
			require.NoError(t, listErr)
			assert.Len(t, out.Connectors, tt.want)
		})
	}
}

func TestProvidersFilter_AwsOnlyEmulated(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		providers []types.StandardsProvider
		wantAny   bool
	}{
		{name: "none", wantAny: true},
		{name: "aws", providers: []types.StandardsProvider{types.StandardsProviderAws}, wantAny: true},
		{name: "azure_only", providers: []types.StandardsProvider{types.StandardsProviderAzure}, wantAny: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, c := newRealClientBackendAndClient(t)
			require.NoError(t, backend.EnableHub(true, "", nil))

			std, err := c.DescribeStandards(
				t.Context(), &securityhubsdk.DescribeStandardsInput{Providers: tt.providers},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAny, len(std.Standards) > 0)

			var ctrlProviders []types.SecurityControlsProvider
			for _, p := range tt.providers {
				ctrlProviders = append(ctrlProviders, types.SecurityControlsProvider(p))
			}

			defs, err := c.ListSecurityControlDefinitions(
				t.Context(), &securityhubsdk.ListSecurityControlDefinitionsInput{Providers: ctrlProviders},
			)
			require.NoError(t, err)
			assert.Equal(t, tt.wantAny, len(defs.SecurityControlDefinitions) > 0)

			enabled, err := c.GetEnabledStandards(
				t.Context(), &securityhubsdk.GetEnabledStandardsInput{Providers: tt.providers},
			)
			require.NoError(t, err)

			if !tt.wantAny {
				assert.Empty(t, enabled.StandardsSubscriptions)
			}
		})
	}
}

func TestDescribeHub_HubArnFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		arn     func(current string) *string
		name    string
		wantErr bool
	}{
		{name: "omitted", arn: func(string) *string { return nil }},
		{name: "matching", arn: aws.String},
		{
			name: "other", wantErr: true,
			arn: func(string) *string { return aws.String("arn:aws:securityhub:us-east-1:000000000000:hub/other") },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend, c := newRealClientBackendAndClient(t)
			require.NoError(t, backend.EnableHub(true, "", nil))

			hub, err := c.DescribeHub(t.Context(), &securityhubsdk.DescribeHubInput{})
			require.NoError(t, err)

			_, err = c.DescribeHub(
				t.Context(), &securityhubsdk.DescribeHubInput{HubArn: tt.arn(aws.ToString(hub.HubArn))},
			)
			if tt.wantErr {
				require.ErrorContains(t, err, "ResourceNotFoundException")

				return
			}

			require.NoError(t, err)
		})
	}
}
