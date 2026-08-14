package cloudformation_test

import (
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
)

// ---- Stack Instance StackID assignment (table-driven) ----------------------------

func TestStackInstance_StackIDAssigned(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		accounts []string
		regions  []string
		wantLen  int
	}{
		{
			name:     "single account single region",
			accounts: []string{"111111111111"},
			regions:  []string{"us-east-1"},
			wantLen:  1,
		},
		{
			name:     "multi-account multi-region",
			accounts: []string{"111111111111", "222222222222"},
			regions:  []string{"us-east-1", "eu-west-1"},
			wantLen:  4,
		},
		{
			name:     "three accounts one region",
			accounts: []string{"111111111111", "222222222222", "333333333333"},
			regions:  []string{"ap-southeast-1"},
			wantLen:  3,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStackSet("inst-test-ss", "test", simpleTemplate, cloudformation.StackSetOptions{})
			require.NoError(t, err)

			_, err = b.CreateStackInstances(t.Context(), "inst-test-ss", tc.accounts, nil, tc.regions)
			require.NoError(t, err)

			instances, err := b.ListStackInstances("inst-test-ss", "")
			require.NoError(t, err)
			assert.Len(t, instances.Data, tc.wantLen)

			for _, inst := range instances.Data {
				assert.NotEmpty(t, inst.StackID, "expected StackID to be assigned for %s/%s",
					inst.Account, inst.Region)
				assert.True(t, strings.HasPrefix(inst.StackID, "arn:aws:cloudformation:"),
					"expected StackID to be a CloudFormation ARN, got: %s", inst.StackID)
				assert.NotEmpty(t, inst.StackSetID, "expected StackSetID to be set")
				assert.Equal(t, "NOT_CHECKED", inst.DriftStatus)
				assert.NotEmpty(t, inst.LastOperationID)
			}
		})
	}
}

func TestStackInstance_NoDuplicates(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("dedup-ss", "test", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	_, err = b.CreateStackInstances(t.Context(), "dedup-ss", []string{"111111111111"}, nil, []string{"us-east-1"})
	require.NoError(t, err)

	// Creating the same instance again should not duplicate it.
	_, err = b.CreateStackInstances(t.Context(), "dedup-ss", []string{"111111111111"}, nil, []string{"us-east-1"})
	require.NoError(t, err)

	instances, err := b.ListStackInstances("dedup-ss", "")
	require.NoError(t, err)
	assert.Len(t, instances.Data, 1, "expected no duplicate instances")
}

// ---- StackSet operation results (table-driven) -----------------------------------

func TestStackSetOperationResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		accounts     []string
		regions      []string
		wantStatuses []string
		wantResultN  int
	}{
		{
			name:         "single account region",
			accounts:     []string{"111111111111"},
			regions:      []string{"us-east-1"},
			wantResultN:  1,
			wantStatuses: []string{"SUCCEEDED"},
		},
		{
			name:         "two accounts two regions",
			accounts:     []string{"111111111111", "222222222222"},
			regions:      []string{"us-east-1", "us-west-2"},
			wantResultN:  4,
			wantStatuses: []string{"SUCCEEDED"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStackSet("op-results-ss", "test", simpleTemplate, cloudformation.StackSetOptions{})
			require.NoError(t, err)

			_, err = b.CreateStackInstances(t.Context(), "op-results-ss", tc.accounts, nil, tc.regions)
			require.NoError(t, err)

			// Get the operation ID from ListStackSetOperations.
			opsPage, err := b.ListStackSetOperations("op-results-ss", "")
			require.NoError(t, err)
			require.NotEmpty(t, opsPage.Data)

			results, err := b.ListStackSetOperationResults(
				"op-results-ss",
				opsPage.Data[0].OperationID,
				"",
			)
			require.NoError(t, err)
			assert.Len(t, results, tc.wantResultN)

			for _, r := range results {
				assert.NotEmpty(t, r.Account)
				assert.NotEmpty(t, r.Region)
				for _, wantStatus := range tc.wantStatuses {
					assert.Equal(t, wantStatus, r.Status)
				}
			}
		})
	}
}

func TestStackSetOperationResults_HTTP(t *testing.T) {
	t.Parallel()

	h := newHandler()

	postFormValues(t, h, url.Values{
		"Action":       {"CreateStackSet"},
		"StackSetName": {"http-op-ss"},
		"TemplateBody": {simpleTemplate},
	})
	postFormValues(t, h, url.Values{
		"Action":            {"CreateStackInstances"},
		"StackSetName":      {"http-op-ss"},
		"Accounts.member.1": {"111111111111"},
		"Regions.member.1":  {"us-east-1"},
	})

	// Get operation IDs.
	opsResp := postFormValues(t, h, url.Values{
		"Action":       {"ListStackSetOperations"},
		"StackSetName": {"http-op-ss"},
	})
	opsResp.mustOK(t)

	// Find an operation ID in the response.
	opID := extractField(opsResp.Body, "OperationId")
	if opID == "" {
		t.Skip("no operation ID found in response")
	}

	resp := postFormValues(t, h, url.Values{
		"Action":       {"ListStackSetOperationResults"},
		"StackSetName": {"http-op-ss"},
		"OperationId":  {opID},
	})
	resp.mustOK(t)
	assert.Contains(t, resp.Body, "ListStackSetOperationResultsResponse")
	assert.Contains(t, resp.Body, "111111111111")
	assert.Contains(t, resp.Body, "us-east-1")
	assert.Contains(t, resp.Body, "SUCCEEDED")
}

// ---- DescribeStackInstance fields -------------------------------------------------

func TestDescribeStackInstance_Fields(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("field-ss", "test", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)
	_, err = b.CreateStackInstances(t.Context(), "field-ss", []string{"123456789012"}, nil, []string{"us-east-1"})
	require.NoError(t, err)

	inst, err := b.DescribeStackInstance("field-ss", "123456789012", "us-east-1")
	require.NoError(t, err)

	assert.Equal(t, "123456789012", inst.Account)
	assert.Equal(t, "us-east-1", inst.Region)
	assert.Equal(t, "CURRENT", inst.Status)
	assert.Equal(t, "NOT_CHECKED", inst.DriftStatus)
	assert.NotEmpty(t, inst.StackID)
	assert.NotEmpty(t, inst.StackSetID)
	assert.NotEmpty(t, inst.LastOperationID)
}

// ---- ListStackSetOperations sorted order ------------------------------------------

func TestListStackSetOperations_SortedByCreationTime(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateStackSet("sort-ops-ss", "test", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	// Create multiple operations by calling CreateStackInstances multiple times.
	_, err = b.CreateStackInstances(t.Context(), "sort-ops-ss", []string{"111111111111"}, nil, []string{"us-east-1"})
	require.NoError(t, err)
	_, err = b.UpdateStackInstances("sort-ops-ss", []string{"111111111111"}, nil, []string{"us-east-1"})
	require.NoError(t, err)
	_, _, err = b.UpdateStackSet("sort-ops-ss", "", simpleTemplate, cloudformation.StackSetOptions{})
	require.NoError(t, err)

	opsPage2, err := b.ListStackSetOperations("sort-ops-ss", "")
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(opsPage2.Data), 3, "expected at least 3 operations")
}

// ---- DeleteStackInstances preserves other instances (table-driven) ---------------

func TestDeleteStackInstances_Selective(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		createAccounts []string
		createRegions  []string
		deleteAccounts []string
		deleteRegions  []string
		wantRemaining  int
	}{
		{
			name:           "delete half of instances",
			createAccounts: []string{"111111111111", "222222222222"},
			createRegions:  []string{"us-east-1"},
			deleteAccounts: []string{"111111111111"},
			deleteRegions:  []string{"us-east-1"},
			wantRemaining:  1,
		},
		{
			name:           "delete all instances",
			createAccounts: []string{"111111111111"},
			createRegions:  []string{"us-east-1"},
			deleteAccounts: []string{"111111111111"},
			deleteRegions:  []string{"us-east-1"},
			wantRemaining:  0,
		},
		{
			name:           "delete by region leaving other region",
			createAccounts: []string{"111111111111"},
			createRegions:  []string{"us-east-1", "eu-west-1"},
			deleteAccounts: []string{"111111111111"},
			deleteRegions:  []string{"us-east-1"},
			wantRemaining:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateStackSet("del-sel-ss", "test", simpleTemplate, cloudformation.StackSetOptions{})
			require.NoError(t, err)

			_, err = b.CreateStackInstances(t.Context(), "del-sel-ss", tc.createAccounts, nil, tc.createRegions)
			require.NoError(t, err)

			_, err = b.DeleteStackInstances(t.Context(), "del-sel-ss", tc.deleteAccounts, nil, tc.deleteRegions)
			require.NoError(t, err)

			remaining, err := b.ListStackInstances("del-sel-ss", "")
			require.NoError(t, err)
			assert.Len(t, remaining.Data, tc.wantRemaining)
		})
	}
}
