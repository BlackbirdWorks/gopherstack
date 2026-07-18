package dynamodb_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestListContributorInsights_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)

	out, err := db.ListContributorInsights(t.Context(), &sdk.ListContributorInsightsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.ContributorInsightsSummaries)
}

func TestUpdateContributorInsights_TableNotFound(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)

	_, err := db.UpdateContributorInsights(t.Context(), &sdk.UpdateContributorInsightsInput{
		TableName:                 aws.String("NonExistent"),
		ContributorInsightsAction: types.ContributorInsightsActionEnable,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestUpdateContributorInsights_TogglesStatus(t *testing.T) {
	t.Parallel()

	db := newTestDBWithCleanup(t)
	createSimplePPRTable(t, db, "CITable")

	out, err := db.UpdateContributorInsights(t.Context(), &sdk.UpdateContributorInsightsInput{
		TableName:                 aws.String("CITable"),
		ContributorInsightsAction: types.ContributorInsightsActionEnable,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ContributorInsightsStatusEnabled, out.ContributorInsightsStatus)

	desc, err := db.DescribeContributorInsights(t.Context(), &sdk.DescribeContributorInsightsInput{
		TableName: aws.String("CITable"),
	})
	require.NoError(t, err)
	assert.Equal(t, types.ContributorInsightsStatusEnabled, desc.ContributorInsightsStatus)

	disabled, err := db.UpdateContributorInsights(t.Context(), &sdk.UpdateContributorInsightsInput{
		TableName:                 aws.String("CITable"),
		ContributorInsightsAction: types.ContributorInsightsActionDisable,
	})
	require.NoError(t, err)
	assert.Equal(t, types.ContributorInsightsStatusDisabled, disabled.ContributorInsightsStatus)
}

func TestDescribeContributorInsights(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(t *testing.T, backend *dynamodb.InMemoryDB)
		name       string
		wantStatus int
	}{
		{
			name: "success_disabled",
			setup: func(t *testing.T, backend *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, backend, "ContribTable", "pk")
			},
			body:       map[string]any{"TableName": "ContribTable"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "table_not_found",
			body:       map[string]any{"TableName": "NoSuchTable"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)

			if tt.setup != nil {
				tt.setup(t, backend)
			}

			code, resp := invokeOp(t, handler, "DescribeContributorInsights", tt.body)
			assert.Equal(t, tt.wantStatus, code)

			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, "DISABLED", resp["ContributorInsightsStatus"])
				assert.NotNil(t, resp["ContributorInsightsRuleList"])
			}
		})
	}
}
