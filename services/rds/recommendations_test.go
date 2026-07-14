package rds_test

import (
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDBRecommendation_PauseActive(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	b.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "rec-1",
		TypeID:           "type-idx-missing",
		Severity:         "medium",
		Status:           "active",
		Description:      "Add index to improve query performance",
	})

	active := b.DescribeDBRecommendations("", "active")
	require.Len(t, active, 1)

	modified, err := b.ModifyDBRecommendation("rec-1", "paused")
	require.NoError(t, err)
	assert.Equal(t, "paused", modified.Status)

	active = b.DescribeDBRecommendations("", "active")
	assert.Empty(t, active, "paused recommendation should not appear in active filter")

	paused := b.DescribeDBRecommendations("", "paused")
	require.Len(t, paused, 1)
	assert.Equal(t, "rec-1", paused[0].RecommendationID)
}

func TestDBRecommendation_InactiveStatus(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	b.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "rec-inactive",
		Status:           "active",
		Description:      "test recommendation",
	})

	_, err := b.ModifyDBRecommendation("rec-inactive", "inactive")
	require.NoError(t, err)

	all := b.DescribeDBRecommendations("", "")
	require.Len(t, all, 1)
	assert.Equal(t, "inactive", all[0].Status)
}

func TestDBRecommendation_MultipleStatusTransitions(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	b.AddDBRecommendation(rds.DBRecommendation{RecommendationID: "r1", Status: "active"})
	b.AddDBRecommendation(rds.DBRecommendation{RecommendationID: "r2", Status: "active"})
	b.AddDBRecommendation(rds.DBRecommendation{RecommendationID: "r3", Status: "active"})

	_, err := b.ModifyDBRecommendation("r1", "paused")
	require.NoError(t, err)

	_, err = b.ModifyDBRecommendation("r2", "inactive")
	require.NoError(t, err)

	assert.Len(t, b.DescribeDBRecommendations("", "active"), 1)
	assert.Len(t, b.DescribeDBRecommendations("", "paused"), 1)
	assert.Len(t, b.DescribeDBRecommendations("", "inactive"), 1)
}

func TestDBRecommendation_AddAndDescribe(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	assert.Equal(t, 0, rds.RecommendationCount(b))

	b.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "rec-add-1",
		TypeID:           "type-index",
		Severity:         "high",
		Status:           "active",
		Description:      "Missing index on user_id",
		ResourceARN:      "arn:aws:rds:us-east-1:123:db:mydb",
	})

	assert.Equal(t, 1, rds.RecommendationCount(b))

	recs := b.DescribeDBRecommendations("rec-add-1", "")
	require.Len(t, recs, 1)
	assert.Equal(t, "high", recs[0].Severity)
	assert.Equal(t, "Missing index on user_id", recs[0].Description)
}

func TestDBRecommendation_ModifyNotFound(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()

	_, err := b.ModifyDBRecommendation("nonexistent", "active")
	require.Error(t, err)
	require.ErrorIs(t, err, rds.ErrInvalidParameter)
}

func TestDBRecommendation_ModifyViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	b.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "rec-handler",
		Status:           "active",
		Description:      "test",
	})

	rec := postRDSForm(t, h,
		"Action=ModifyDBRecommendation&Version=2014-10-31"+
			"&RecommendationId=rec-handler&Status=paused")
	require.Equal(t, http.StatusOK, rec.Code)

	recs := b.DescribeDBRecommendations("rec-handler", "")
	require.Len(t, recs, 1)
	assert.Equal(t, "paused", recs[0].Status)
}

func TestDBRecommendation_DescribeViaHandler(t *testing.T) {
	t.Parallel()

	b := newBatch3Backend()
	h := rds.NewHandler(b)

	b.AddDBRecommendation(rds.DBRecommendation{
		RecommendationID: "rec-desc-handler",
		Status:           "active",
		Severity:         "medium",
		Description:      "Optimize checkpoint interval",
	})

	rec := postRDSForm(t, h,
		"Action=DescribeDBRecommendations&Version=2014-10-31&RecommendationId=rec-desc-handler")
	require.Equal(t, http.StatusOK, rec.Code)

	respStr := rec.Body.String()
	assert.Contains(t, respStr, "rec-desc-handler")
	assert.Contains(t, respStr, "Optimize checkpoint interval")
}

func TestModifyDBRecommendation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		recID     string
		status    string
		wantErr   bool
	}{
		{
			name:      "not found",
			recID:     "missing-rec",
			status:    "accepted",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.ModifyDBRecommendation(tt.recID, tt.status)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.status, got.Status)
		})
	}
}

func TestDescribeDBRecommendations(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		recID     string
		status    string
		wantCount int
	}{
		{name: "all empty", recID: "", status: "", wantCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got := b.DescribeDBRecommendations(tt.recID, tt.status)
			assert.Len(t, got, tt.wantCount)
		})
	}
}
