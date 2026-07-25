package pipes_test

// Covers ListPipes: pagination (Limit/NextToken), filtering by state/source/
// target prefix, and result ordering.

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// TestPagination_Limit verifies Limit parameter is respected in ListPipes.
func TestPagination_Limit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numPipes  int
		limit     int
		wantCount int
		wantToken bool
	}{
		{name: "limit_2_of_5", numPipes: 5, limit: 2, wantCount: 2, wantToken: true},
		{name: "limit_equals_count", numPipes: 3, limit: 3, wantCount: 3, wantToken: false},
		{name: "limit_exceeds_count", numPipes: 2, limit: 10, wantCount: 2, wantToken: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			for i := range tt.numPipes {
				_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
					RoleARN:      "arn:aws:iam::123456789012:role/r",
					Name:         "pipe-" + string(rune('a'+i)) + "-" + tt.name,
					Source:       "arn:aws:sqs:us-west-2:123456789012:q",
					Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
					DesiredState: "RUNNING",
				})
				require.NoError(t, err)
			}

			h := pipes.NewHandler(b)

			url := "/v1/pipes?Limit=" + string(rune('0'+tt.limit))
			if tt.limit >= 10 {
				url = "/v1/pipes?Limit=10"
			}
			rec := auditDo(t, h, http.MethodGet, url, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			pipeList, _ := resp["Pipes"].([]any)
			assert.Len(t, pipeList, tt.wantCount)
		})
	}
}

// TestPagination_NextToken verifies NextToken pagination works correctly.
func TestPagination_NextToken(t *testing.T) {
	t.Parallel()

	b := auditNewBackend()
	for i := range 5 {
		_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
			RoleARN:      "arn:aws:iam::123456789012:role/r",
			Name:         "pag-pipe-" + string(rune('a'+i)),
			Source:       "arn:aws:sqs:us-west-2:123456789012:q",
			Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
			DesiredState: "RUNNING",
		})
		require.NoError(t, err)
	}

	h := pipes.NewHandler(b)

	// Fetch page 1 (2 items)
	page1 := auditDo(t, h, http.MethodGet, "/v1/pipes?Limit=2", nil)
	require.Equal(t, http.StatusOK, page1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(page1.Body.Bytes(), &resp1))
	list1 := resp1["Pipes"].([]any)
	assert.Len(t, list1, 2)
	assert.NotEmpty(t, resp1["NextToken"], "NextToken should be present when more pages exist")

	// Fetch page 2 using NextToken
	token := resp1["NextToken"].(string)
	page2 := auditDo(t, h, http.MethodGet, "/v1/pipes?Limit=2&NextToken="+token, nil)
	require.Equal(t, http.StatusOK, page2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(page2.Body.Bytes(), &resp2))
	list2 := resp2["Pipes"].([]any)
	assert.NotEmpty(t, list2)

	// No overlap between pages
	names1 := make(map[string]bool)
	for _, p := range list1 {
		pm := p.(map[string]any)
		names1[pm["Name"].(string)] = true
	}
	for _, p := range list2 {
		pm := p.(map[string]any)
		assert.False(t, names1[pm["Name"].(string)], "page 2 should not repeat page 1 items")
	}
}

// TestPagination_FilterByCurrentState verifies CurrentState filter in ListPipes.
func TestPagination_FilterByCurrentState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterState  string
		wantMinCount int
	}{
		{name: "filter_creating", filterState: "CREATING", wantMinCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				RoleARN:      "arn:aws:iam::123456789012:role/r",
				Name:         "filter-state-pipe-" + tt.name,
				Source:       "arn:aws:sqs:us-west-2:123456789012:q",
				Target:       "arn:aws:lambda:us-west-2:123456789012:function:fn",
				DesiredState: "RUNNING",
			})
			require.NoError(t, err)

			// Query immediately — pipe should be in CREATING state
			result, err := b.ListPipes(context.Background(), pipes.ListPipesFilter{CurrentState: tt.filterState})
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(result.Pipes), tt.wantMinCount)
		})
	}
}

// TestListPipes_SourceTargetPrefix verifies source/target prefix filtering.
func TestListPipes_SourceTargetPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		targetPrefix string
		wantCount    int
	}{
		{name: "filter_by_lambda_target", targetPrefix: "arn:aws:lambda:", wantCount: 2},
		{name: "filter_by_sfn_target", targetPrefix: "arn:aws:states:", wantCount: 1},
		{name: "filter_by_nonexistent", targetPrefix: "arn:aws:s3:", wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := auditNewBackend()
			targets := []string{
				"arn:aws:lambda:us-west-2:123456789012:function:fn1",
				"arn:aws:lambda:us-west-2:123456789012:function:fn2",
				"arn:aws:states:us-west-2:123456789012:stateMachine:sm",
			}
			for i, target := range targets {
				_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
					RoleARN:      "arn:aws:iam::123456789012:role/r",
					Name:         "prefix-pipe-" + string(rune('a'+i)) + "-" + tt.name,
					Source:       "arn:aws:sqs:us-west-2:123456789012:q",
					Target:       target,
					DesiredState: "RUNNING",
				})
				require.NoError(t, err)
			}

			result, err := b.ListPipes(context.Background(), pipes.ListPipesFilter{TargetPrefix: tt.targetPrefix})
			require.NoError(t, err)
			assert.Len(t, result.Pipes, tt.wantCount)
		})
	}
}

// TestListPipes_LexicographicOrder verifies that ListPipes returns pipes
// in stable lexicographic order regardless of insertion order.
func TestListPipes_LexicographicOrder(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pipeNames []string
		wantOrder []string
	}{
		{
			name:      "alphabetic_order",
			pipeNames: []string{"charlie", "alpha", "bravo"},
			wantOrder: []string{"alpha", "bravo", "charlie"},
		},
		{
			name:      "numeric_suffix_order",
			pipeNames: []string{"pipe-10", "pipe-2", "pipe-1"},
			wantOrder: []string{"pipe-1", "pipe-10", "pipe-2"},
		},
		{
			name:      "mixed_case",
			pipeNames: []string{"Zulu", "apple", "Mango"},
			wantOrder: []string{"Mango", "Zulu", "apple"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b3Backend()
			for _, n := range tt.pipeNames {
				_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
					Name:         n,
					RoleARN:      "arn:aws:iam::111122223333:role/r",
					Source:       b3SQSSource,
					Target:       b3LambdaTarget,
					DesiredState: "STOPPED",
				})
				require.NoError(t, err)
			}

			result, err := b.ListPipes(context.Background(), pipes.ListPipesFilter{})
			require.NoError(t, err)
			require.Len(t, result.Pipes, len(tt.pipeNames))

			for i, p := range result.Pipes {
				assert.Equal(t, tt.wantOrder[i], p.Name,
					"pipe at position %d should be %q", i, tt.wantOrder[i])
			}
		})
	}
}
