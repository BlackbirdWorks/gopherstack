package ssm_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ssm"
)

func TestGetParametersByPath_PaginationManyPages(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	ctx := context.Background()

	// Insert 25 parameters under /paginate/.
	for i := range 25 {
		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  fmt.Sprintf("/paginate/p%02d", i),
			Value: fmt.Sprintf("v%d", i),
			Type:  "String",
		})
		require.NoError(t, err)
	}

	// Collect all via paginated calls (MaxResults=10).
	var allNames []string
	var nextToken string

	for {
		maxResults := int64(10)
		out, err := b.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{
			Path:       "/paginate/",
			MaxResults: &maxResults,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		for _, p := range out.Parameters {
			allNames = append(allNames, p.Name)
		}

		if out.NextToken == "" {
			break
		}

		nextToken = out.NextToken
	}

	assert.Len(t, allNames, 25, "pagination must return all 25 parameters")

	// All names must be unique.
	seen := make(map[string]bool, len(allNames))
	for _, n := range allNames {
		assert.False(t, seen[n], "duplicate parameter %q in paginated results", n)
		seen[n] = true
	}

	// All must be under /paginate/.
	for _, n := range allNames {
		assert.True(t, strings.HasPrefix(n, "/paginate/"), "unexpected name %q", n)
	}
}

// TestCollectPathParams_LinearScan verifies GetParametersByPath works correctly
// using the linear-scan approach after removal of the sorted-slice index.
func TestCollectPathParams_LinearScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		params    []string // parameter names to insert
		name      string
		path      string
		wantNames []string
		recursive bool
	}{
		{
			name:      "non-recursive picks direct children only",
			params:    []string{"/a/b", "/a/b/c", "/a/d"},
			path:      "/a/",
			recursive: false,
			wantNames: []string{"/a/b", "/a/d"},
		},
		{
			name:      "recursive picks all descendants",
			params:    []string{"/x/y", "/x/y/z", "/x/other"},
			path:      "/x/",
			recursive: true,
			wantNames: []string{"/x/other", "/x/y", "/x/y/z"},
		},
		{
			name:      "no match returns empty slice",
			params:    []string{"/a/b"},
			path:      "/z/",
			recursive: true,
			wantNames: []string{},
		},
		{
			name:      "results sorted by name",
			params:    []string{"/p/z", "/p/a", "/p/m"},
			path:      "/p/",
			recursive: false,
			wantNames: []string{"/p/a", "/p/m", "/p/z"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			b := ssm.NewInMemoryBackend()

			for _, name := range tc.params {
				_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
					Name:  name,
					Type:  ssm.StringType,
					Value: "v",
				})
				require.NoError(t, err)
			}

			// Path without trailing slash is normalised by GetParametersByPath.
			path := tc.path
			if len(path) > 0 && path[len(path)-1] == '/' {
				path = path[:len(path)-1]
			}

			out, err := b.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{
				Path:      path,
				Recursive: tc.recursive,
			})
			require.NoError(t, err)

			got := make([]string, 0, len(out.Parameters))
			for _, p := range out.Parameters {
				got = append(got, p.Name)
			}

			if len(tc.wantNames) == 0 {
				require.Empty(t, got)
			} else {
				require.Equal(t, tc.wantNames, got)
			}
		})
	}
}
func TestDescribeParameters_TierFilter(t *testing.T) {
	t.Parallel()
	h := newHandler()

	postJSON(t, h, "PutParameter", map[string]any{"Name": "/p/a", "Type": "String", "Value": "v", "Tier": "Standard"})
	postJSON(t, h, "PutParameter", map[string]any{"Name": "/p/b", "Type": "String", "Value": "v", "Tier": "Advanced"})

	code, out := postJSON(t, h, "DescribeParameters", map[string]any{
		"ParameterFilters": []map[string]any{
			{"Key": "Tier", "Values": []string{"Advanced"}},
		},
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/p/b", params[0].(map[string]any)["Name"])
}
func TestDescribeParameters_DataTypeFilter(t *testing.T) {
	t.Parallel()
	h := newHandler()

	postJSON(t, h, "PutParameter", map[string]any{"Name": "/q/a", "Type": "String", "Value": "v", "DataType": "text"})
	postJSON(
		t,
		h,
		"PutParameter",
		map[string]any{"Name": "/q/b", "Type": "String", "Value": "ami-xyz", "DataType": "aws:ec2:image"},
	)

	code, out := postJSON(t, h, "DescribeParameters", map[string]any{
		"ParameterFilters": []map[string]any{
			{"Key": "DataType", "Values": []string{"aws:ec2:image"}},
		},
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/q/b", params[0].(map[string]any)["Name"])
}
func TestDescribeParameters_MetadataHasTierAndDataType(t *testing.T) {
	t.Parallel()
	h := newHandler()

	postJSON(t, h, "PutParameter", map[string]any{
		"Name":     "/m/x",
		"Type":     "String",
		"Value":    "v",
		"Tier":     "Advanced",
		"DataType": "text",
	})

	code, out := postJSON(t, h, "DescribeParameters", map[string]any{})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	require.Len(t, params, 1)
	meta := params[0].(map[string]any)
	assert.Equal(t, "Advanced", meta["Tier"])
	assert.Equal(t, "text", meta["DataType"])
}
func TestGetParameterHistory_HasNewFields(t *testing.T) {
	t.Parallel()
	h := newHandler()

	postJSON(t, h, "PutParameter", map[string]any{
		"Name":           "/hist/p",
		"Type":           "String",
		"Value":          "v1",
		"Tier":           "Standard",
		"DataType":       "text",
		"AllowedPattern": ".*",
	})

	code, out := postJSON(t, h, "GetParameterHistory", map[string]any{"Name": "/hist/p"})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	require.Len(t, params, 1)
	entry := params[0].(map[string]any)
	assert.Equal(t, "Standard", entry["Tier"])
	assert.Equal(t, "text", entry["DataType"])
	assert.Equal(t, ".*", entry["AllowedPattern"])
}
func TestGetParametersByPath_NoPrefixCollision(t *testing.T) {
	t.Parallel()
	h := newHandler()

	// /app and /application share a prefix — /app/ must not match /application/key
	for _, name := range []string{"/app/x", "/application/key"} {
		postJSON(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := postJSON(t, h, "GetParametersByPath", map[string]any{
		"Path": "/app",
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/app/x", params[0].(map[string]any)["Name"])
}
func TestGetParametersByPath_RecursiveFindsAll(t *testing.T) {
	t.Parallel()
	h := newHandler()

	for _, name := range []string{"/svc/a", "/svc/b/c", "/svc/b/d"} {
		postJSON(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := postJSON(t, h, "GetParametersByPath", map[string]any{
		"Path":      "/svc",
		"Recursive": true,
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 3)
}
func TestGetParametersByPath_NonRecursiveDirectOnly(t *testing.T) {
	t.Parallel()
	h := newHandler()

	for _, name := range []string{"/svc2/a", "/svc2/b/c"} {
		postJSON(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := postJSON(t, h, "GetParametersByPath", map[string]any{
		"Path":      "/svc2",
		"Recursive": false,
	})

	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
	assert.Equal(t, "/svc2/a", params[0].(map[string]any)["Name"])
}
func TestGetParametersByPath_TrailingSlashEquivalent(t *testing.T) {
	t.Parallel()
	h := newHandler()

	postJSON(t, h, "PutParameter", map[string]any{"Name": "/env/key", "Type": "String", "Value": "v"})

	code1, out1 := postJSON(t, h, "GetParametersByPath", map[string]any{"Path": "/env"})
	code2, out2 := postJSON(t, h, "GetParametersByPath", map[string]any{"Path": "/env/"})

	assert.Equal(t, http.StatusOK, code1)
	assert.Equal(t, http.StatusOK, code2)
	assert.Len(t, out2["Parameters"].([]any), len(out1["Parameters"].([]any)))
}
func TestSSMHandler_AdditionalOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		action   string
		setup    func(b *ssm.InMemoryBackend)
		body     string
		wantCode int
	}{
		{
			name:   "GetParameterHistory_success",
			action: "GetParameterHistory",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(
					context.TODO(),
					&ssm.PutParameterInput{Name: "/hist-param", Type: "String", Value: "v1"},
				)
			},
			body:     `{"Name":"/hist-param"}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "GetParameterHistory_invalid_json",
			action:   "GetParameterHistory",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:   "DeleteParameters_success",
			action: "DeleteParameters",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(
					context.TODO(),
					&ssm.PutParameterInput{Name: "/del-p1", Type: "String", Value: "v"},
				)
			},
			body:     `{"Names":["/del-p1","/del-missing"]}`,
			wantCode: http.StatusOK,
		},
		{
			name:   "GetParametersByPath_success",
			action: "GetParametersByPath",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(
					context.TODO(),
					&ssm.PutParameterInput{Name: "/app/config", Type: "String", Value: "v"},
				)
			},
			body:     `{"Path":"/app","Recursive":true}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "GetParametersByPath_invalid_json",
			action:   "GetParametersByPath",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:   "DescribeParameters_success",
			action: "DescribeParameters",
			setup: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(
					context.TODO(),
					&ssm.PutParameterInput{Name: "/desc-param", Type: "String", Value: "v"},
				)
			},
			body:     `{}`,
			wantCode: http.StatusOK,
		},
		{
			name:     "GetParameters_invalid_json",
			action:   "GetParameters",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "DeleteParameter_invalid_json",
			action:   "DeleteParameter",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "DeleteParameters_invalid_json",
			action:   "DeleteParameters",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "AddTagsToResource_invalid_json",
			action:   "AddTagsToResource",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "RemoveTagsFromResource_invalid_json",
			action:   "RemoveTagsFromResource",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "ListTagsForResource_invalid_json",
			action:   "ListTagsForResource",
			body:     `{invalid`,
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(b)
			}

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
func TestSSMBounds_GetParameterHistory(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, putErr := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/hist/p", Value: "v1", Type: "String"})
	require.NoError(t, putErr)

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
		{ptr64(-1), "-1 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
				Name:       "/hist/p",
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				require.Error(t, err)
				assert.Nil(t, out)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}
func TestSSMPagination_GetParameterHistory(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, putErr := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/hist/multi", Value: "v1", Type: "String"})
	require.NoError(t, putErr)

	for i := 2; i <= 5; i++ {
		_, putErr = b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:      "/hist/multi",
			Value:     fmt.Sprintf("v%d", i),
			Type:      "String",
			Overwrite: true,
		})
		require.NoError(t, putErr)
	}

	maxR := ptr64(2)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{
			Name:       "/hist/multi",
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.Parameters)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total, "all 5 versions seen")
	assert.Equal(t, 3, pages, "ceil(5/2)=3 pages")
}
func TestSSMBounds_GetParametersByPath(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, putErr := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/path/a", Value: "v", Type: "String"})
	require.NoError(t, putErr)

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default 10", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(10), "10 is valid cap", false},
		{ptr64(11), "11 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{
				Path:       "/path/",
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}
func TestSSMPagination_GetParametersByPath(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	for i := range 5 {
		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  fmt.Sprintf("/bypath/param-%02d", i),
			Value: "v",
			Type:  "String",
		})
		require.NoError(t, err)
	}

	maxR := ptr64(2)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{
			Path:       "/bypath/",
			Recursive:  true,
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.Parameters)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 5, total)
	assert.Equal(t, 3, pages)
}
func TestSSMBounds_DescribeParameters(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	_, putErr := b.PutParameter(ctx, &ssm.PutParameterInput{Name: "/desc/p", Value: "v", Type: "String"})
	require.NoError(t, putErr)

	tests := []struct {
		maxResults *int64
		name       string
		wantError  bool
	}{
		{nil, "nil uses default", false},
		{ptr64(1), "1 is valid", false},
		{ptr64(50), "50 is valid cap", false},
		{ptr64(51), "51 exceeds cap", true},
		{ptr64(0), "0 is invalid", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.DescribeParameters(ctx, &ssm.DescribeParametersInput{
				MaxResults: tc.maxResults,
			})

			if tc.wantError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.NotNil(t, out)
			}
		})
	}
}
func TestSSMPagination_DescribeParameters(t *testing.T) {
	t.Parallel()

	_, b := newTestHandler(t)
	ctx := context.Background()

	for i := range 7 {
		_, err := b.PutParameter(ctx, &ssm.PutParameterInput{
			Name:  fmt.Sprintf("/describe/param-%02d", i),
			Value: "v",
			Type:  "String",
		})
		require.NoError(t, err)
	}

	maxR := ptr64(3)
	var nextToken string
	total := 0
	pages := 0

	for {
		out, err := b.DescribeParameters(ctx, &ssm.DescribeParametersInput{
			MaxResults: maxR,
			NextToken:  nextToken,
		})
		require.NoError(t, err)

		total += len(out.Parameters)
		pages++
		nextToken = out.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Equal(t, 7, total)
	assert.Equal(t, 3, pages) // 3+3+1
}
func TestFull_ParameterStore_History_Versions(t *testing.T) {
	t.Parallel()
	h := newHandler()

	for i := range 3 {
		postJSON(t, h, "PutParameter", map[string]any{
			"Name":      "/hist/v",
			"Type":      "String",
			"Value":     "version",
			"Overwrite": i > 0,
		})
	}

	code, out := postJSON(t, h, "GetParameterHistory", map[string]any{"Name": "/hist/v"})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 3)
}
func TestFull_ParameterStore_History_Pagination(t *testing.T) {
	t.Parallel()
	h := newHandler()

	for i := range 5 {
		postJSON(t, h, "PutParameter", map[string]any{
			"Name":      "/paginated/p",
			"Type":      "String",
			"Value":     "v",
			"Overwrite": i > 0,
		})
	}

	maxR := int64(2)
	code, out := postJSON(t, h, "GetParameterHistory", map[string]any{
		"Name":       "/paginated/p",
		"MaxResults": maxR,
	})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 2)
	assert.NotEmpty(t, out["NextToken"])
}
func TestFull_ParameterStore_DescribeParameters_Pagination(t *testing.T) {
	t.Parallel()
	h := newHandler()

	for i := range 5 {
		name := "/pg/" + string(rune('a'+i))
		postJSON(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	maxR := int64(2)
	code, out := postJSON(t, h, "DescribeParameters", map[string]any{"MaxResults": &maxR})
	assert.Equal(t, http.StatusOK, code)
	assert.Len(t, out["Parameters"].([]any), 2)
	nextToken := out["NextToken"].(string)
	assert.NotEmpty(t, nextToken)

	// Next page
	code, out = postJSON(t, h, "DescribeParameters", map[string]any{
		"MaxResults": &maxR,
		"NextToken":  nextToken,
	})
	assert.Equal(t, http.StatusOK, code)
	assert.Len(t, out["Parameters"].([]any), 2)
}
func TestFull_DescribeParameters_NameFilter_BeginsWith(t *testing.T) {
	t.Parallel()
	h := newHandler()

	for _, name := range []string{"/prod/a", "/prod/b", "/dev/c"} {
		postJSON(t, h, "PutParameter", map[string]any{"Name": name, "Type": "String", "Value": "v"})
	}

	code, out := postJSON(t, h, "DescribeParameters", map[string]any{
		"ParameterFilters": []map[string]any{
			{"Key": "Name", "Option": "BeginsWith", "Values": []string{"/prod/"}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 2)
}
func TestFull_DescribeParameters_TypeFilter(t *testing.T) {
	t.Parallel()
	h := newHandler()

	postJSON(t, h, "PutParameter", map[string]any{"Name": "/t/str", "Type": "String", "Value": "v"})
	postJSON(t, h, "PutParameter", map[string]any{"Name": "/t/sl", "Type": "StringList", "Value": "a,b,c"})

	code, out := postJSON(t, h, "DescribeParameters", map[string]any{
		"ParameterFilters": []map[string]any{
			{"Key": "Type", "Values": []string{"StringList"}},
		},
	})
	assert.Equal(t, http.StatusOK, code)
	params := out["Parameters"].([]any)
	assert.Len(t, params, 1)
}

// TestGetParameterHistory_Pagination exercises pagination with nextToken.
func TestGetParameterHistory_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		nextToken string
		wantEmpty bool
	}{
		{
			name:      "beyond_end_next_token",
			nextToken: "999",
			wantEmpty: true,
		},
		{
			name:      "valid_next_token",
			nextToken: "1",
			wantEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ssm.NewInMemoryBackend()
			_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: "/paged", Type: "String", Value: "v1"})
			_, _ = b.PutParameter(
				context.TODO(),
				&ssm.PutParameterInput{Name: "/paged", Type: "String", Value: "v2", Overwrite: true},
			)
			_, _ = b.PutParameter(
				context.TODO(),
				&ssm.PutParameterInput{Name: "/paged", Type: "String", Value: "v3", Overwrite: true},
			)

			out, err := b.GetParameterHistory(context.TODO(), &ssm.GetParameterHistoryInput{
				Name:      "/paged",
				NextToken: tt.nextToken,
			})
			require.NoError(t, err)
			if tt.wantEmpty {
				assert.Empty(t, out.Parameters)
			} else {
				assert.NotEmpty(t, out.Parameters)
			}
		})
	}
}

// TestGetParametersByPath_StartBeyondEnd exercises the beyond-end pagination branch.
func TestGetParametersByPath_StartBeyondEnd(t *testing.T) {
	t.Parallel()

	b := ssm.NewInMemoryBackend()
	_, _ = b.PutParameter(context.TODO(), &ssm.PutParameterInput{Name: "/app/x", Type: "String", Value: "v"})

	out, err := b.GetParametersByPath(context.TODO(), &ssm.GetParametersByPathInput{
		Path:      "/app",
		Recursive: true,
		NextToken: "999",
	})
	require.NoError(t, err)
	assert.Empty(t, out.Parameters)
}
