package codestarconnections_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/codestarconnections"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConnectionName_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		connName   string
		wantStatus int
	}{
		{
			name:       "valid simple name",
			connName:   "my-conn",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid name with dots",
			connName:   "my.conn.1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid name with underscores",
			connName:   "my_conn_1",
			wantStatus: http.StatusOK,
		},
		{
			name:       "valid max length name",
			connName:   "abcdefghijklmnopqrstuvwxyz123456",
			wantStatus: http.StatusOK,
		},
		{
			name:       "name too long (33 chars)",
			connName:   "abcdefghijklmnopqrstuvwxyz1234567",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name with invalid chars (space)",
			connName:   "my conn",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "name with invalid chars (slash)",
			connName:   "my/conn",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty name from body missing ConnectionName",
			connName:   "",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{"ProviderType": "GitHub"}
			if tt.connName != "" {
				body["ConnectionName"] = tt.connName
			}

			rec := doRequest(t, h, "CreateConnection", body)
			assert.Equal(t, tt.wantStatus, rec.Code, "body=%v", body)
		})
	}
}

func TestProviderType_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		providerType string
		wantStatus   int
	}{
		{name: "GitHub", providerType: "GitHub", wantStatus: http.StatusOK},
		{name: "Bitbucket", providerType: "Bitbucket", wantStatus: http.StatusOK},
		{name: "GitLab", providerType: "GitLab", wantStatus: http.StatusOK},
		{name: "GitHubEnterpriseServer", providerType: "GitHubEnterpriseServer", wantStatus: http.StatusOK},
		{name: "GitLabSelfManaged", providerType: "GitLabSelfManaged", wantStatus: http.StatusOK},
		{name: "empty provider type is allowed", providerType: "", wantStatus: http.StatusOK},
		{name: "invalid provider type", providerType: "Subversion", wantStatus: http.StatusBadRequest},
		{name: "case sensitive invalid", providerType: "github", wantStatus: http.StatusBadRequest},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			body := map[string]any{
				"ConnectionName": "conn-" + tt.providerType + "-" + string(rune('0'+i)),
				"ProviderType":   tt.providerType,
			}
			if tt.providerType == "" {
				body["ConnectionName"] = "conn-empty-pt"
			}

			rec := doRequest(t, h, "CreateConnection", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestListConnections_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 5 {
		createCSCConn(t, h, "pag-conn-"+string(rune('a'+i)), "GitHub")
	}

	// First page: MaxResults=3.
	rec1 := doRequest(t, h, "ListConnections", map[string]any{"MaxResults": 3})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseResp(t, rec1)
	conns1, ok := resp1["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, conns1, 3)
	nextToken, hasNext := resp1["NextToken"].(string)
	assert.True(t, hasNext && nextToken != "", "expected NextToken for first page")

	// Second page.
	rec2 := doRequest(t, h, "ListConnections", map[string]any{
		"MaxResults": 3,
		"NextToken":  nextToken,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseResp(t, rec2)
	conns2, ok := resp2["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, conns2, 2)
	assert.Empty(t, resp2["NextToken"], "no NextToken on last page")

	// Collect all names and verify they're the same set.
	allNames := make(map[string]bool)
	for _, c := range conns1 {
		cm := c.(map[string]any)
		allNames[cm["ConnectionName"].(string)] = true
	}

	for _, c := range conns2 {
		cm := c.(map[string]any)
		allNames[cm["ConnectionName"].(string)] = true
	}

	assert.Len(t, allNames, 5)
}

func TestConnection_HostArnOmittedWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "no-host-conn", "GitHub")

	rec := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	conn := resp["Connection"].(map[string]any)
	_, hasHostArn := conn["HostArn"]
	assert.False(t, hasHostArn, "HostArn should be omitted when empty")
}

func TestConnection_HostArnIncludedWhenSet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	const fakeHostArn = "arn:aws:codestar-connections:us-east-1:000000000000:host/myhost/abc12345"

	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "has-host-conn",
		"ProviderType":   "GitHubEnterpriseServer",
		"HostArn":        fakeHostArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	connArn := resp["ConnectionArn"].(string)

	getRec := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
	require.Equal(t, http.StatusOK, getRec.Code)

	getResp := parseResp(t, getRec)
	conn := getResp["Connection"].(map[string]any)
	assert.Equal(t, fakeHostArn, conn["HostArn"])
}

func TestListConnections_EmptyIsArray(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "ListConnections", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	conns, ok := resp["Connections"].([]any)
	require.True(t, ok, "Connections should be an array, not null")
	assert.Empty(t, conns)
}

func TestCreateConnection_TagsRoundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionName": "tag-rt-conn",
		"ProviderType":   "GitHub",
		"Tags": []map[string]string{
			{"Key": "env", "Value": "prod"},
			{"Key": "team", "Value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	tags, ok := resp["Tags"].([]any)
	require.True(t, ok, "CreateConnection must echo Tags in response (real CreateConnectionOutput.Tags)")
	require.Len(t, tags, 2)

	tag0 := tags[0].(map[string]any)
	tag1 := tags[1].(map[string]any)
	assert.Equal(t, "env", tag0["Key"])
	assert.Equal(t, "team", tag1["Key"])

	arn := resp["ConnectionArn"].(string)
	recTags := doRequest(t, h, "ListTagsForResource", map[string]any{"ResourceArn": arn})
	require.Equal(t, http.StatusOK, recTags.Code)

	tagsResp := parseResp(t, recTags)
	listTags := tagsResp["Tags"].([]any)
	require.Len(t, listTags, 2)
}

func TestConnection_StatusAvailableOnCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	connArn := createCSCConn(t, h, "status-avail-conn", "GitHub")

	rec := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseResp(t, rec)
	conn := resp["Connection"].(map[string]any)
	assert.Equal(t, "AVAILABLE", conn["ConnectionStatus"])
}

func TestPagination_NoNextTokenWhenFits(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for i := range 3 {
		createCSCConn(t, h, "fit-conn-"+string(rune('a'+i)), "GitHub")
	}

	// MaxResults=10 should show all 3 with no NextToken.
	rec := doRequest(t, h, "ListConnections", map[string]any{"MaxResults": 10})
	require.Equal(t, http.StatusOK, rec.Code)
	resp := parseResp(t, rec)
	conns, ok := resp["Connections"].([]any)
	require.True(t, ok)
	assert.Len(t, conns, 3)
	assert.Empty(t, resp["NextToken"])
}

// TestCreateConnection_WithHostArn verifies HostArn is accepted.
func TestCreateConnection_WithHostArn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		wantArn bool
		wantOK  bool
	}{
		{
			name: "with_host_arn",
			body: map[string]any{
				"ConnectionName": "ghe-conn",
				"ProviderType":   "GitHubEnterpriseServer",
				"HostArn":        "arn:aws:codestar-connections:us-east-1:123:host/ghe",
			},
			wantArn: true,
			wantOK:  true,
		},
		{
			name: "without_host_arn",
			body: map[string]any{
				"ConnectionName": "gh-conn",
				"ProviderType":   "GitHub",
			},
			wantArn: true,
			wantOK:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateConnection", tt.body)

			if tt.wantOK {
				require.Equal(t, http.StatusOK, rec.Code)
			}

			if tt.wantArn {
				resp := parseResp(t, rec)
				assert.NotEmpty(t, resp["ConnectionArn"])
			}
		})
	}
}

// TestGetConnection_Fields verifies all expected fields are returned.
func TestGetConnection_Fields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		connName      string
		providerType  string
		wantStatus    string
		wantOwnerAcct string
	}{
		{
			name:          "github_connection_fields",
			connName:      "my-gh-conn",
			providerType:  "GitHub",
			wantStatus:    "AVAILABLE",
			wantOwnerAcct: "000000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			connArn := createCSCConn(t, h, tt.connName, tt.providerType)

			rec := doRequest(t, h, "GetConnection", map[string]any{"ConnectionArn": connArn})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			conn, ok := resp["Connection"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.connName, conn["ConnectionName"])
			assert.Equal(t, tt.providerType, conn["ProviderType"])
			assert.Equal(t, tt.wantStatus, conn["ConnectionStatus"])
			assert.Equal(t, tt.wantOwnerAcct, conn["OwnerAccountId"])
			assert.Equal(t, connArn, conn["ConnectionArn"])
		})
	}
}

// TestListConnections_Sorted verifies connections are returned sorted by name.
func TestListConnections_Sorted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		connNames []string
		wantOrder []string
	}{
		{
			name:      "sorted_alpha",
			connNames: []string{"zebra-conn", "alpha-conn", "mango-conn"},
			wantOrder: []string{"alpha-conn", "mango-conn", "zebra-conn"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for _, n := range tt.connNames {
				createCSCConn(t, h, n, "GitHub")
			}

			rec := doRequest(t, h, "ListConnections", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			conns, ok := resp["Connections"].([]any)
			require.True(t, ok)
			require.Len(t, conns, len(tt.wantOrder))

			for i, wantName := range tt.wantOrder {
				connMap, isMap := conns[i].(map[string]any)
				require.True(t, isMap)
				assert.Equal(t, wantName, connMap["ConnectionName"])
			}
		})
	}
}

// TestListConnections_HostArnFilter verifies filtering by HostArn.
func TestListConnections_HostArnFilter(t *testing.T) {
	t.Parallel()

	const hostArn = "arn:aws:codestar-connections:us-east-1:123:host/myghe"

	tests := []struct {
		name        string
		applyFilter bool
		wantCount   int
	}{
		{name: "no_filter_returns_all", applyFilter: false, wantCount: 2},
		{name: "host_filter_returns_one", applyFilter: true, wantCount: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := codestarconnections.NewInMemoryBackend("000000000000", "us-east-1")
			h := codestarconnections.NewHandler(b)

			_, err := b.CreateConnection(context.Background(), "ghe-conn", "GitHubEnterpriseServer", hostArn, nil)
			require.NoError(t, err)

			_, err = b.CreateConnection(context.Background(), "gh-conn", "GitHub", "", nil)
			require.NoError(t, err)

			body := map[string]any{}
			if tt.applyFilter {
				body["HostArnFilter"] = hostArn
			}

			rec := doRequest(t, h, "ListConnections", body)
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseResp(t, rec)
			conns, ok := resp["Connections"].([]any)
			require.True(t, ok)
			assert.Len(t, conns, tt.wantCount)
		})
	}
}
