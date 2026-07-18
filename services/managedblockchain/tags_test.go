package managedblockchain_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/services/managedblockchain"
)

func TestInMemoryBackend_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tag and untag resource"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			network, _, err := b.CreateNetwork(testRegion, testAccountID, "tagged-net", "", "", "", "m1", "", nil, nil)
			require.NoError(t, err)

			// TagResource on network
			err = b.TagResource(network.Arn, map[string]string{"env": "test", "team": "backend"})
			require.NoError(t, err)

			// ListTagsForResource on network
			tags, err := b.ListTagsForResource(network.Arn)
			require.NoError(t, err)
			assert.Equal(t, "test", tags["env"])
			assert.Equal(t, "backend", tags["team"])

			// UntagResource on network
			err = b.UntagResource(network.Arn, []string{"team"})
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(network.Arn)
			require.NoError(t, err)
			assert.Equal(t, "test", tags["env"])
			assert.NotContains(t, tags, "team")
		})
	}
}

func TestInMemoryBackend_TagOperationsOnMember(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tag and untag member"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()

			network, initialMember, err := b.CreateNetwork(
				testRegion,
				testAccountID,
				"net1",
				"",
				"",
				"",
				"initial",
				"",
				nil,
				nil,
			)
			require.NoError(t, err)

			// Tag the initial member
			err = b.TagResource(initialMember.Arn, map[string]string{"role": "primary"})
			require.NoError(t, err)

			// List tags on member
			tags, err := b.ListTagsForResource(initialMember.Arn)
			require.NoError(t, err)
			assert.Equal(t, "primary", tags["role"])

			// Untag member
			err = b.UntagResource(initialMember.Arn, []string{"role"})
			require.NoError(t, err)

			tags, err = b.ListTagsForResource(initialMember.Arn)
			require.NoError(t, err)
			assert.Empty(t, tags)

			// Verify ListTagsForResource on nonexistent ARN
			_, err = b.ListTagsForResource("arn:aws:managedblockchain:us-east-1:000000000000:networks/nonexistent")
			require.Error(t, err)
			assert.ErrorIs(t, err, awserr.ErrNotFound)

			_ = network
		})
	}
}

func TestHandler_TagOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "list tag and untag network"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create network
			rec := doRequest(t, h, http.MethodPost, "/networks",
				map[string]any{"Name": "tagged-net", "MemberConfiguration": map[string]any{"Name": "m1"}})
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			// Get the network to find its ARN
			networkID := createResp["NetworkId"].(string)
			rec = doRequest(t, h, http.MethodGet, "/networks/"+networkID, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var netResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &netResp))
			network := netResp["Network"].(map[string]any)
			arn := network["Arn"].(string)
			assert.NotEmpty(t, arn)

			// TagResource
			rec = doRequest(t, h, http.MethodPost, "/tags/"+arn,
				map[string]any{"Tags": map[string]string{"env": "test"}})
			assert.Equal(t, http.StatusNoContent, rec.Code)

			// ListTagsForResource
			rec = doRequest(t, h, http.MethodGet, "/tags/"+arn, nil)
			assert.Equal(t, http.StatusOK, rec.Code)

			var tagsResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
			tags := tagsResp["Tags"].(map[string]any)
			assert.Equal(t, "test", tags["env"])

			// UntagResource
			rec = doRequest(t, h, http.MethodDelete, "/tags/"+arn+"?tagKeys=env", nil)
			assert.Equal(t, http.StatusNoContent, rec.Code)
		})
	}
}

func TestHandler_TagErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "list tags not found",
			method:     http.MethodGet,
			path:       "/tags/arn:aws:managedblockchain:us-east-1:000000000000:networks/nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "tag resource not found",
			method:     http.MethodPost,
			path:       "/tags/arn:aws:managedblockchain:us-east-1:000000000000:networks/nonexistent",
			body:       map[string]any{"Tags": map[string]string{"k": "v"}},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UntagResourceQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "untag resource with tags",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			netID, _ := createTestNetwork(t, h)

			// Get network ARN.
			netRec := doRequest(t, h, http.MethodGet, "/networks/"+netID, nil)
			require.Equal(t, http.StatusOK, netRec.Code)

			var netResp map[string]any
			require.NoError(t, json.Unmarshal(netRec.Body.Bytes(), &netResp))
			network := netResp["Network"].(map[string]any)
			arn := network["Arn"].(string)

			// Tag it.
			tagRec := doRequest(t, h, http.MethodPost, "/tags/"+arn,
				map[string]any{"Tags": map[string]string{"key1": "val1", "key2": "val2"}})
			require.Equal(t, http.StatusNoContent, tagRec.Code)

			// Untag.
			e := echo.New()
			req := httptest.NewRequest(http.MethodDelete, "/tags/"+arn+"?tagKeys=key1", http.NoBody)
			w := httptest.NewRecorder()
			c := e.NewContext(req, w)
			err := h.Handler()(c)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, w.Code)
		})
	}
}

// TestInMemoryBackend_TagNode verifies nodes can be tagged after creation.
func TestInMemoryBackend_TagNode(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	node := b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")

	err := b.TagResource(node.Arn, map[string]string{"stage": "prod"})
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(node.Arn)
	require.NoError(t, err)
	assert.Equal(t, "prod", tags["stage"])

	err = b.UntagResource(node.Arn, []string{"stage"})
	require.NoError(t, err)

	tags, err = b.ListTagsForResource(node.Arn)
	require.NoError(t, err)
	assert.Empty(t, tags)
}

// TestHandler_TagNodeViaHTTP verifies node tagging works via HTTP handler.
func TestHandler_TagNodeViaHTTP(t *testing.T) {
	t.Parallel()

	b := managedblockchain.NewInMemoryBackend()
	n := b.AddNetworkInternal(testRegion, testAccountID, "net1")
	m := b.AddMemberInternal(testRegion, testAccountID, n.ID, "member1")
	node := b.AddNodeInternal(testRegion, testAccountID, n.ID, m.ID, "bc.t3.small.ethereum")
	h := managedblockchain.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	rec := doRequest(t, h, http.MethodPost, "/tags/"+node.Arn,
		map[string]any{"Tags": map[string]string{"tier": "gold"}})
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec2 := doRequest(t, h, http.MethodGet, "/tags/"+node.Arn, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	tags := resp["Tags"].(map[string]any)
	assert.Equal(t, "gold", tags["tier"])
}
