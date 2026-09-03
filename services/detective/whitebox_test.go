package detective

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func doWhiteboxRequest(t *testing.T, h *Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var marshalErr error

		bodyBytes, marshalErr = json.Marshal(body)
		require.NoError(t, marshalErr)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func seedGraph(b *InMemoryBackend, arnStr string) {
	b.mu.Lock("test.seedGraph")
	defer b.mu.Unlock()
	b.graphs.Put(&storedGraph{Arn: arnStr, CreatedTime: time.Now().UTC()})
}

// seedMember inserts a synthetic member record directly into the backend,
// bypassing CreateMembers (which forbids the backend's own account from
// inviting itself). Used only to exercise ListInvitations pagination with
// multiple entries, since ListInvitations returns graphs where b.accountID
// itself holds a membership -- something the normal CreateMembers/
// AcceptInvitation flow can never produce for the account that owns the
// backend instance under test.
func seedMember(b *InMemoryBackend, graphARN, accountID, status string) {
	b.mu.Lock("test.seedMember")
	defer b.mu.Unlock()

	now := time.Now().UTC()
	b.members.Put(&storedMember{
		InvitedTime:     now,
		UpdatedTime:     now,
		AccountID:       accountID,
		AdministratorID: "999999999999",
		EmailAddress:    "seed@example.com",
		GraphARN:        graphARN,
		Status:          status,
	})
}

// TestListInvitationsOpaqueToken verifies ListInvitations' NextToken is an
// opaque base64 offset, not the raw next GraphArn. A single backend instance
// can never be a member of more than one graph through the normal
// CreateMembers/AcceptInvitation flow (an account cannot invite itself), so
// this seeds member records directly to exercise multi-page pagination.
func TestListInvitationsOpaqueToken(t *testing.T) {
	t.Parallel()
	b := NewInMemoryBackend("000000000000", "us-east-1")
	h := NewHandler(b)

	seedGraph(b, "arn:aws:detective:us-east-1:111111111111:graph:aaaabbbbcccc00001111222233334444")
	seedGraph(b, "arn:aws:detective:us-east-1:222222222222:graph:bbbbccccdddd00001111222233335555")
	seedMember(b, "arn:aws:detective:us-east-1:111111111111:graph:aaaabbbbcccc00001111222233334444",
		b.AccountID(), "INVITED")
	seedMember(b, "arn:aws:detective:us-east-1:222222222222:graph:bbbbccccdddd00001111222233335555",
		b.AccountID(), "ENABLED")

	rec := doWhiteboxRequest(t, h, http.MethodPost, "/invitations/list", map[string]any{"MaxResults": 1})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tok, hasTok := resp["NextToken"].(string)
	require.True(t, hasTok, "NextToken must be present when more results exist")

	_, err := base64.StdEncoding.DecodeString(tok)
	require.NoError(t, err, "NextToken must be opaque base64, not a raw GraphArn")

	rec2 := doWhiteboxRequest(
		t,
		h,
		http.MethodPost,
		"/invitations/list",
		map[string]any{"MaxResults": 1, "NextToken": tok},
	)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	invitations, ok := resp2["Invitations"].([]any)
	require.True(t, ok)
	assert.Len(t, invitations, 1, "second page must return the remaining invitation")

	_, hasTok2 := resp2["NextToken"]
	assert.False(t, hasTok2, "NextToken must be absent on the last page")
}

// TestDecodePageToken_NegativeOffset verifies that a token decoding to a
// negative offset is rejected, matching every other caller's malformed-token
// handling, rather than reaching graphs[start:end] as a negative slice bound.
// LTU= is base64 for "-5".
func TestDecodePageToken_NegativeOffset(t *testing.T) {
	t.Parallel()

	const negativeToken = "LTU="

	_, err := decodePageToken(negativeToken)
	require.Error(t, err, "a negative-offset token must be rejected, not accepted as -5")
}

// TestListGraphs_NegativeToken verifies ListGraphs does not panic when handed
// a continuation token that decodes to a negative offset.
func TestListGraphs_NegativeToken(t *testing.T) {
	t.Parallel()

	const negativeToken = "LTU="

	b := NewInMemoryBackend("000000000000", "us-east-1")
	seedGraph(b, "arn:aws:detective:us-east-1:111111111111:graph:aaaabbbbcccc00001111222233334444")

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("ListGraphs panicked on negative-offset token: %v", r)
			}
		}()

		_, _, err := b.ListGraphs(10, negativeToken)
		require.Error(t, err, "a negative-offset token must be rejected")
	}()
}
