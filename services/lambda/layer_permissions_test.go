package lambda_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/lambda"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_LayerVersionPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup         func(*lambda.InMemoryBackend)
		name          string
		layerName     string
		statementID   string
		action        string
		principal     string
		version       int64
		wantAddErr    bool
		wantGetErr    bool
		wantRemoveErr bool
	}{
		{
			name: "add_and_get_permission",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			layerName:   "my-layer",
			version:     1,
			statementID: "stmt-1",
			action:      "lambda:GetLayerVersion",
			principal:   "*",
		},
		{
			name:        "layer_not_found",
			layerName:   "missing",
			version:     1,
			statementID: "stmt-1",
			action:      "lambda:GetLayerVersion",
			principal:   "*",
			wantAddErr:  true,
		},
		{
			name: "version_not_found",
			setup: func(bk *lambda.InMemoryBackend) {
				_, _ = bk.PublishLayerVersion(publishLayerInput("my-layer", "", []byte("z"), nil))
			},
			layerName:   "my-layer",
			version:     99,
			statementID: "stmt-1",
			action:      "lambda:GetLayerVersion",
			principal:   "*",
			wantAddErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := newLayerBackend(t)
			if tt.setup != nil {
				tt.setup(bk)
			}

			addInput := &lambda.AddLayerVersionPermissionInput{
				StatementID: tt.statementID,
				Action:      tt.action,
				Principal:   tt.principal,
			}

			addOut, addErr := bk.AddLayerVersionPermission(tt.layerName, tt.version, addInput)

			if tt.wantAddErr {
				require.Error(t, addErr)

				return
			}

			require.NoError(t, addErr)
			assert.NotEmpty(t, addOut.Statement)

			// GetLayerVersionPolicy should include the statement.
			policy, getErr := bk.GetLayerVersionPolicy(tt.layerName, tt.version)
			require.NoError(t, getErr)
			assert.Contains(t, policy.Policy, tt.statementID)

			// RemoveLayerVersionPermission should succeed.
			removeErr := bk.RemoveLayerVersionPermission(tt.layerName, tt.version, tt.statementID, "")
			require.NoError(t, removeErr)

			// Policy should no longer contain the statement.
			policy2, _ := bk.GetLayerVersionPolicy(tt.layerName, tt.version)
			assert.NotContains(t, policy2.Policy, tt.statementID)
		})
	}
}

// --- Layer permission HTTP tests ---

func TestLayerVersionPermission_AddGetRemove(t *testing.T) {
	t.Parallel()

	h, bk := newInMemoryHandler(t)

	// Publish a layer version first
	_, err := bk.PublishLayerVersion(&lambda.PublishLayerVersionInput{
		LayerName:          "my-layer",
		CompatibleRuntimes: []string{"python3.12"},
		Description:        "test layer",
		Content:            &lambda.LayerVersionContentInput{},
	})
	require.NoError(t, err)

	// Add permission
	rec := callInMemoryHandler(
		t, h, http.MethodPost,
		"/2018-10-31/layers/my-layer/versions/1/policy",
		`{"StatementId":"allow-all","Action":"lambda:GetLayerVersion","Principal":"*"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)
	assert.Contains(t, rec.Body.String(), "RevisionId")

	// Get policy
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers/my-layer/versions/1/policy", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "allow-all")

	// Remove permission
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2018-10-31/layers/my-layer/versions/1/policy/allow-all", "{}")
	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestLayerVersionPermission_AddNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	rec := callInMemoryHandler(
		t, h, http.MethodPost,
		"/2018-10-31/layers/nonexistent-layer/versions/1/policy",
		`{"StatementId":"s1","Action":"lambda:GetLayerVersion","Principal":"*"}`,
	)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestLayer_Policy_AddAndGet(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/pol-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)

	addRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/pol-layer/versions/1/policy",
		`{"StatementId":"s1","Action":"lambda:GetLayerVersion","Principal":"*"}`)
	require.Equal(t, http.StatusCreated, addRec.Code)

	var addOut lambda.AddLayerVersionPermissionOutput
	require.NoError(t, json.NewDecoder(addRec.Body).Decode(&addOut))
	assert.NotEmpty(t, addOut.Statement)
	assert.NotEmpty(t, addOut.RevisionID)

	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2018-10-31/layers/pol-layer/versions/1/policy", "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var pol lambda.LayerVersionPolicy
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&pol))
	assert.NotEmpty(t, pol.Policy)
}

// TestLayerVersionPermission_Duplicate locks in that AddLayerVersionPermission
// rejects a StatementId that already exists with ResourceConflictException
// (409), matching AddPermission's function-policy behavior, instead of
// silently overwriting the existing statement.
func TestLayerVersionPermission_Duplicate(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/dup-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)

	body := `{"StatementId":"s1","Action":"lambda:GetLayerVersion","Principal":"*"}`
	path := "/2018-10-31/layers/dup-layer/versions/1/policy"

	rec1 := callInMemoryHandler(t, h, http.MethodPost, path, body)
	require.Equal(t, http.StatusCreated, rec1.Code)

	rec2 := callInMemoryHandler(t, h, http.MethodPost, path, body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

// TestLayerVersionPermission_RevisionId locks in the RevisionId
// optimistic-concurrency check on AddLayerVersionPermission /
// RemoveLayerVersionPermission (query-bound "RevisionId" param, per the AWS
// smithy model — verified against serializers.go): a stale RevisionId is
// rejected with PreconditionFailedException (412) and the policy is left
// unmodified; a fresh one succeeds.
func TestLayerVersionPermission_RevisionId(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/rev-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)

	policyPath := "/2018-10-31/layers/rev-layer/versions/1/policy"

	addRec := callInMemoryHandler(t, h, http.MethodPost, policyPath,
		`{"StatementId":"s1","Action":"lambda:GetLayerVersion","Principal":"*"}`)
	require.Equal(t, http.StatusCreated, addRec.Code)

	var addOut lambda.AddLayerVersionPermissionOutput
	require.NoError(t, json.NewDecoder(addRec.Body).Decode(&addOut))
	rev1 := addOut.RevisionID
	require.NotEmpty(t, rev1)

	// Stale RevisionId on a second Add is rejected.
	staleAddRec := callInMemoryHandler(t, h, http.MethodPost,
		policyPath+"?RevisionId=not-the-real-revision",
		`{"StatementId":"s2","Action":"lambda:GetLayerVersion","Principal":"*"}`)
	assert.Equal(t, http.StatusPreconditionFailed, staleAddRec.Code)

	getRec := callInMemoryHandler(t, h, http.MethodGet, policyPath, "")
	var pol lambda.LayerVersionPolicy
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&pol))
	assert.NotContains(t, pol.Policy, "s2")

	// Stale RevisionId on Remove is rejected and leaves the statement in place.
	staleDelRec := callInMemoryHandler(t, h, http.MethodDelete,
		policyPath+"/s1?RevisionId=not-the-real-revision", "")
	assert.Equal(t, http.StatusPreconditionFailed, staleDelRec.Code)

	getRec2 := callInMemoryHandler(t, h, http.MethodGet, policyPath, "")
	var pol2 lambda.LayerVersionPolicy
	require.NoError(t, json.NewDecoder(getRec2.Body).Decode(&pol2))
	assert.Contains(t, pol2.Policy, "s1")

	// The correct RevisionId succeeds.
	okDelRec := callInMemoryHandler(t, h, http.MethodDelete,
		policyPath+"/s1?RevisionId="+rev1, "")
	assert.Equal(t, http.StatusNoContent, okDelRec.Code)
}

func TestLayer_Policy_Remove(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)

	callInMemoryHandler(t, h, http.MethodPost, "/2018-10-31/layers/pol-rm-layer/versions",
		`{"Content":{"ZipFile":"UEsDBAA="}}`)
	callInMemoryHandler(t, h, http.MethodPost,
		"/2018-10-31/layers/pol-rm-layer/versions/1/policy",
		`{"StatementId":"stmt1","Action":"lambda:GetLayerVersion","Principal":"*"}`)

	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2018-10-31/layers/pol-rm-layer/versions/1/policy/stmt1", "")
	assert.Equal(t, http.StatusNoContent, delRec.Code)
}
