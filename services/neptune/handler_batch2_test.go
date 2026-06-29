package neptune_test

import (
	"maps"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- DeleteDBCluster: DeletionProtection ---

func TestBatch2_DeleteDBCluster_DeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		protection   string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "deletion_protection_enabled",
			protection:   "true",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
		{
			name:         "deletion_protection_disabled",
			protection:   "false",
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-prot-cluster"},
				"DeletionProtection":  {tt.protection},
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-prot-cluster"},
				"SkipFinalSnapshot":   {"true"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- StopDBCluster / StartDBCluster state validation ---

func TestBatch2_StopDBCluster_AlreadyStopped(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "state-cluster")

	// First stop succeeds.
	rr := doRequest(t, h, url.Values{
		"Action":              {"StopDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"state-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Second stop on already-stopped cluster must fail.
	rr = doRequest(t, h, url.Values{
		"Action":              {"StopDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"state-cluster"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidDBClusterStateFault")
}

func TestBatch2_StartDBCluster_AlreadyAvailable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "start-avail-cluster")

	// Cluster is available; starting it must fail.
	rr := doRequest(t, h, url.Values{
		"Action":              {"StartDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"start-avail-cluster"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidDBClusterStateFault")
}

func TestBatch2_StartDBCluster_AfterStop_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "stop-start-cluster")

	doRequest(t, h, url.Values{
		"Action":              {"StopDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"stop-start-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":              {"StartDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"stop-start-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "StartDBClusterResponse")
}

// --- CreateDBInstance: DBClusterIdentifier required ---

func TestBatch2_CreateDBInstance_MissingClusterID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"no-cluster-inst"},
		"DBInstanceClass":      {"db.r5.large"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

// --- PromotionTier range validation ---

func TestBatch2_PromotionTier_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		tier         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "valid_tier_0",
			tier:         "0",
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBInstanceResponse",
		},
		{
			name:         "valid_tier_15",
			tier:         "15",
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBInstanceResponse",
		},
		{
			name:         "invalid_tier_16",
			tier:         "16",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "invalid_tier_negative",
			tier:         "-1",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, "tier-cluster")
			rr := doRequest(t, h, url.Values{
				"Action":               {"CreateDBInstance"},
				"Version":              {"2014-10-31"},
				"DBInstanceIdentifier": {"tier-inst-" + tt.tier},
				"DBClusterIdentifier":  {"tier-cluster"},
				"DBInstanceClass":      {"db.r5.large"},
				"PromotionTier":        {tt.tier},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestBatch2_ModifyDBInstance_PromotionTierInvalid(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "mod-tier-cluster")
	createInstance(t, h, "mod-tier-inst", "mod-tier-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"mod-tier-inst"},
		"PromotionTier":        {"20"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

// --- Parameter group family validation ---

func TestBatch2_CreateDBClusterParameterGroup_InvalidFamily(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		family       string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "invalid_family",
			family:       "mysql5.7",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "empty_family",
			family:       "",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "valid_neptune12",
			family:       "neptune1.2",
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterParameterGroupResponse",
		},
		{
			name:         "valid_neptune13",
			family:       "neptune1.3",
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterParameterGroupResponse",
		},
		{
			name:         "valid_neptune14",
			family:       "neptune1.4",
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBClusterParameterGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":                      {"CreateDBClusterParameterGroup"},
				"Version":                     {"2014-10-31"},
				"DBClusterParameterGroupName": {"pg-" + strings.ReplaceAll(tt.name, "_", "-")},
				"DBParameterGroupFamily":      {tt.family},
				"Description":                 {"test"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestBatch2_CreateDBParameterGroup_InvalidFamily(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		family       string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "invalid_family",
			family:       "aurora-mysql5.7",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "valid_neptune13",
			family:       "neptune1.3",
			wantStatus:   http.StatusOK,
			wantContains: "CreateDBParameterGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":                 {"CreateDBParameterGroup"},
				"Version":                {"2014-10-31"},
				"DBParameterGroupName":   {"pg-" + strings.ReplaceAll(tt.name, "_", "-")},
				"DBParameterGroupFamily": {tt.family},
				"Description":            {"test"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- Tag operations: ARN validation ---

func TestBatch2_Tags_UnknownARN(t *testing.T) {
	t.Parallel()

	unknownARN := "arn:aws:neptune:us-east-1:000000000000:cluster:nonexistent-cluster"

	tests := []struct {
		extra  url.Values
		name   string
		action string
	}{
		{
			name:   "add_tags_unknown_arn",
			action: "AddTagsToResource",
			extra: url.Values{
				"Tags.Tag.1.Key":   {"Env"},
				"Tags.Tag.1.Value": {"test"},
			},
		},
		{
			name:   "list_tags_unknown_arn",
			action: "ListTagsForResource",
		},
		{
			name:   "remove_tags_unknown_arn",
			action: "RemoveTagsFromResource",
			extra: url.Values{
				"TagKeys.member.1": {"Env"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":       {tt.action},
				"Version":      {"2014-10-31"},
				"ResourceName": {unknownARN},
			}
			maps.Copy(vals, tt.extra)
			rr := doRequest(t, h, vals)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
		})
	}
}

// --- Tag validation: key/value length and max count ---

func TestBatch2_Tags_Validation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "tag-val-cluster")
	clusterARN := "arn:aws:neptune:us-east-1:000000000000:cluster:tag-val-cluster"

	t.Run("key_too_long", func(t *testing.T) {
		t.Parallel()

		h2 := newTestHandler(t)
		createCluster(t, h2, "tag-key-cluster")
		arnStr := "arn:aws:neptune:us-east-1:000000000000:cluster:tag-key-cluster"
		longKey := strings.Repeat("k", 129)
		rr := doRequest(t, h2, url.Values{
			"Action":           {"AddTagsToResource"},
			"Version":          {"2014-10-31"},
			"ResourceName":     {arnStr},
			"Tags.Tag.1.Key":   {longKey},
			"Tags.Tag.1.Value": {"v"},
		})
		assert.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
	})

	t.Run("value_too_long", func(t *testing.T) {
		t.Parallel()

		longVal := strings.Repeat("v", 257)
		rr := doRequest(t, h, url.Values{
			"Action":           {"AddTagsToResource"},
			"Version":          {"2014-10-31"},
			"ResourceName":     {clusterARN},
			"Tags.Tag.1.Key":   {"mykey"},
			"Tags.Tag.1.Value": {longVal},
		})
		assert.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
	})

	t.Run("valid_tag_max_key_length", func(t *testing.T) {
		t.Parallel()

		h2 := newTestHandler(t)
		createCluster(t, h2, "tag-maxkey-cluster")
		arnStr := "arn:aws:neptune:us-east-1:000000000000:cluster:tag-maxkey-cluster"
		exactKey := strings.Repeat("k", 128)
		rr := doRequest(t, h2, url.Values{
			"Action":           {"AddTagsToResource"},
			"Version":          {"2014-10-31"},
			"ResourceName":     {arnStr},
			"Tags.Tag.1.Key":   {exactKey},
			"Tags.Tag.1.Value": {"v"},
		})
		assert.Equal(t, http.StatusOK, rr.Code)
	})
}

// --- Tags on real cluster (ARN extracted from response) ---

func TestBatch2_Tags_OnRealCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"real-tag-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Extract ARN from response.
	body := rr.Body.String()
	arnStart := strings.Index(body, "arn:aws:")
	require.Greater(t, arnStart, -1)
	arnEnd := strings.Index(body[arnStart:], "<") + arnStart
	clusterARN := body[arnStart:arnEnd]

	// Add tag.
	rr = doRequest(t, h, url.Values{
		"Action":           {"AddTagsToResource"},
		"Version":          {"2014-10-31"},
		"ResourceName":     {clusterARN},
		"Tags.Tag.1.Key":   {"Project"},
		"Tags.Tag.1.Value": {"gopherstack"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// List tags.
	rr = doRequest(t, h, url.Values{
		"Action":       {"ListTagsForResource"},
		"Version":      {"2014-10-31"},
		"ResourceName": {clusterARN},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "Project")
	assert.Contains(t, rr.Body.String(), "gopherstack")
}
