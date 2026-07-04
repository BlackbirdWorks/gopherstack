package mq_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mq"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func newAudit2Handler(t *testing.T) *mq.Handler {
	t.Helper()

	return mq.NewHandler(mq.NewInMemoryBackend("123456789012", "us-east-1"))
}

func createAudit2Broker(t *testing.T, h *mq.Handler, name string) string {
	t.Helper()

	rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
		"brokerName": name,
		"engineType": mq.EngineTypeActiveMQ,
	})
	require.Equal(t, http.StatusAccepted, rec.Code, "CreateBroker failed: %s", rec.Body.String())

	return parseAccuracyMQ(t, rec)["brokerId"].(string)
}

// ── Username validation ───────────────────────────────────────────────────────

func TestMQ_Audit2_UsernameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		username string
		name     string
		wantCode int
	}{
		{
			name:     "valid username alphanumeric",
			username: "alice",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid username with hyphen",
			username: "alice-b",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid username with underscore",
			username: "alice_b",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid username exactly 2 chars",
			username: "ab",
			wantCode: http.StatusOK,
		},
		{
			name:     "valid username exactly 100 chars",
			username: strings.Repeat("a", 100),
			wantCode: http.StatusOK,
		},
		{
			name:     "username too short (1 char) rejected",
			username: "a",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "username too long (101 chars) rejected",
			username: strings.Repeat("a", 101),
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "username starting with hyphen rejected",
			username: "-bad",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "username starting with underscore rejected",
			username: "_bad",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "username with @-sign rejected",
			username: "user@host",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "username with colon rejected",
			username: "user:name",
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "username with comma rejected",
			username: "user,name",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAudit2Handler(t)
			bid := createAudit2Broker(t, h, "test-broker")

			rec := doAccuracyMQ(t, h, http.MethodPost,
				fmt.Sprintf("/v1/brokers/%s/users/%s", bid, tt.username),
				map[string]any{"password": "ValidPass12!!"})
			assert.Equal(t, tt.wantCode, rec.Code, "username=%q: %s", tt.username, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				body := parseAccuracyMQ(t, rec)
				assert.Equal(t, "BadRequestException", body["__type"])
			}
		})
	}
}

// TestMQ_Audit2_UsernameValidation_Backend covers characters that cannot appear
// in a URL path (e.g. space) and must be tested at the backend level.
func TestMQ_Audit2_UsernameValidation_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		username string
		name     string
		wantErr  bool
	}{
		{name: "space in username rejected", username: "bad user", wantErr: true},
		{name: "slash in username rejected", username: "bad/user", wantErr: true},
		{name: "valid underscore username", username: "valid_user", wantErr: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mq.NewInMemoryBackend("123456789012", "us-east-1")
			br, err := b.CreateBrokerWithOptions(
				"bk", mq.DeploymentModeSingleInstance, mq.EngineTypeActiveMQ,
				"", "", false, false, nil, nil, nil, nil, nil,
			)
			require.NoError(t, err)

			err = b.CreateUser(br.BrokerID, tt.username, "ValidPass12!!", nil, false)
			if tt.wantErr {
				assert.Error(t, err, "username=%q should be rejected", tt.username)
			} else {
				assert.NoError(t, err, "username=%q should be accepted", tt.username)
			}
		})
	}
}

// ── ActiveMQ max-user-count limit ─────────────────────────────────────────────

func TestMQ_Audit2_ActiveMQ_MaxUsers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		count    int
		wantCode int
	}{
		{
			name:     "creating 249th user succeeds",
			count:    249,
			wantCode: http.StatusOK,
		},
		{
			name:     "creating 250th user succeeds (at limit)",
			count:    250,
			wantCode: http.StatusOK,
		},
		{
			name:     "creating 251st user fails (over limit)",
			count:    251,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mq.NewInMemoryBackend("123456789012", "us-east-1")
			h := mq.NewHandler(b)

			bid := createAudit2Broker(t, h, "big-broker")

			// Seed users up to tt.count-1 via backend to avoid HTTP overhead.
			for i := range tt.count - 1 {
				err := b.CreateUser(bid, fmt.Sprintf("user%04d", i), "ValidPass12!!", nil, false)
				require.NoError(t, err)
			}

			// The last create via HTTP is the one under test.
			username := fmt.Sprintf("user%04d", tt.count-1)
			rec := doAccuracyMQ(t, h, http.MethodPost,
				fmt.Sprintf("/v1/brokers/%s/users/%s", bid, username),
				map[string]any{"password": "ValidPass12!!"})
			assert.Equal(t, tt.wantCode, rec.Code, "count=%d: %s", tt.count, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				body := parseAccuracyMQ(t, rec)
				assert.Equal(t, "BadRequestException", body["__type"])
			}
		})
	}
}

// ── ActiveMQ max-groups-per-user limit ───────────────────────────────────────

func TestMQ_Audit2_ActiveMQ_MaxGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		groups   []string
		wantCode int
	}{
		{
			name:     "zero groups accepted",
			groups:   nil,
			wantCode: http.StatusOK,
		},
		{
			name:     "20 groups accepted (at limit)",
			groups:   makeGroups(20),
			wantCode: http.StatusOK,
		},
		{
			name:     "21 groups rejected (over limit)",
			groups:   makeGroups(21),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAudit2Handler(t)
			bid := createAudit2Broker(t, h, "groups-broker")

			body := map[string]any{"password": "ValidPass12!!"}
			if tt.groups != nil {
				body["groups"] = tt.groups
			}

			rec := doAccuracyMQ(t, h, http.MethodPost,
				fmt.Sprintf("/v1/brokers/%s/users/testuser", bid),
				body)
			assert.Equal(t, tt.wantCode, rec.Code, "groups=%d: %s", len(tt.groups), rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				resp := parseAccuracyMQ(t, rec)
				assert.Equal(t, "BadRequestException", resp["__type"])
			}
		})
	}
}

func TestMQ_Audit2_UpdateUser_MaxGroups(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		groups   []string
		wantCode int
	}{
		{
			name:     "update to 20 groups accepted",
			groups:   makeGroups(20),
			wantCode: http.StatusOK,
		},
		{
			name:     "update to 21 groups rejected",
			groups:   makeGroups(21),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAudit2Handler(t)
			bid := createAudit2Broker(t, h, "upd-groups-broker")

			// Create user first.
			rec := doAccuracyMQ(t, h, http.MethodPost,
				fmt.Sprintf("/v1/brokers/%s/users/editme", bid),
				map[string]any{"password": "ValidPass12!!"})
			require.Equal(t, http.StatusOK, rec.Code)

			// Now update with the test groups.
			rec = doAccuracyMQ(t, h, http.MethodPut,
				fmt.Sprintf("/v1/brokers/%s/users/editme", bid),
				map[string]any{"groups": tt.groups})
			assert.Equal(t, tt.wantCode, rec.Code, "groups=%d: %s", len(tt.groups), rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				resp := parseAccuracyMQ(t, rec)
				assert.Equal(t, "BadRequestException", resp["__type"])
			}
		})
	}
}

// ── CreateTags: tag key/value length validation ───────────────────────────────

func TestMQ_Audit2_CreateTags_KeyValueValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid key and value accepted",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusOK,
		},
		{
			name:     "key at 128 chars accepted",
			tags:     map[string]string{strings.Repeat("k", 128): "v"},
			wantCode: http.StatusOK,
		},
		{
			name:     "key over 128 chars rejected",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty key rejected",
			tags:     map[string]string{"": "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value at 256 chars accepted",
			tags:     map[string]string{"k": strings.Repeat("v", 256)},
			wantCode: http.StatusOK,
		},
		{
			name:     "value over 256 chars rejected",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty value accepted",
			tags:     map[string]string{"k": ""},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAudit2Handler(t)
			bid := createAudit2Broker(t, h, "tag-broker")

			// Get the broker ARN.
			descRec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid, nil)
			require.Equal(t, http.StatusOK, descRec.Code)
			brokerARN := parseAccuracyMQ(t, descRec)["brokerArn"].(string)

			rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/tags/"+brokerARN,
				map[string]any{"tags": tt.tags})
			assert.Equal(t, tt.wantCode, rec.Code, "tags=%v: %s", tt.tags, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				resp := parseAccuracyMQ(t, rec)
				assert.Equal(t, "BadRequestException", resp["__type"])
			}
		})
	}
}

// ── CreateTags: max 50 tags per resource ─────────────────────────────────────

func TestMQ_Audit2_CreateTags_MaxTagsPerResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		addTags  map[string]string
		name     string
		wantCode int
		existing int
	}{
		{
			name:     "adding tags when at 49 (to reach 50) succeeds",
			existing: 49,
			addTags:  map[string]string{"tag50": "v"},
			wantCode: http.StatusOK,
		},
		{
			name:     "adding tag to resource already at 50 fails",
			existing: 50,
			addTags:  map[string]string{"tag51": "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "overwriting existing key does not count as new tag",
			existing: 50,
			addTags:  map[string]string{"tag0": "updated"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mq.NewInMemoryBackend("123456789012", "us-east-1")
			h := mq.NewHandler(b)

			bid := createAudit2Broker(t, h, "maxtag-broker")

			descRec := doAccuracyMQ(t, h, http.MethodGet, "/v1/brokers/"+bid, nil)
			require.Equal(t, http.StatusOK, descRec.Code)
			brokerARN := parseAccuracyMQ(t, descRec)["brokerArn"].(string)

			// Seed existing tags directly via backend.
			existing := make(map[string]string, tt.existing)
			for i := range tt.existing {
				existing[fmt.Sprintf("tag%d", i)] = "v"
			}

			if tt.existing > 0 {
				err := b.CreateTags(brokerARN, existing)
				require.NoError(t, err, "seeding %d tags failed", tt.existing)
			}

			rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/tags/"+brokerARN,
				map[string]any{"tags": tt.addTags})
			assert.Equal(t, tt.wantCode, rec.Code, "existing=%d add=%v: %s", tt.existing, tt.addTags, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				resp := parseAccuracyMQ(t, rec)
				assert.Equal(t, "BadRequestException", resp["__type"])
			}
		})
	}
}

// ── CreateBroker initial tags: key/value length validation ───────────────────

func TestMQ_Audit2_CreateBroker_Tags_KeyValueValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags     map[string]string
		name     string
		wantCode int
	}{
		{
			name:     "valid tags accepted at create time",
			tags:     map[string]string{"env": "prod"},
			wantCode: http.StatusAccepted,
		},
		{
			name:     "key over 128 chars rejected at create time",
			tags:     map[string]string{strings.Repeat("k", 129): "v"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "value over 256 chars rejected at create time",
			tags:     map[string]string{"k": strings.Repeat("v", 257)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "empty tag key rejected at create time",
			tags:     map[string]string{"": "v"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAudit2Handler(t)

			rec := doAccuracyMQ(t, h, http.MethodPost, "/v1/brokers", map[string]any{
				"brokerName": "tagged-broker",
				"engineType": mq.EngineTypeActiveMQ,
				"tags":       tt.tags,
			})
			assert.Equal(t, tt.wantCode, rec.Code, "tags=%v: %s", tt.tags, rec.Body.String())

			if tt.wantCode == http.StatusBadRequest {
				resp := parseAccuracyMQ(t, rec)
				assert.Equal(t, "BadRequestException", resp["__type"])
			}
		})
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func makeGroups(n int) []string {
	groups := make([]string, n)
	for i := range groups {
		groups[i] = fmt.Sprintf("group%d", i)
	}

	return groups
}
