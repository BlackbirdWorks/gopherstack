package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteEventSubscription(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEventSubscriptionInternal("del-es", "arn:aws:sns:us-east-1:123:topic")

	rec := doDMS(t, h, "DeleteEventSubscription", map[string]any{
		"SubscriptionName": "del-es",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 0, h.Backend.EventSubscriptionCount())

	rec2 := doDMS(t, h, "DeleteEventSubscription", map[string]any{
		"SubscriptionName": "del-es",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestModifyEventSubscription(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEventSubscriptionInternal("mod-es", "arn:aws:sns:us-east-1:123:topic")

	enabled := true
	rec := doDMS(t, h, "ModifyEventSubscription", map[string]any{
		"SubscriptionName": "mod-es",
		"Enabled":          enabled,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyEventSubscription", map[string]any{
		"SubscriptionName": "nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestCreateEventSubscription_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	body := map[string]any{
		"SubscriptionName": "dup-sub",
		"SnsTopicArn":      "arn:aws:sns:us-east-1:123:topic",
	}

	rec1 := doDMS(t, h, "CreateEventSubscription", body)
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doDMS(t, h, "CreateEventSubscription", body)
	require.Equal(t, http.StatusConflict, rec2.Code)

	var errBody map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errBody))
	assert.Equal(t, "ResourceAlreadyExistsFault", errBody["__type"])
}

func TestDescribeEvents(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	// Create a replication instance to generate a creation event.
	rec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "test-ri",
		"ReplicationInstanceClass":      "dms.t3.micro",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create an endpoint to generate another event.
	rec = doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "src",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeEvents must now return at least these two events.
	rec = doDMS(t, h, "DescribeEvents", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseJSON(t, rec)
	events, ok := body["Events"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(events), 1, "at least endpoint creation event expected")

	e0, ok := events[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, e0["Message"])
	assert.NotEmpty(t, e0["Date"])
}

func TestHandler_CreateEventSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateEventSubscription", map[string]any{
					"SubscriptionName": "my-sub",
					"SnsTopicArn":      "arn:aws:sns:us-east-1:123:my-topic",
					"SourceType":       "replication-instance",
					"EventCategories":  []string{"creation", "deletion"},
					"Enabled":          true,
					"Tags": []map[string]string{
						{"Key": "Env", "Value": "prod"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				es, ok := resp["EventSubscription"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-sub", es["SubscriptionName"])
				assert.Equal(t, "arn:aws:sns:us-east-1:123:my-topic", es["SnsTopicArn"])
				assert.Equal(t, "active", es["Status"])
				assert.Equal(t, true, es["Enabled"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				doDMS(t, h, "CreateEventSubscription", map[string]any{
					"SubscriptionName": "dup-sub",
					"SnsTopicArn":      "arn:aws:sns:us-east-1:123:topic",
				})
				rec := doDMS(t, h, "CreateEventSubscription", map[string]any{
					"SubscriptionName": "dup-sub",
					"SnsTopicArn":      "arn:aws:sns:us-east-1:123:topic",
				})
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "missing_subscription_name",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateEventSubscription", map[string]any{
					"SnsTopicArn": "arn:aws:sns:us-east-1:123:topic",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "missing_sns_topic_arn",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "CreateEventSubscription", map[string]any{
					"SubscriptionName": "no-topic",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

func TestNonNilEventSubscriptionSlices(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "CreateEventSubscription", map[string]any{
		"SubscriptionName": "sub1",
		"SnsTopicArn":      "arn:aws:sns:us-east-1:123:topic1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseJSON(t, rec)
	es := body["EventSubscription"].(map[string]any)
	require.NotNil(t, es["SourceIdsList"])
	require.NotNil(t, es["EventCategories"])
}

func TestEventSubscriptionSeedHelper(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddEventSubscriptionInternal("seed-sub", "arn:aws:sns:us-east-1:123:topic")
	assert.Equal(t, 1, h.Backend.EventSubscriptionCount())
}

func TestHandler_DescribeEventCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "returns_all_categories_no_filter",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeEventCategories", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				groups, ok := resp["EventCategoryGroupList"].([]any)
				require.True(t, ok)
				assert.NotEmpty(t, groups)
			},
		},
		{
			name: "filter_by_replication_instance_source_type",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeEventCategories", map[string]any{
					"SourceType": "replication-instance",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				groups := resp["EventCategoryGroupList"].([]any)
				require.Len(t, groups, 1)
				g := groups[0].(map[string]any)
				assert.Equal(t, "replication-instance", g["SourceType"])
				cats, ok := g["EventCategories"].([]any)
				require.True(t, ok)
				assert.NotEmpty(t, cats)
			},
		},
		{
			name: "filter_by_replication_task_source_type",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeEventCategories", map[string]any{
					"SourceType": "replication-task",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				groups := resp["EventCategoryGroupList"].([]any)
				require.Len(t, groups, 1)
				g := groups[0].(map[string]any)
				assert.Equal(t, "replication-task", g["SourceType"])
			},
		},
		{
			name: "unknown_source_type_returns_empty",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DescribeEventCategories", map[string]any{
					"SourceType": "unknown-type",
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				groups := resp["EventCategoryGroupList"].([]any)
				assert.Empty(t, groups)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}
