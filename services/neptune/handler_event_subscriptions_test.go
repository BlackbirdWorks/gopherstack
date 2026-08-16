package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func TestHandler_CreateEventSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*neptune.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_subscription_success",
			vals: url.Values{
				"Action":           {"CreateEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"test-sub"},
				"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:neptune-events"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-sub",
		},
		{
			name: "create_subscription_duplicate",
			setup: func(h *neptune.Handler) {
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"test-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:neptune-events"},
				})
			},
			vals: url.Values{
				"Action":           {"CreateEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"test-sub"},
				"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:neptune-events"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionAlreadyExist",
		},
		{
			name: "create_subscription_missing_topic",
			vals: url.Values{
				"Action":           {"CreateEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"test-sub2"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_AddSourceIdentifierToSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*neptune.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "add_source_id_success",
			setup: func(h *neptune.Handler) {
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"src-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:events"},
				})
			},
			vals: url.Values{
				"Action":           {"AddSourceIdentifierToSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"src-sub"},
				"SourceIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-cluster",
		},
		{
			name: "add_source_id_not_found",
			vals: url.Values{
				"Action":           {"AddSourceIdentifierToSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"no-such-sub"},
				"SourceIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionNotFound",
		},
		{
			name: "add_source_id_missing_source",
			setup: func(h *neptune.Handler) {
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"src-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:events"},
				})
			},
			vals: url.Values{
				"Action":           {"AddSourceIdentifierToSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"src-sub"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- EventSubscription comprehensive coverage ----

func TestEventSubscription_AllSourceIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateEventSubscription"},
		"Version":              {"2014-10-31"},
		"SubscriptionName":     {"sub-sources"},
		"SnsTopicArn":          {"arn:aws:sns:us-east-1:000000000000:test-topic"},
		"SourceIds.SourceId.1": {"cluster-a"},
		"SourceIds.SourceId.2": {"cluster-b"},
		"SourceIds.SourceId.3": {"cluster-c"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "cluster-a")
	assert.Contains(t, body, "cluster-b")
	assert.Contains(t, body, "cluster-c")
	assert.Contains(t, body, "arn:aws:sns")
}

func TestEventSubscription_AddRemoveSourceID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-add-remove"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
	})

	doRequest(t, h, url.Values{
		"Action":           {"AddSourceIdentifierToSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-add-remove"},
		"SourceIdentifier": {"cluster-new"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":           {"DescribeEventSubscriptions"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-add-remove"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "cluster-new")

	doRequest(t, h, url.Values{
		"Action":           {"RemoveSourceIdentifierFromSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-add-remove"},
		"SourceIdentifier": {"cluster-new"},
	})

	rr = doRequest(t, h, url.Values{
		"Action":           {"DescribeEventSubscriptions"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-add-remove"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.NotContains(t, rr.Body.String(), "cluster-new")
}

func TestEventSubscription_ModifySNSTopic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-modify-sns"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:old-topic"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":           {"ModifyEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-modify-sns"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:new-topic"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "new-topic")
}

func TestEventSubscription_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-dup"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
	})
	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-dup"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "SubscriptionAlreadyExist")
}

func TestEventSubscription_DescribeAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, name := range []string{"sub-a", "sub-b", "sub-c"} {
		doRequest(t, h, url.Values{
			"Action":           {"CreateEventSubscription"},
			"Version":          {"2014-10-31"},
			"SubscriptionName": {name},
			"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
		})
	}

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEventSubscriptions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "sub-a")
	assert.Contains(t, body, "sub-b")
	assert.Contains(t, body, "sub-c")
}

func TestEventSubscription_Status(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-status"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "active")
}

// ---- DescribeEventCategories ----

func TestDescribeEventCategories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEventCategories"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "db-cluster")
	assert.Contains(t, body, "db-instance")
	assert.Contains(t, body, "failover")
	assert.Contains(t, body, "maintenance")
}

// TestCreateEventSubscription_MissingSNS verifies error on missing SnsTopicArn.
func TestCreateEventSubscription_MissingSNS(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-no-sns"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

// TestAddSourceIdentifier_SubscriptionNotFound verifies proper error.
func TestAddSourceIdentifier_SubscriptionNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":           {"AddSourceIdentifierToSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"nonexistent-sub"},
		"SourceIdentifier": {"some-id"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "SubscriptionNotFound")
}

// TestSubscriptionAlreadyExists verifies duplicate subscription error.
func TestSubscriptionAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"dup-sub"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:test"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"dup-sub"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:test"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "SubscriptionAlreadyExist")
}

// TestDescribeEventCategories_ReturnsCategories verifies non-empty event categories.
func TestDescribeEventCategories_ReturnsCategories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEventCategories"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "db-cluster")
	assert.Contains(t, body, "db-instance")
	assert.Contains(t, body, "failover")
	assert.Contains(t, body, "maintenance")
}

// --- Event subscription lifecycle ---

func TestEventSubscription_DescribeModifyDeleteLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// create subscription
	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-1"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:my-topic"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-1")

	// describe all
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeEventSubscriptions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-1")
	assert.Contains(t, rr.Body.String(), "my-topic")

	// describe by name
	rr = doRequest(t, h, url.Values{
		"Action":           {"DescribeEventSubscriptions"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-1"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-1")

	// modify
	rr = doRequest(t, h, url.Values{
		"Action":           {"ModifyEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-1"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:new-topic"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-1")

	// delete
	rr = doRequest(t, h, url.Values{
		"Action":           {"DeleteEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-1"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-1")

	// second delete must fail
	rr = doRequest(t, h, url.Values{
		"Action":           {"DeleteEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-1"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "SubscriptionNotFound")
}

func TestEventSubscription_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains string
	}{
		{
			name: "describe_not_found",
			vals: url.Values{
				"Action":           {"DescribeEventSubscriptions"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"no-such"},
			},
			wantContains: "SubscriptionNotFound",
		},
		{
			name: "modify_not_found",
			vals: url.Values{
				"Action":           {"ModifyEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"no-such"},
				"SnsTopicArn":      {"arn:x"},
			},
			wantContains: "SubscriptionNotFound",
		},
		{
			name: "delete_not_found",
			vals: url.Values{
				"Action":           {"DeleteEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"no-such"},
			},
			wantContains: "SubscriptionNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestEventSubscription_RemoveSourceIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-src"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
	})
	doRequest(t, h, url.Values{
		"Action":           {"AddSourceIdentifierToSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-src"},
		"SourceIdentifier": {"my-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":           {"RemoveSourceIdentifierFromSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-src"},
		"SourceIdentifier": {"my-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-src")
}

func TestDescribeEvents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEvents"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "DescribeEventsResponse")
}

// --- Event Subscriptions ---

// TestCreateDescribeDeleteEventSubscription tests full event subscription lifecycle.
func TestCreateDescribeDeleteEventSubscription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-01"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:neptune-events"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "sub-01")

	// Describe
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeEventSubscriptions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "sub-01")

	// Add source identifier
	rr = doRequest(t, h, url.Values{
		"Action":           {"AddSourceIdentifierToSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-01"},
		"SourceIdentifier": {"my-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Remove source identifier
	rr = doRequest(t, h, url.Values{
		"Action":           {"RemoveSourceIdentifierFromSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-01"},
		"SourceIdentifier": {"my-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Modify subscription
	rr = doRequest(t, h, url.Values{
		"Action":           {"ModifyEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-01"},
		"Enabled":          {"true"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Delete
	rr = doRequest(t, h, url.Values{
		"Action":           {"DeleteEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}
