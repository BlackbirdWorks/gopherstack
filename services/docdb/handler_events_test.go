package docdb_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestHandler_EventSubscriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_event_subscription",
			vals: url.Values{
				"Action":           {"CreateEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
				"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:my-topic"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-sub",
		},
		{
			name: "add_source_identifier",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"my-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:my-topic"},
				})
			},
			vals: url.Values{
				"Action":           {"AddSourceIdentifierToSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
				"SourceIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-cluster",
		},
		{
			name: "delete_event_subscription",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"my-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:my-topic"},
				})
			},
			vals: url.Values{
				"Action":           {"DeleteEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteEventSubscriptionResponse",
		},
		{
			name: "create_duplicate_subscription",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"dup-sub"},
				})
			},
			vals: url.Values{
				"Action":           {"CreateEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"dup-sub"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionAlreadyExist",
		},
		{
			name: "delete_nonexistent_subscription",
			vals: url.Values{
				"Action":           {"DeleteEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionNotFound",
		},
		{
			name: "add_source_id_nonexistent_subscription",
			vals: url.Values{
				"Action":           {"AddSourceIdentifierToSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"nonexistent"},
				"SourceIdentifier": {"some-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeEventSubscriptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_all_subscriptions",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"my-sub"},
				})
			},
			vals: url.Values{
				"Action":  {"DescribeEventSubscriptions"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-sub",
		},
		{
			name: "describe_subscription_by_name",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"my-sub"},
				})
			},
			vals: url.Values{
				"Action":           {"DescribeEventSubscriptions"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-sub",
		},
		{
			name: "describe_subscriptions_empty",
			vals: url.Values{
				"Action":  {"DescribeEventSubscriptions"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeEventSubscriptionsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_ModifyEventSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "modify_subscription",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"my-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:old-topic"},
				})
			},
			vals: url.Values{
				"Action":           {"ModifyEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
				"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:new-topic"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "new-topic",
		},
		{
			name: "modify_subscription_not_found",
			vals: url.Values{
				"Action":           {"ModifyEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"nonexistent"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_RemoveSourceIdentifierFromSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "remove_source_identifier",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":               {"CreateEventSubscription"},
					"Version":              {"2014-10-31"},
					"SubscriptionName":     {"my-sub"},
					"SourceIds.SourceId.1": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":           {"RemoveSourceIdentifierFromSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"my-sub"},
				"SourceIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "RemoveSourceIdentifierFromSubscriptionResponse",
		},
		{
			name: "remove_source_identifier_not_found",
			vals: url.Values{
				"Action":           {"RemoveSourceIdentifierFromSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"nonexistent"},
				"SourceIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "SubscriptionNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_events",
			vals: url.Values{
				"Action":  {"DescribeEvents"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeEventsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeEventCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "describe_event_categories",
			vals: url.Values{
				"Action":  {"DescribeEventCategories"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeEventCategoriesResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestDescribeEventCategoriesFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals            url.Values
		name            string
		wantContains    string
		wantNotContains string
		wantStatus      int
	}{
		{
			name: "no_source_type_filter",
			vals: url.Values{
				"Action":  {"DescribeEventCategories"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "db-cluster",
		},
		{
			name: "filter_by_db_instance",
			vals: url.Values{
				"Action":     {"DescribeEventCategories"},
				"Version":    {"2014-10-31"},
				"SourceType": {"db-instance"},
			},
			wantStatus:      http.StatusOK,
			wantContains:    "db-instance",
			wantNotContains: "db-cluster-snapshot",
		},
		{
			name: "filter_by_snapshot",
			vals: url.Values{
				"Action":     {"DescribeEventCategories"},
				"Version":    {"2014-10-31"},
				"SourceType": {"db-cluster-snapshot"},
			},
			wantStatus:      http.StatusOK,
			wantContains:    "db-cluster-snapshot",
			wantNotContains: "db-instance",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
			if tt.wantNotContains != "" {
				assert.NotContains(t, rr.Body.String(), tt.wantNotContains)
			}
		})
	}
}

func TestEventSubscriptionSourceType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_subscription_with_source_type",
			vals: url.Values{
				"Action":                          {"CreateEventSubscription"},
				"Version":                         {"2014-10-31"},
				"SubscriptionName":                {"my-sub"},
				"SourceType":                      {"db-cluster"},
				"EventCategories.EventCategory.1": {"backup"},
				"EventCategories.EventCategory.2": {"failover"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "db-cluster",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestEventSubscription_FullLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "modify_subscription_topic",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"mod-sub"},
					"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:old-topic"},
				})
			},
			vals: url.Values{
				"Action":           {"ModifyEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"mod-sub"},
				"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:new-topic"},
			},
			wantStatus:   200,
			wantContains: "new-topic",
		},
		{
			name: "remove_source_identifier",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":               {"CreateEventSubscription"},
					"Version":              {"2014-10-31"},
					"SubscriptionName":     {"src-id-sub"},
					"SourceIds.SourceId.1": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":           {"RemoveSourceIdentifierFromSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"src-id-sub"},
				"SourceIdentifier": {"my-cluster"},
			},
			wantStatus:   200,
			wantContains: "RemoveSourceIdentifierFromSubscriptionResponse",
		},
		{
			name: "describe_event_subscriptions",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":           {"CreateEventSubscription"},
					"Version":          {"2014-10-31"},
					"SubscriptionName": {"desc-sub"},
				})
			},
			vals: url.Values{
				"Action":           {"DescribeEventSubscriptions"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"desc-sub"},
			},
			wantStatus:   200,
			wantContains: "desc-sub",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestDescribeEventCategories(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sourceType   string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "all_categories",
			sourceType:   "",
			wantContains: "db-cluster",
			wantStatus:   200,
		},
		{
			name:         "db_cluster_categories",
			sourceType:   "db-cluster",
			wantContains: "failover",
			wantStatus:   200,
		},
		{
			name:         "db_instance_categories",
			sourceType:   "db-instance",
			wantContains: "recovery",
			wantStatus:   200,
		},
		{
			name:         "snapshot_categories",
			sourceType:   "db-cluster-snapshot",
			wantContains: "restoration",
			wantStatus:   200,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			vals := url.Values{
				"Action":  {"DescribeEventCategories"},
				"Version": {"2014-10-31"},
			}
			if tt.sourceType != "" {
				vals.Set("SourceType", tt.sourceType)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}
