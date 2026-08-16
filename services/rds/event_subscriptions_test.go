package rds_test

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestEventSubscription_CRUD(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	sub, err := b.CreateEventSubscription(
		"sub1",
		"arn:aws:sns:us-east-1:123:topic",
		"db-instance",
		nil,
		nil,
	)
	require.NoError(t, err)
	assert.Equal(t, "sub1", sub.SubscriptionName)
	assert.True(t, sub.Enabled)

	subs, err := b.DescribeEventSubscriptions("sub1")
	require.NoError(t, err)
	require.Len(t, subs, 1)

	_, err = b.DeleteEventSubscription("sub1")
	require.NoError(t, err)

	_, err = b.DescribeEventSubscriptions("sub1")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrEventSubscriptionNotFound)
}

func TestEventSubscription_ModifyToggle(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateEventSubscription(
		"sub2",
		"arn:aws:sns:us-east-1:123:topic",
		"db-instance",
		nil,
		nil,
	)
	require.NoError(t, err)

	enabled := false
	_, err = b.ModifyEventSubscription("sub2", "", "db-cluster", nil, nil, &enabled)
	require.NoError(t, err)

	subs, err := b.DescribeEventSubscriptions("sub2")
	require.NoError(t, err)
	assert.False(t, subs[0].Enabled)
}

func TestEventSubscription_AddRemoveSource(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateEventSubscription(
		"sub3",
		"arn:aws:sns:us-east-1:123:topic",
		"db-instance",
		nil,
		nil,
	)
	require.NoError(t, err)

	_, err = b.AddSourceIdentifierToSubscription("sub3", "db-instance-1")
	require.NoError(t, err)

	subs, err := b.DescribeEventSubscriptions("sub3")
	require.NoError(t, err)
	assert.Contains(t, subs[0].SourceIDs, "db-instance-1")

	_, err = b.RemoveSourceIdentifierFromSubscription("sub3", "db-instance-1")
	require.NoError(t, err)

	subs, err = b.DescribeEventSubscriptions("sub3")
	require.NoError(t, err)
	assert.NotContains(t, subs[0].SourceIDs, "db-instance-1")
}

func TestEventSubscription_Duplicate(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()
	_, err := b.CreateEventSubscription("dup-sub", "arn:aws:sns:us-east-1:123:topic", "", nil, nil)
	require.NoError(t, err)

	_, err = b.CreateEventSubscription("dup-sub", "arn:aws:sns:us-east-1:123:topic", "", nil, nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrEventSubscriptionAlreadyExists)
}

func TestEventSubscription_HTTP(t *testing.T) {
	t.Parallel()

	h := newBatch2Handler()

	rec := postRDSForm(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"http-sub"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:rds-events"},
		"Enabled":          {"true"},
		"SourceType":       {"db-instance"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":           {"ModifyEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"http-sub"},
		"Enabled":          {"false"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":           {"DescribeEventSubscriptions"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"http-sub"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h, url.Values{
		"Action":           {"DeleteEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"http-sub"},
	}.Encode())
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestConcurrent_EventSubscription(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	var wg sync.WaitGroup
	for i := range 10 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := fmt.Sprintf("sub-%d", n)
			_, _ = b.CreateEventSubscription(
				name,
				"arn:aws:sns:us-east-1:123:t",
				"db-instance",
				nil,
				nil,
			)
			_, _ = b.DescribeEventSubscriptions(name)
		}(i)
	}
	wg.Wait()

	subs, err := b.DescribeEventSubscriptions("")
	require.NoError(t, err)
	assert.Len(t, subs, 10)
}

func TestRDS_EventSubscriptions(t *testing.T) {
	t.Parallel()

	h := newRDSHandler()

	// CreateEventSubscription
	rec := postRDSForm(t, h, url.Values{
		"Action":           []string{"CreateEventSubscription"},
		"SubscriptionName": []string{"my-sub"},
		"SnsTopicArn":      []string{"arn:aws:sns:us-east-1:123456789012:rds-events"},
		"Enabled":          []string{"true"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DescribeEventSubscriptions
	rec = postRDSForm(t, h, url.Values{
		"Action":           []string{"DescribeEventSubscriptions"},
		"SubscriptionName": []string{"my-sub"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// ModifyEventSubscription
	rec = postRDSForm(t, h, url.Values{
		"Action":           []string{"ModifyEventSubscription"},
		"SubscriptionName": []string{"my-sub"},
		"Enabled":          []string{"false"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DescribeEvents
	rec = postRDSForm(t, h, url.Values{
		"Action": []string{"DescribeEvents"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DescribeEventCategories
	rec = postRDSForm(t, h, url.Values{
		"Action": []string{"DescribeEventCategories"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)

	// DeleteEventSubscription
	rec = postRDSForm(t, h, url.Values{
		"Action":           []string{"DeleteEventSubscription"},
		"SubscriptionName": []string{"my-sub"},
	}.Encode())
	assert.Equal(t, 200, rec.Code)
}

// TestRemoveSourceIdentifierFromSubscription exercises the new operation.
func TestRemoveSourceIdentifierFromSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs        error
		setup            func(b *rds.InMemoryBackend)
		name             string
		subscriptionName string
		sourceIdentifier string
		wantIDCount      int
		wantErr          bool
	}{
		{
			name: "success_removes_identifier",
			setup: func(b *rds.InMemoryBackend) {
				b.AddEventSubscriptionInternal("sub1", "arn:aws:sns:us-east-1:000:my-topic")
				_, _ = b.AddSourceIdentifierToSubscription("sub1", "db-id-1")
				_, _ = b.AddSourceIdentifierToSubscription("sub1", "db-id-2")
			},
			subscriptionName: "sub1",
			sourceIdentifier: "db-id-1",
			wantIDCount:      1,
		},
		{
			name: "noop_when_not_present",
			setup: func(b *rds.InMemoryBackend) {
				b.AddEventSubscriptionInternal("sub2", "arn:aws:sns:us-east-1:000:my-topic")
				_, _ = b.AddSourceIdentifierToSubscription("sub2", "db-id-1")
			},
			subscriptionName: "sub2",
			sourceIdentifier: "not-there",
			wantIDCount:      1,
		},
		{
			name:             "subscription_not_found",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "no-such-sub",
			sourceIdentifier: "db-id-1",
			wantErr:          true,
			wantErrIs:        rds.ErrEventSubscriptionNotFound,
		},
		{
			name:             "empty_subscription_name",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "",
			sourceIdentifier: "db-id-1",
			wantErr:          true,
			wantErrIs:        rds.ErrInvalidParameter,
		},
		{
			name:             "empty_source_identifier",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "sub3",
			sourceIdentifier: "",
			wantErr:          true,
			wantErrIs:        rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			sub, err := b.RemoveSourceIdentifierFromSubscription(tt.subscriptionName, tt.sourceIdentifier)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}

			require.NoError(t, err)
			require.NotNil(t, sub)
			assert.Len(t, sub.SourceIDs, tt.wantIDCount)
		})
	}
}

// TestAddSourceIdentifierToSubscription_DeduplicatesCode verifies idempotent add.
func TestAddSourceIdentifierToSubscription_DeduplicatesCode(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddEventSubscriptionInternal("sub1", "arn:aws:sns:us-east-1:000:my-topic")

	sub1, err := b.AddSourceIdentifierToSubscription("sub1", "db-id-1")
	require.NoError(t, err)
	assert.Len(t, sub1.SourceIDs, 1)

	// Adding same ID again should be idempotent.
	sub2, err := b.AddSourceIdentifierToSubscription("sub1", "db-id-1")
	require.NoError(t, err)
	assert.Len(t, sub2.SourceIDs, 1, "duplicate source identifier should not be added twice")
}

// TestHTTP_RemoveSourceIdentifierFromSubscription tests the HTTP handler.
func TestHTTP_RemoveSourceIdentifierFromSubscription(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddEventSubscriptionInternal("my-sub", "arn:aws:sns:us-east-1:000:my-topic")
	_, _ = b.AddSourceIdentifierToSubscription("my-sub", "db-id-1")
	h := rds.NewHandler(b)

	tests := []struct {
		name           string
		body           string
		wantCode       string
		wantContains   string
		wantStatusCode int
	}{
		{
			name: "success",
			body: "Action=RemoveSourceIdentifierFromSubscription" +
				"&Version=2014-10-31&SubscriptionName=my-sub&SourceIdentifier=db-id-1",
			wantStatusCode: http.StatusOK,
			wantContains:   "RemoveSourceIdentifierFromSubscriptionResponse",
		},
		{
			name: "subscription_not_found",
			body: "Action=RemoveSourceIdentifierFromSubscription" +
				"&Version=2014-10-31&SubscriptionName=no-such&SourceIdentifier=db-id-1",
			wantStatusCode: http.StatusBadRequest,
			wantCode:       "SubscriptionNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postRDSForm(t, h, tt.body)
			assert.Equal(t, tt.wantStatusCode, rec.Code)

			if tt.wantCode != "" {
				assert.Contains(t, rec.Body.String(), tt.wantCode)
			}

			if tt.wantContains != "" {
				assert.Contains(t, rec.Body.String(), tt.wantContains)
			}
		})
	}
}

// TestRDSBackend_AddSourceIdentifierToSubscription tests AddSourceIdentifierToSubscription.
func TestRDSBackend_AddSourceIdentifierToSubscription(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs        error
		setup            func(b *rds.InMemoryBackend)
		name             string
		subscriptionName string
		sourceIdentifier string
		wantSourceIDs    []string
		wantErr          bool
	}{
		{
			name:             "creates_new_subscription",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "my-sub",
			sourceIdentifier: "my-db-instance",
			wantSourceIDs:    []string{"my-db-instance"},
		},
		{
			name: "adds_to_existing",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.AddSourceIdentifierToSubscription("my-sub", "first-db")
			},
			subscriptionName: "my-sub",
			sourceIdentifier: "second-db",
			wantSourceIDs:    []string{"first-db", "second-db"},
		},
		{
			name: "idempotent_duplicate",
			setup: func(b *rds.InMemoryBackend) {
				_, _ = b.AddSourceIdentifierToSubscription("my-sub", "my-db")
			},
			subscriptionName: "my-sub",
			sourceIdentifier: "my-db",
			wantSourceIDs:    []string{"my-db"},
		},
		{
			name:             "empty_subscription_name",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "",
			sourceIdentifier: "my-db",
			wantErr:          true,
			wantErrIs:        rds.ErrInvalidParameter,
		},
		{
			name:             "empty_source_identifier",
			setup:            func(_ *rds.InMemoryBackend) {},
			subscriptionName: "my-sub",
			sourceIdentifier: "",
			wantErr:          true,
			wantErrIs:        rds.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := rds.NewInMemoryBackend("000000000000", "us-east-1")
			tt.setup(b)

			sub, err := b.AddSourceIdentifierToSubscription(tt.subscriptionName, tt.sourceIdentifier)

			if tt.wantErr {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErrIs)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.subscriptionName, sub.SubscriptionName)
			assert.Equal(t, tt.wantSourceIDs, sub.SourceIDs)
		})
	}
}

func TestCreateEventSubscription(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs  error
		name       string
		subName    string
		snsARN     string
		sourceType string
		sourceIDs  []string
		wantErr    bool
	}{
		{
			name:       "success",
			subName:    "my-sub",
			snsARN:     "arn:aws:sns:us-east-1:123456789012:my-topic",
			sourceType: "db-instance",
			sourceIDs:  []string{"db-1"},
		},
		{
			name:      "empty name",
			subName:   "",
			wantErr:   true,
			wantErrIs: rds.ErrInvalidParameter,
		},
		{
			name:      "already exists",
			subName:   "dup-sub",
			snsARN:    "arn:aws:sns:us-east-1:123456789012:topic",
			wantErr:   true,
			wantErrIs: rds.ErrEventSubscriptionAlreadyExists,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if tt.name == "already exists" {
				_, err := b.CreateEventSubscription(tt.subName, tt.snsARN, tt.sourceType, tt.sourceIDs, nil)
				require.NoError(t, err)
			}
			got, err := b.CreateEventSubscription(tt.subName, tt.snsARN, tt.sourceType, tt.sourceIDs, nil)
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.subName, got.SubscriptionName)
			assert.Equal(t, tt.snsARN, got.SnsTopicArn)
			assert.True(t, got.Enabled)
			assert.Equal(t, "active", got.Status)
		})
	}
}

func TestDeleteEventSubscription(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		subName   string
		wantErr   bool
	}{
		{name: "success", subName: "my-sub"},
		{name: "not found", subName: "missing", wantErr: true, wantErrIs: rds.ErrEventSubscriptionNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateEventSubscription(
					tt.subName, "arn:aws:sns:us-east-1:123456789012:topic", "", nil, nil,
				)
				require.NoError(t, err)
			}
			got, err := b.DeleteEventSubscription(tt.subName)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.subName, got.SubscriptionName)
		})
	}
}

func TestDescribeEventSubscriptions(t *testing.T) {
	t.Parallel()
	tests := []struct {
		wantErrIs error
		name      string
		filter    string
		wantCount int
		wantErr   bool
	}{
		{name: "all", filter: "", wantCount: 2},
		{name: "named", filter: "sub-1", wantCount: 1},
		{name: "not found", filter: "missing", wantErr: true, wantErrIs: rds.ErrEventSubscriptionNotFound},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			_, err := b.CreateEventSubscription("sub-1", "arn:aws:sns:us-east-1:123456789012:t", "", nil, nil)
			require.NoError(t, err)
			_, err = b.CreateEventSubscription("sub-2", "arn:aws:sns:us-east-1:123456789012:t", "", nil, nil)
			require.NoError(t, err)
			got, err := b.DescribeEventSubscriptions(tt.filter)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			assert.Len(t, got, tt.wantCount)
		})
	}
}

func TestModifyEventSubscription(t *testing.T) {
	t.Parallel()
	trueBool := true
	tests := []struct {
		wantErrIs   error
		enabled     *bool
		name        string
		subName     string
		newSNS      string
		newSource   string
		wantErr     bool
		wantEnabled bool
	}{
		{
			name:        "success full update",
			subName:     "my-sub",
			newSNS:      "arn:aws:sns:us-east-1:123456789012:new-topic",
			newSource:   "db-cluster",
			enabled:     &trueBool,
			wantEnabled: true,
		},
		{
			name:      "not found",
			subName:   "missing",
			wantErr:   true,
			wantErrIs: rds.ErrEventSubscriptionNotFound,
		},
		{
			name:        "partial update",
			subName:     "my-sub",
			newSNS:      "arn:aws:sns:us-east-1:123456789012:new-topic",
			wantEnabled: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			if !tt.wantErr {
				_, err := b.CreateEventSubscription(tt.subName, "arn:aws:sns:us-east-1:123456789012:old", "", nil, nil)
				require.NoError(t, err)
			}
			got, err := b.ModifyEventSubscription(tt.subName, tt.newSNS, tt.newSource, nil, nil, tt.enabled)
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, tt.wantErrIs)

				return
			}
			require.NoError(t, err)
			if tt.newSNS != "" {
				assert.Equal(t, tt.newSNS, got.SnsTopicArn)
			}
		})
	}
}

func TestDescribeEvents(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		sourceID        string
		sourceType      string
		durationMinutes int
		wantMinCount    int
	}{
		{name: "all", wantMinCount: 0},
		{name: "by sourceID", sourceID: "db-1", wantMinCount: 0},
		{name: "by sourceType", sourceType: "db-instance", wantMinCount: 0},
		{name: "by duration", durationMinutes: 60, wantMinCount: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.DescribeEvents(tt.sourceID, tt.sourceType, tt.durationMinutes)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, len(got), tt.wantMinCount)
		})
	}
}

func TestDescribeEventCategories(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		sourceType string
		wantLen    int
	}{
		{name: "all", sourceType: "", wantLen: 11},
		{name: "filtered", sourceType: "db-instance", wantLen: 11},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)
			got, err := b.DescribeEventCategories(tt.sourceType)
			require.NoError(t, err)
			assert.Len(t, got, tt.wantLen)
		})
	}
}

// TestDescribeEventsSorted verifies events are returned sorted by creation time.
func TestDescribeEventsSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	// Create instances to generate events (each CreateDBInstance publishes an event).
	for _, id := range []string{"db-ev1", "db-ev2", "db-ev3"} {
		_, err := b.CreateDBInstance(id, "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
		require.NoError(t, err)
	}
	got, err := b.DescribeEvents("", "", 0)
	require.NoError(t, err)
	for i := 1; i < len(got); i++ {
		assert.False(t, got[i].CreatedAt.Before(got[i-1].CreatedAt),
			"events should be sorted ascending by creation time")
	}
}

// TestEventSubscriptionEventCategories verifies EventCategories field in CreateEventSubscription.
func TestEventSubscriptionEventCategories(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	cats := []string{"backup", "availability"}
	sub, err := b.CreateEventSubscription(
		"cat-sub", "arn:aws:sns:us-east-1:123456789012:topic", "db-instance", nil, cats,
	)
	require.NoError(t, err)
	assert.Equal(t, cats, sub.EventCategories)
	// Modify to update categories.
	newCats := []string{"failover"}
	sub, err = b.ModifyEventSubscription("cat-sub", "", "", nil, newCats, nil)
	require.NoError(t, err)
	assert.Equal(t, newCats, sub.EventCategories)
}
