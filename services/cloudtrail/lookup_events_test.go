package cloudtrail_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

// TestLookupEvents_RecordAndFilter verifies that recorded events are returned by
// LookupEvents, honoring lookup attributes, time ranges, ordering, and pagination.
func TestLookupEvents_RecordAndFilter(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)

	newBackendWithEvents := func() *cloudtrail.InMemoryBackend {
		b := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
		b.RecordEvent(cloudtrail.Event{
			EventID:     "evt-1",
			EventName:   "RunInstances",
			EventSource: "ec2.amazonaws.com",
			Username:    "alice",
			ReadOnly:    "false",
			AccessKeyID: "AKIA1",
			EventTime:   base,
			Resources:   []cloudtrail.EventResource{{ResourceName: "i-123", ResourceType: "AWS::EC2::Instance"}},
		})
		b.RecordEvent(cloudtrail.Event{
			EventID:     "evt-2",
			EventName:   "CreateBucket",
			EventSource: "s3.amazonaws.com",
			Username:    "bob",
			ReadOnly:    "false",
			AccessKeyID: "AKIA2",
			EventTime:   base.Add(time.Hour),
		})
		b.RecordEvent(cloudtrail.Event{
			EventID:     "evt-3",
			EventName:   "DescribeInstances",
			EventSource: "ec2.amazonaws.com",
			Username:    "alice",
			ReadOnly:    "true",
			EventTime:   base.Add(2 * time.Hour),
		})

		return b
	}

	tests := []struct {
		name     string
		wantIDs  []string
		input    cloudtrail.LookupEventsInput
		wantNext bool
	}{
		{
			name:    "no_filters_newest_first",
			input:   cloudtrail.LookupEventsInput{},
			wantIDs: []string{"evt-3", "evt-2", "evt-1"},
		},
		{
			name: "filter_event_name",
			input: cloudtrail.LookupEventsInput{
				LookupAttributes: []cloudtrail.LookupAttribute{
					{AttributeKey: "EventName", AttributeValue: "CreateBucket"},
				},
			},
			wantIDs: []string{"evt-2"},
		},
		{
			name: "filter_event_source",
			input: cloudtrail.LookupEventsInput{
				LookupAttributes: []cloudtrail.LookupAttribute{
					{AttributeKey: "EventSource", AttributeValue: "ec2.amazonaws.com"},
				},
			},
			wantIDs: []string{"evt-3", "evt-1"},
		},
		{
			name: "filter_username_and_readonly_anded",
			input: cloudtrail.LookupEventsInput{
				LookupAttributes: []cloudtrail.LookupAttribute{
					{AttributeKey: "Username", AttributeValue: "alice"},
					{AttributeKey: "ReadOnly", AttributeValue: "true"},
				},
			},
			wantIDs: []string{"evt-3"},
		},
		{
			name: "filter_access_key",
			input: cloudtrail.LookupEventsInput{
				LookupAttributes: []cloudtrail.LookupAttribute{
					{AttributeKey: "AccessKeyId", AttributeValue: "AKIA1"},
				},
			},
			wantIDs: []string{"evt-1"},
		},
		{
			name: "filter_resource_name",
			input: cloudtrail.LookupEventsInput{
				LookupAttributes: []cloudtrail.LookupAttribute{
					{AttributeKey: "ResourceName", AttributeValue: "i-123"},
				},
			},
			wantIDs: []string{"evt-1"},
		},
		{
			name: "time_range_excludes_outside",
			input: cloudtrail.LookupEventsInput{
				StartTime: new(base.Add(30 * time.Minute)),
				EndTime:   new(base.Add(90 * time.Minute)),
			},
			wantIDs: []string{"evt-2"},
		},
		{
			name: "max_results_paginates",
			input: cloudtrail.LookupEventsInput{
				MaxResults: 2,
			},
			wantIDs:  []string{"evt-3", "evt-2"},
			wantNext: true,
		},
		{
			name: "no_match_returns_empty",
			input: cloudtrail.LookupEventsInput{
				LookupAttributes: []cloudtrail.LookupAttribute{
					{AttributeKey: "EventName", AttributeValue: "Nonexistent"},
				},
			},
			wantIDs: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackendWithEvents()
			out := b.LookupEvents(tt.input)

			gotIDs := make([]string, 0, len(out.Events))
			for _, e := range out.Events {
				gotIDs = append(gotIDs, e.EventID)
			}

			assert.Equal(t, tt.wantIDs, gotIDs)

			if tt.wantNext {
				assert.NotEmpty(t, out.NextToken)
			} else {
				assert.Empty(t, out.NextToken)
			}
		})
	}
}

// TestLookupEvents_NextTokenPagination verifies that the NextToken from one page
// returns the remaining events on the subsequent page.
func TestLookupEvents_NextTokenPagination(t *testing.T) {
	t.Parallel()

	b := cloudtrail.NewInMemoryBackend("123456789012", config.DefaultRegion)
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)

	for i := range 5 {
		b.RecordEvent(cloudtrail.Event{
			EventName: "Op",
			EventTime: base.Add(time.Duration(i) * time.Minute),
		})
	}

	page1 := b.LookupEvents(cloudtrail.LookupEventsInput{MaxResults: 2})
	require.Len(t, page1.Events, 2)
	require.NotEmpty(t, page1.NextToken)

	page2 := b.LookupEvents(cloudtrail.LookupEventsInput{MaxResults: 2, NextToken: page1.NextToken})
	require.Len(t, page2.Events, 2)
	require.NotEmpty(t, page2.NextToken)

	page3 := b.LookupEvents(cloudtrail.LookupEventsInput{MaxResults: 2, NextToken: page2.NextToken})
	require.Len(t, page3.Events, 1)
	require.Empty(t, page3.NextToken)
}
