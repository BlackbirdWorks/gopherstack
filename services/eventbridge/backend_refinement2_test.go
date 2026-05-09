package eventbridge_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func newBackend() *eventbridge.InMemoryBackend {
	return eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
}

// ---------------------------------------------------------------------------
// CreateEventBus validations
// ---------------------------------------------------------------------------

func TestRefinement2_CreateEventBus_RejectsAWSPrefix(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		busName string
	}{
		{
			name:    "aws.partner prefix rejected",
			busName: "aws.partner.saas.us-east-1.12345678",
			wantErr: eventbridge.ErrInvalidParameter,
		},
		{
			name:    "aws.events prefix rejected",
			busName: "aws.events",
			wantErr: eventbridge.ErrInvalidParameter,
		},
		{
			name:    "custom bus allowed",
			busName: "my-bus",
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateEventBus(tt.busName, "")
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement2_CreateEventBus_RejectsLongName(t *testing.T) {
	t.Parallel()
	b := newBackend()
	longName := make([]byte, 257)
	for i := range longName {
		longName[i] = 'x'
	}
	_, err := b.CreateEventBus(string(longName), "")
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

// ---------------------------------------------------------------------------
// PutRule mutual exclusion validation
// ---------------------------------------------------------------------------

func TestRefinement2_PutRule_MutualExclusion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		input   eventbridge.PutRuleInput
		name    string
	}{
		{
			name: "both set rejects",
			input: eventbridge.PutRuleInput{
				Name:               "r",
				ScheduleExpression: "rate(1 minute)",
				EventPattern:       `{"source":["x"]}`,
			},
			wantErr: eventbridge.ErrInvalidParameter,
		},
		{
			name:    "neither set rejects",
			input:   eventbridge.PutRuleInput{Name: "r"},
			wantErr: eventbridge.ErrInvalidParameter,
		},
		{
			name:    "only schedule allowed",
			input:   eventbridge.PutRuleInput{Name: "r", ScheduleExpression: "rate(5 minutes)"},
			wantErr: nil,
		},
		{
			name:    "only pattern allowed",
			input:   eventbridge.PutRuleInput{Name: "r", EventPattern: `{"source":["test"]}`},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.PutRule(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CreateAPIDestination HTTPMethod validation
// ---------------------------------------------------------------------------

func TestRefinement2_CreateAPIDestination_HTTPMethodValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		method  string
	}{
		{name: "GET allowed", method: "GET"},
		{name: "POST allowed", method: "POST"},
		{name: "PUT allowed", method: "PUT"},
		{name: "DELETE allowed", method: "DELETE"},
		{name: "PATCH allowed", method: "PATCH"},
		{name: "HEAD allowed", method: "HEAD"},
		{name: "OPTIONS allowed", method: "OPTIONS"},
		{name: "INVALID rejected", method: "CONNECT", wantErr: eventbridge.ErrInvalidParameter},
		{name: "empty rejected", method: "", wantErr: eventbridge.ErrInvalidParameter},
		{name: "TRACE rejected", method: "TRACE", wantErr: eventbridge.ErrInvalidParameter},
	}

	connARN := "arn:aws:events:us-east-1:123:connection/test"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateAPIDestination(eventbridge.CreateAPIDestinationInput{
				Name:               "dst-" + tt.name,
				ConnectionArn:      connARN,
				InvocationEndpoint: "https://example.com/hook",
				HTTPMethod:         tt.method,
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CreateConnection AuthorizationType validation
// ---------------------------------------------------------------------------

func TestRefinement2_CreateConnection_AuthTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		authType string
	}{
		{name: "API_KEY allowed", authType: "API_KEY"},
		{name: "BASIC allowed", authType: "BASIC"},
		{name: "OAUTH_CLIENT_CREDENTIALS allowed", authType: "OAUTH_CLIENT_CREDENTIALS"},
		{name: "INVALID rejected", authType: "INVALID", wantErr: eventbridge.ErrInvalidParameter},
		{name: "empty rejected", authType: "", wantErr: eventbridge.ErrInvalidParameter},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateConnection(eventbridge.CreateConnectionInput{
				Name:              "conn-" + tt.name,
				AuthorizationType: tt.authType,
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// CreateArchive validations
// ---------------------------------------------------------------------------

func TestRefinement2_CreateArchive_NameLengthValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr     error
		name        string
		archiveName string
	}{
		{name: "normal name ok", archiveName: "my-archive"},
		{name: "exactly 48 chars ok", archiveName: "12345678901234567890123456789012345678901234567a"},
		{
			name:        "49 chars rejected",
			archiveName: "1234567890123456789012345678901234567890123456789",
			wantErr:     eventbridge.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateArchive(eventbridge.CreateArchiveInput{
				ArchiveName:    tt.archiveName,
				EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestRefinement2_CreateArchive_NegativeRetentionRejects(t *testing.T) {
	t.Parallel()
	b := newBackend()
	_, err := b.CreateArchive(eventbridge.CreateArchiveInput{
		ArchiveName:    "bad-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
		RetentionDays:  -1,
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

// ---------------------------------------------------------------------------
// PutTargets limit enforcement
// ---------------------------------------------------------------------------

func TestRefinement2_PutTargets_EnforcesLimit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		targetCount int
		wantErr     bool
	}{
		{name: "5 targets allowed", targetCount: 5, wantErr: false},
		{name: "6 targets rejected", targetCount: 6, wantErr: true},
		{name: "0 targets rejected", targetCount: 0, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.PutRule(eventbridge.PutRuleInput{Name: "r", ScheduleExpression: "rate(1 minute)"})
			require.NoError(t, err)

			targets := make([]eventbridge.Target, tt.targetCount)
			for i := range targets {
				targets[i] = eventbridge.Target{
					ID:  "t" + string(rune('0'+i)),
					Arn: "arn:aws:sqs:us-east-1:123:q",
				}
			}

			_, err = b.PutTargets("r", "", targets)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// StartReplay time ordering validation
// ---------------------------------------------------------------------------

func TestRefinement2_StartReplay_TimeOrdering(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	earlier := now.Add(-2 * time.Hour)
	later := now.Add(2 * time.Hour)

	tests := []struct {
		start   time.Time
		end     time.Time
		wantErr error
		name    string
	}{
		{name: "start before end ok", start: earlier, end: now, wantErr: nil},
		{name: "start equals end rejected", start: now, end: now, wantErr: eventbridge.ErrInvalidParameter},
		{name: "start after end rejected", start: later, end: earlier, wantErr: eventbridge.ErrInvalidParameter},
		{name: "both zero ok", start: time.Time{}, end: time.Time{}, wantErr: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.StartReplay(eventbridge.StartReplayInput{
				ReplayName:     "r-" + tt.name,
				EventSourceArn: "arn:aws:events:us-east-1:123:archive/my-archive",
				EventStartTime: tt.start,
				EventEndTime:   tt.end,
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestEventPattern event JSON validation
// ---------------------------------------------------------------------------

func TestRefinement2_TestEventPattern_EventJSONValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		pattern string
		event   string
		wantOk  bool
	}{
		{
			name:    "valid pattern and event matches",
			pattern: `{"source":["my.app"]}`,
			event:   `{"source":"my.app","detail-type":"Login","detail":{}}`,
			wantOk:  true,
		},
		{
			name:    "valid pattern non-matching event",
			pattern: `{"source":["other.app"]}`,
			event:   `{"source":"my.app","detail-type":"Login","detail":{}}`,
			wantOk:  false,
		},
		{
			name:    "invalid event JSON rejects",
			pattern: `{"source":["test"]}`,
			event:   `not-json`,
			wantErr: eventbridge.ErrInvalidParameter,
		},
		{
			name:    "invalid pattern JSON rejects",
			pattern: `not-json`,
			event:   `{"source":"my.app"}`,
			wantErr: eventbridge.ErrInvalidParameter,
		},
		{
			name:    "empty pattern rejects",
			pattern: "",
			event:   `{"source":"my.app"}`,
			wantErr: eventbridge.ErrInvalidParameter,
		},
		{
			name:    "empty event rejects",
			pattern: `{"source":["my.app"]}`,
			event:   "",
			wantErr: eventbridge.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			ok, err := b.TestEventPattern(tt.pattern, tt.event)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantOk, ok)
		})
	}
}

// ---------------------------------------------------------------------------
// UpdateArchive
// ---------------------------------------------------------------------------

func TestRefinement2_UpdateArchive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(*eventbridge.InMemoryBackend)
		name     string
		wantDesc string
		input    eventbridge.UpdateArchiveInput
	}{
		{
			name:    "not found returns error",
			input:   eventbridge.UpdateArchiveInput{ArchiveName: "no-such"},
			wantErr: eventbridge.ErrNotFound,
		},
		{
			name: "updates description",
			setup: func(b *eventbridge.InMemoryBackend) {
				b.AddArchiveInternal(&eventbridge.Archive{
					ArchiveName:    "a1",
					EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
					State:          "ENABLED",
				})
			},
			input:    eventbridge.UpdateArchiveInput{ArchiveName: "a1", Description: "new desc"},
			wantDesc: "new desc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			got, err := b.UpdateArchive(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, got.Description)
		})
	}
}

// ---------------------------------------------------------------------------
// DeleteArchive / DescribeArchive / ListArchives
// ---------------------------------------------------------------------------

func TestRefinement2_ArchiveCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeArchive("missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("delete not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		require.ErrorIs(t, b.DeleteArchive("missing"), eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.CreateArchive(eventbridge.CreateArchiveInput{
			ArchiveName:    "rt-archive",
			EventSourceArn: "arn:aws:events:us-east-1:123:event-bus/default",
		})
		require.NoError(t, err)

		got, err := b.DescribeArchive("rt-archive")
		require.NoError(t, err)
		assert.Equal(t, "rt-archive", got.ArchiveName)

		archives, _, err := b.ListArchives("rt-", "")
		require.NoError(t, err)
		assert.Len(t, archives, 1)

		require.NoError(t, b.DeleteArchive("rt-archive"))
		_, err = b.DescribeArchive("rt-archive")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})
}

// ---------------------------------------------------------------------------
// DeleteConnection / DescribeConnection / ListConnections / UpdateConnection
// ---------------------------------------------------------------------------

func TestRefinement2_ConnectionCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeConnection("missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("delete not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		require.ErrorIs(t, b.DeleteConnection("missing"), eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.CreateConnection(eventbridge.CreateConnectionInput{
			Name:              "my-conn",
			AuthorizationType: "API_KEY",
		})
		require.NoError(t, err)

		got, err := b.DescribeConnection("my-conn")
		require.NoError(t, err)
		assert.Equal(t, "my-conn", got.Name)

		updated, err := b.UpdateConnection(eventbridge.UpdateConnectionInput{
			Name:        "my-conn",
			Description: "updated desc",
		})
		require.NoError(t, err)
		assert.Equal(t, "updated desc", updated.Description)

		conns, _, err := b.ListConnections("my-", "")
		require.NoError(t, err)
		assert.Len(t, conns, 1)

		require.NoError(t, b.DeleteConnection("my-conn"))
		_, err = b.DescribeConnection("my-conn")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})
}

// ---------------------------------------------------------------------------
// DeleteEndpoint / DescribeEndpoint / ListEndpoints / UpdateEndpoint
// ---------------------------------------------------------------------------

func TestRefinement2_EndpointCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeEndpoint("missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("delete not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		require.ErrorIs(t, b.DeleteEndpoint("missing"), eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.CreateEndpoint(eventbridge.CreateEndpointInput{
			Name: "my-ep",
			EventBuses: []eventbridge.EndpointEventBus{
				{EventBusArn: "arn:aws:events:us-east-1:123:event-bus/default"},
			},
		})
		require.NoError(t, err)

		got, err := b.DescribeEndpoint("my-ep")
		require.NoError(t, err)
		assert.Equal(t, "my-ep", got.Name)

		updated, err := b.UpdateEndpoint(eventbridge.UpdateEndpointInput{
			Name:        "my-ep",
			Description: "updated",
		})
		require.NoError(t, err)
		assert.Equal(t, "updated", updated.Description)

		eps, _, err := b.ListEndpoints("my-", "")
		require.NoError(t, err)
		assert.Len(t, eps, 1)

		require.NoError(t, b.DeleteEndpoint("my-ep"))
		_, err = b.DescribeEndpoint("my-ep")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})
}

// ---------------------------------------------------------------------------
// DescribeAPIDestination / ListAPIDestinations / UpdateAPIDestination
// ---------------------------------------------------------------------------

func TestRefinement2_APIDestinationCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeAPIDestination("missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		b.AddAPIDestinationInternal(&eventbridge.APIDestination{
			Name:                "dst1",
			ConnectionArn:       "arn:aws:events:us-east-1:123:connection/c1",
			HTTPMethod:          "POST",
			InvocationEndpoint:  "https://example.com/hook",
			APIDestinationState: "ACTIVE",
		})

		got, err := b.DescribeAPIDestination("dst1")
		require.NoError(t, err)
		assert.Equal(t, "dst1", got.Name)

		updated, err := b.UpdateAPIDestination(eventbridge.UpdateAPIDestinationInput{
			Name:        "dst1",
			Description: "desc updated",
		})
		require.NoError(t, err)
		assert.Equal(t, "desc updated", updated.Description)

		dsts, _, err := b.ListAPIDestinations("", "")
		require.NoError(t, err)
		assert.Len(t, dsts, 1)
	})
}

// ---------------------------------------------------------------------------
// DescribeEventSource / ListEventSources
// ---------------------------------------------------------------------------

func TestRefinement2_EventSourceCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeEventSource("missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		b.AddEventSourceInternal(&eventbridge.EventSource{
			Name:  "aws.partner.test",
			State: "PENDING",
		})

		got, err := b.DescribeEventSource("aws.partner.test")
		require.NoError(t, err)
		assert.Equal(t, "aws.partner.test", got.Name)

		sources, _, err := b.ListEventSources("aws.partner", "")
		require.NoError(t, err)
		assert.Len(t, sources, 1)
	})
}

// ---------------------------------------------------------------------------
// DescribePartnerEventSource / DeletePartnerEventSource / ListPartnerEventSources
// ---------------------------------------------------------------------------

func TestRefinement2_PartnerEventSourceCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribePartnerEventSource("missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("delete not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		require.ErrorIs(t, b.DeletePartnerEventSource("missing"), eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		b.AddPartnerSourceInternal(&eventbridge.PartnerEventSource{
			Name: "aws.partner.test.123",
		})

		got, err := b.DescribePartnerEventSource("aws.partner.test.123")
		require.NoError(t, err)
		assert.Equal(t, "aws.partner.test.123", got.Name)

		srcs, _, err := b.ListPartnerEventSources("aws.partner", "")
		require.NoError(t, err)
		assert.Len(t, srcs, 1)

		require.NoError(t, b.DeletePartnerEventSource("aws.partner.test.123"))
		_, err = b.DescribePartnerEventSource("aws.partner.test.123")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})
}

// ---------------------------------------------------------------------------
// PutPartnerEvents
// ---------------------------------------------------------------------------

func TestRefinement2_PutPartnerEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()
	results := b.PutPartnerEvents([]eventbridge.EventEntry{
		{Source: "aws.partner.test", DetailType: "Ping", Detail: "{}"},
	})
	assert.Len(t, results, 1)
	assert.Empty(t, results[0].ErrorCode)
}

// ---------------------------------------------------------------------------
// DescribeReplay / ListReplays / StartReplay round-trip
// ---------------------------------------------------------------------------

func TestRefinement2_ReplayCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeReplay("missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()

		start := time.Now().Add(-2 * time.Hour)
		end := time.Now().Add(-1 * time.Hour)

		created, err := b.StartReplay(eventbridge.StartReplayInput{
			ReplayName:     "my-replay",
			EventSourceArn: "arn:aws:events:us-east-1:123:archive/my-archive",
			EventStartTime: start,
			EventEndTime:   end,
		})
		require.NoError(t, err)
		assert.Equal(t, "my-replay", created.ReplayName)
		assert.Equal(t, "STARTING", created.State)

		got, err := b.DescribeReplay("my-replay")
		require.NoError(t, err)
		assert.Equal(t, "my-replay", got.ReplayName)

		replays, _, err := b.ListReplays("my-", "")
		require.NoError(t, err)
		assert.Len(t, replays, 1)

		// Duplicate name rejected.
		_, err = b.StartReplay(eventbridge.StartReplayInput{
			ReplayName:     "my-replay",
			EventSourceArn: "arn:aws:events:us-east-1:123:archive/my-archive",
		})
		require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
	})
}

// ---------------------------------------------------------------------------
// ListRuleNamesByTarget
// ---------------------------------------------------------------------------

func TestRefinement2_ListRuleNamesByTarget(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		targetARN string
		want      []string
	}{
		{
			name:      "finds rules with matching target",
			targetARN: "arn:aws:lambda:us-east-1:123:function:fn",
			want:      []string{"r1"},
		},
		{
			name:      "no match returns empty",
			targetARN: "arn:aws:lambda:us-east-1:123:function:other",
			want:      []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.PutRule(eventbridge.PutRuleInput{Name: "r1", ScheduleExpression: "rate(1 minute)"})
			require.NoError(t, err)
			_, err = b.PutTargets("r1", "", []eventbridge.Target{
				{ID: "t1", Arn: "arn:aws:lambda:us-east-1:123:function:fn"},
			})
			require.NoError(t, err)

			got, _, err := b.ListRuleNamesByTarget(tt.targetARN, "", "")
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// UpdateEventBus
// ---------------------------------------------------------------------------

func TestRefinement2_UpdateEventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    eventbridge.UpdateEventBusInput
		wantErr  error
		wantDesc string
	}{
		{
			name:    "not found returns error",
			input:   eventbridge.UpdateEventBusInput{Name: "no-such"},
			wantErr: eventbridge.ErrEventBusNotFound,
		},
		{
			name:     "updates description on default bus",
			input:    eventbridge.UpdateEventBusInput{Name: "", Description: "new"},
			wantDesc: "new",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			got, err := b.UpdateEventBus(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantDesc, got.Description)
		})
	}
}

// ---------------------------------------------------------------------------
// PutPermission / RemovePermission (no-ops)
// ---------------------------------------------------------------------------

func TestRefinement2_PutRemovePermission_NoOp(t *testing.T) {
	t.Parallel()
	b := newBackend()
	require.NoError(t, b.PutPermission(eventbridge.PutPermissionInput{StatementID: "s1"}))
	require.NoError(t, b.RemovePermission(eventbridge.RemovePermissionInput{StatementID: "s1"}))
}

// ---------------------------------------------------------------------------
// ListArchives / ListConnections / ListEndpoints pagination edge cases
// ---------------------------------------------------------------------------

func TestRefinement2_ListPagination(t *testing.T) {
	t.Parallel()

	t.Run("list archives empty returns empty slice not nil", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		got, next, err := b.ListArchives("", "")
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Empty(t, next)
	})

	t.Run("list connections empty returns empty slice not nil", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		got, next, err := b.ListConnections("", "")
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Empty(t, next)
	})

	t.Run("list endpoints empty returns empty slice not nil", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		got, next, err := b.ListEndpoints("", "")
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Empty(t, next)
	})

	t.Run("list replays empty returns empty slice not nil", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		got, next, err := b.ListReplays("", "")
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Empty(t, next)
	})

	t.Run("list API destinations empty returns empty slice not nil", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		got, next, err := b.ListAPIDestinations("", "")
		require.NoError(t, err)
		assert.Empty(t, got)
		assert.Empty(t, next)
	})

	t.Run("list rule names by target with no targets returns empty", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		got, _, err := b.ListRuleNamesByTarget("arn:aws:lambda:us-east-1:123:function:fn", "", "")
		require.NoError(t, err)
		assert.Empty(t, got)
	})
}
