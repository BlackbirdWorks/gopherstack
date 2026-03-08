package chaos_test

import (
	"fmt"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFaultStore_GetSetRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*chaos.FaultStore)
		name      string
		wantRules []chaos.FaultRule
	}{
		{
			name:      "empty store returns empty slice",
			wantRules: []chaos.FaultRule{},
		},
		{
			name: "set rules replaces all rules",
			setup: func(s *chaos.FaultStore) {
				s.SetRules([]chaos.FaultRule{
					{Service: "s3"},
					{Service: "dynamodb"},
				})
			},
			wantRules: []chaos.FaultRule{
				{Service: "s3"},
				{Service: "dynamodb"},
			},
		},
		{
			name: "set rules to empty clears existing rules",
			setup: func(s *chaos.FaultStore) {
				s.SetRules([]chaos.FaultRule{{Service: "s3"}})
				s.SetRules([]chaos.FaultRule{})
			},
			wantRules: []chaos.FaultRule{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			if tt.setup != nil {
				tt.setup(store)
			}

			got := store.GetRules()
			assert.Equal(t, tt.wantRules, got)
		})
	}
}

func TestFaultStore_AppendRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*chaos.FaultStore)
		name      string
		append    []chaos.FaultRule
		wantCount int
	}{
		{
			name:      "append to empty store",
			append:    []chaos.FaultRule{{Service: "s3"}},
			wantCount: 1,
		},
		{
			name: "append to non-empty store adds rules",
			setup: func(s *chaos.FaultStore) {
				s.SetRules([]chaos.FaultRule{{Service: "s3"}})
			},
			append:    []chaos.FaultRule{{Service: "dynamodb"}, {Service: "sqs"}},
			wantCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			if tt.setup != nil {
				tt.setup(store)
			}

			store.AppendRules(tt.append)
			assert.Len(t, store.GetRules(), tt.wantCount)
		})
	}
}

func TestFaultStore_DeleteRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*chaos.FaultStore)
		name      string
		delete    []chaos.FaultRule
		wantRules []chaos.FaultRule
	}{
		{
			name: "delete matching rule by service",
			setup: func(s *chaos.FaultStore) {
				s.SetRules([]chaos.FaultRule{
					{Service: "s3"},
					{Service: "dynamodb"},
				})
			},
			delete:    []chaos.FaultRule{{Service: "s3"}},
			wantRules: []chaos.FaultRule{{Service: "dynamodb"}},
		},
		{
			name: "delete non-existent rule leaves store unchanged",
			setup: func(s *chaos.FaultStore) {
				s.SetRules([]chaos.FaultRule{{Service: "s3"}})
			},
			delete:    []chaos.FaultRule{{Service: "lambda"}},
			wantRules: []chaos.FaultRule{{Service: "s3"}},
		},
		{
			name: "delete matches on service+operation+region combination",
			setup: func(s *chaos.FaultStore) {
				s.SetRules([]chaos.FaultRule{
					{Service: "s3", Operation: "PutObject", Region: "us-east-1"},
					{Service: "s3", Operation: "GetObject", Region: "us-east-1"},
				})
			},
			delete:    []chaos.FaultRule{{Service: "s3", Operation: "PutObject", Region: "us-east-1"}},
			wantRules: []chaos.FaultRule{{Service: "s3", Operation: "GetObject", Region: "us-east-1"}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			if tt.setup != nil {
				tt.setup(store)
			}

			store.DeleteRules(tt.delete)
			assert.Equal(t, tt.wantRules, store.GetRules())
		})
	}
}

func TestFaultStore_Match(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		svc         string
		op          string
		region      string
		wantService string
		rules       []chaos.FaultRule
		wantMatched bool
	}{
		{
			name:        "empty rules never match",
			rules:       []chaos.FaultRule{},
			svc:         "s3",
			wantMatched: false,
		},
		{
			name:        "empty rule matches everything",
			rules:       []chaos.FaultRule{{}},
			svc:         "s3",
			op:          "PutObject",
			region:      "us-east-1",
			wantMatched: true,
		},
		{
			name:        "service rule matches matching service",
			rules:       []chaos.FaultRule{{Service: "s3"}},
			svc:         "s3",
			op:          "PutObject",
			wantMatched: true,
			wantService: "s3",
		},
		{
			name:        "service rule does not match different service",
			rules:       []chaos.FaultRule{{Service: "s3"}},
			svc:         "dynamodb",
			wantMatched: false,
		},
		{
			name: "operation rule matches when operation matches",
			rules: []chaos.FaultRule{
				{Service: "dynamodb", Operation: "GetItem"},
			},
			svc:         "dynamodb",
			op:          "GetItem",
			wantMatched: true,
		},
		{
			name: "operation rule does not match different operation",
			rules: []chaos.FaultRule{
				{Service: "dynamodb", Operation: "GetItem"},
			},
			svc:         "dynamodb",
			op:          "PutItem",
			wantMatched: false,
		},
		{
			name: "region rule matches when region matches",
			rules: []chaos.FaultRule{
				{Service: "s3", Region: "us-east-1"},
			},
			svc:         "s3",
			region:      "us-east-1",
			wantMatched: true,
		},
		{
			name: "region rule does not match different region",
			rules: []chaos.FaultRule{
				{Service: "s3", Region: "us-east-1"},
			},
			svc:         "s3",
			region:      "eu-west-1",
			wantMatched: false,
		},
		{
			name: "first matching rule wins",
			rules: []chaos.FaultRule{
				{Service: "s3"},
				{Service: "s3", Operation: "PutObject"},
			},
			svc:         "s3",
			op:          "PutObject",
			wantMatched: true,
			wantService: "s3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			store.SetRules(tt.rules)

			rule, matched := store.Match(tt.svc, tt.op, tt.region)
			assert.Equal(t, tt.wantMatched, matched)

			if tt.wantService != "" {
				assert.Equal(t, tt.wantService, rule.Service)
			}
		})
	}
}

func TestFaultStore_Effects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		effects chaos.NetworkEffects
	}{
		{
			name:    "default effects are zero value",
			effects: chaos.NetworkEffects{},
		},
		{
			name:    "set fixed latency",
			effects: chaos.NetworkEffects{Latency: 200},
		},
		{
			name: "set latency range",
			effects: chaos.NetworkEffects{
				LatencyRange: &chaos.LatencyRange{Min: 100, Max: 500},
			},
		},
		{
			name:    "set jitter",
			effects: chaos.NetworkEffects{Jitter: 50},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			store.SetEffects(tt.effects)

			got := store.GetEffects()
			assert.Equal(t, tt.effects, got)
		})
	}
}

func TestFaultRule_ShouldTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		rule        chaos.FaultRule
		wantTrigger bool
	}{
		{
			name:        "zero probability always triggers (treated as 1.0)",
			rule:        chaos.FaultRule{Probability: 0},
			wantTrigger: true,
		},
		{
			name:        "probability 1.0 always triggers",
			rule:        chaos.FaultRule{Probability: 1.0},
			wantTrigger: true,
		},
		{
			name:        "probability above 1.0 always triggers",
			rule:        chaos.FaultRule{Probability: 2.0},
			wantTrigger: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.rule.ShouldTrigger()
			assert.Equal(t, tt.wantTrigger, got)
		})
	}
}

func TestFaultRule_EffectiveError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantCode       string
		rule           chaos.FaultRule
		wantStatusCode int
	}{
		{
			name:           "no error field uses default 503 ServiceUnavailable",
			rule:           chaos.FaultRule{},
			wantStatusCode: 503,
			wantCode:       "ServiceUnavailable",
		},
		{
			name: "custom error is returned as-is",
			rule: chaos.FaultRule{
				Error: &chaos.FaultError{StatusCode: 400, Code: "ProvisionedThroughputExceededException"},
			},
			wantStatusCode: 400,
			wantCode:       "ProvisionedThroughputExceededException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := tt.rule.EffectiveError()
			assert.Equal(t, tt.wantStatusCode, got.StatusCode)
			assert.Equal(t, tt.wantCode, got.Code)
		})
	}
}

func TestNetworkEffects_TotalDelayMs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		effects        chaos.NetworkEffects
		wantMinDelayMs int
		wantMaxDelayMs int
	}{
		{
			name:           "zero effects returns zero delay",
			effects:        chaos.NetworkEffects{},
			wantMinDelayMs: 0,
			wantMaxDelayMs: 0,
		},
		{
			name:           "fixed latency returns exact delay",
			effects:        chaos.NetworkEffects{Latency: 200},
			wantMinDelayMs: 200,
			wantMaxDelayMs: 200,
		},
		{
			name: "latency range returns delay within range",
			effects: chaos.NetworkEffects{
				LatencyRange: &chaos.LatencyRange{Min: 100, Max: 500},
			},
			wantMinDelayMs: 100,
			wantMaxDelayMs: 499,
		},
		{
			name:           "jitter adds additional delay",
			effects:        chaos.NetworkEffects{Jitter: 50},
			wantMinDelayMs: 0,
			wantMaxDelayMs: 49,
		},
		{
			name: "combined latency and jitter",
			effects: chaos.NetworkEffects{
				Latency: 100,
				Jitter:  50,
			},
			wantMinDelayMs: 100,
			wantMaxDelayMs: 149,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Run several times to exercise the random components.
			for range 50 {
				got := tt.effects.TotalDelayMs()
				require.GreaterOrEqual(t, got, tt.wantMinDelayMs,
					"expected delay >= %d, got %d", tt.wantMinDelayMs, got)
				require.LessOrEqual(t, got, tt.wantMaxDelayMs,
					"expected delay <= %d, got %d", tt.wantMaxDelayMs, got)
			}
		})
	}
}

func TestFaultStore_RecordActivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(*chaos.FaultStore)
		name      string
		wantFirst string // Service of the newest (index 0) entry returned by GetActivity
		wantLast  string // Service of the oldest (last-index) entry returned by GetActivity
		wantLen   int
	}{
		{
			name:    "empty store returns empty activity",
			wantLen: 0,
		},
		{
			name: "single event is returned",
			setup: func(s *chaos.FaultStore) {
				s.RecordActivity(chaos.ActivityEvent{Service: "s3", Triggered: true})
			},
			wantLen:   1,
			wantFirst: "s3",
		},
		{
			name: "multiple events are returned newest-first",
			setup: func(s *chaos.FaultStore) {
				s.RecordActivity(chaos.ActivityEvent{Service: "s3"})
				s.RecordActivity(chaos.ActivityEvent{Service: "dynamodb"})
				s.RecordActivity(chaos.ActivityEvent{Service: "sqs"})
			},
			wantLen:   3,
			wantFirst: "sqs",
			wantLast:  "s3",
		},
		{
			name: "ring buffer trims to 100 entries and releases old backing array",
			setup: func(s *chaos.FaultStore) {
				// Record 105 events; only the last 100 should survive.
				for i := range 105 {
					s.RecordActivity(chaos.ActivityEvent{
						Service: fmt.Sprintf("svc-%03d", i),
					})
				}
			},
			wantLen:   100,
			wantFirst: "svc-104", // newest
			wantLast:  "svc-005", // oldest retained
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := chaos.NewFaultStore()
			if tt.setup != nil {
				tt.setup(store)
			}

			got := store.GetActivity()

			require.Len(t, got, tt.wantLen)
			if tt.wantFirst != "" {
				assert.Equal(t, tt.wantFirst, got[0].Service)
			}
			if tt.wantLast != "" {
				assert.Equal(t, tt.wantLast, got[len(got)-1].Service)
			}
		})
	}
}
