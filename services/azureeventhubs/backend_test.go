package azureeventhubs_test

import (
	"context"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azureeventhubs"
)

func hubRef() azureeventhubs.HubRef { return azureeventhubs.HubRef{Namespace: testNS, Hub: testHub} }

func newBackend(t *testing.T, cfg azureeventhubs.HubConfig) *azureeventhubs.InMemoryBackend {
	t.Helper()

	b := azureeventhubs.NewInMemoryBackend()
	require.NoError(t, b.EnsureNamespace(testNS))
	_, err := b.CreateHub(hubRef(), cfg)
	require.NoError(t, err)

	return b
}

func ev(body string) azureeventhubs.NewEvent { return azureeventhubs.NewEvent{Raw: []byte(body)} }

func TestBackend_RetentionExpiry(t *testing.T) {
	t.Parallel()

	b := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 2, Retention: time.Hour})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	azureeventhubs.SetNow(b, func() time.Time { return now })

	_, err := b.Append(hubRef(), 0, []azureeventhubs.NewEvent{ev("old1"), ev("old2")})
	require.NoError(t, err)

	now = now.Add(40 * time.Minute)

	_, err = b.Append(hubRef(), 0, []azureeventhubs.NewEvent{ev("fresh")})
	require.NoError(t, err)

	tests := []struct {
		name      string
		advance   time.Duration
		wantEmpty bool
		wantBegin int64
		wantLast  int64
	}{
		{name: "within retention", advance: 10 * time.Minute, wantBegin: 0, wantLast: 2},
		{name: "oldest expired", advance: 30 * time.Minute, wantBegin: 2, wantLast: 2},
		{name: "everything expired", advance: 2 * time.Hour, wantEmpty: true, wantBegin: 3, wantLast: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b2 := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 1, Retention: time.Hour})
			clock := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
			azureeventhubs.SetNow(b2, func() time.Time { return clock })

			_, appendErr := b2.Append(hubRef(), 0, []azureeventhubs.NewEvent{ev("old1"), ev("old2")})
			require.NoError(t, appendErr)

			clock = clock.Add(40 * time.Minute)
			_, appendErr = b2.Append(hubRef(), 0, []azureeventhubs.NewEvent{ev("fresh")})
			require.NoError(t, appendErr)

			clock = clock.Add(tt.advance)

			info, infoErr := b2.PartitionInfo(hubRef(), 0)
			require.NoError(t, infoErr)
			assert.Equal(t, tt.wantEmpty, info.Empty)
			assert.Equal(t, tt.wantBegin, info.BeginSequence)
			assert.Equal(t, tt.wantLast, info.LastSequence)
		})
	}

	assert.Equal(t, 1, b.Sweep(now.Add(5*time.Hour)))

	info, err := b.PartitionInfo(hubRef(), 0)
	require.NoError(t, err)
	assert.True(t, info.Empty)
}

func TestBackend_Resolve(t *testing.T) {
	t.Parallel()

	b := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 1})
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	for i := range 4 {
		step := now.Add(time.Duration(i) * time.Minute)
		azureeventhubs.SetNow(b, func() time.Time { return step })

		_, err := b.Append(hubRef(), 0, []azureeventhubs.NewEvent{ev("event-" + strconv.Itoa(i))})
		require.NoError(t, err)
	}

	info, err := b.PartitionInfo(hubRef(), 0)
	require.NoError(t, err)

	tests := []struct {
		expr string
		want int64
	}{
		{expr: "amqp.annotation.x-opt-offset > '-1'", want: 0},
		{expr: "amqp.annotation.x-opt-offset > '@latest'", want: 4},
		{expr: "amqp.annotation.x-opt-offset >= '@latest'", want: 4},
		{expr: "amqp.annotation.x-opt-offset > '" + strconv.FormatInt(info.LastOffset, 10) + "'", want: 4},
		{expr: "amqp.annotation.x-opt-offset >= '" + strconv.FormatInt(info.LastOffset, 10) + "'", want: 3},
		{expr: "amqp.annotation.x-opt-sequence-number > '1'", want: 2},
		{expr: "amqp.annotation.x-opt-sequence-number >= '1'", want: 1},
		{expr: "amqp.annotation.x-opt-sequence-number > '99'", want: 4},
		{
			expr: "amqp.annotation.x-opt-enqueued-time > '" + strconv.FormatInt(
				now.Add(time.Minute).UnixMilli(),
				10,
			) + "'",
			want: 2,
		},
		{
			expr: "amqp.annotation.x-opt-enqueued-time >= '" + strconv.FormatInt(
				now.Add(time.Minute).UnixMilli(),
				10,
			) + "'",
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.expr, func(t *testing.T) {
			t.Parallel()

			sel, parseErr := azureeventhubs.ParseSelector(tt.expr)
			require.NoError(t, parseErr)

			got, resErr := b.Resolve(hubRef(), 0, sel)
			require.NoError(t, resErr)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseSelector_Invalid(t *testing.T) {
	t.Parallel()

	for _, expr := range []string{
		"", "x > 1", "amqp.annotation.x-opt-offset > ''", "amqp.annotation.x-opt-offset > 'abc'",
		"amqp.annotation.x-opt-sequence-number < '1'", "amqp.annotation.x-opt-foo > '1'",
	} {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()

			_, err := azureeventhubs.ParseSelector(expr)
			require.ErrorIs(t, err, azureeventhubs.ErrInvalidSelector)
		})
	}
}

func TestBackend_HubLifecycle(t *testing.T) {
	t.Parallel()

	b := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 4})

	tests := []struct {
		run  func() error
		want error
		name string
	}{
		{name: "too many partitions", run: func() error {
			_, err := b.CreateHub(
				azureeventhubs.HubRef{Namespace: testNS, Hub: "x"},
				azureeventhubs.HubConfig{PartitionCount: 5000},
			)

			return err
		}, want: azureeventhubs.ErrInvalidPartitionCount},
		{name: "retention too long", run: func() error {
			_, err := b.CreateHub(
				azureeventhubs.HubRef{Namespace: testNS, Hub: "x"},
				azureeventhubs.HubConfig{Retention: 1000 * 24 * time.Hour},
			)

			return err
		}, want: azureeventhubs.ErrInvalidRetention},
		{name: "shrink partitions", run: func() error {
			_, err := b.CreateHub(hubRef(), azureeventhubs.HubConfig{PartitionCount: 2})

			return err
		}, want: azureeventhubs.ErrPartitionCountShrink},
		{name: "unknown namespace", run: func() error {
			_, err := b.CreateHub(azureeventhubs.HubRef{Namespace: "nope", Hub: "x"}, azureeventhubs.HubConfig{})

			return err
		}, want: azureeventhubs.ErrNamespaceNotFound},
		{name: "invalid name", run: func() error {
			_, err := b.CreateHub(azureeventhubs.HubRef{Namespace: testNS, Hub: "a/b"}, azureeventhubs.HubConfig{})

			return err
		}, want: azureeventhubs.ErrInvalidName},
		{name: "default group protected", run: func() error {
			return b.DeleteConsumerGroup(hubRef(), azureeventhubs.DefaultConsumerGroup)
		}, want: azureeventhubs.ErrProtectedEntity},
		{
			name: "missing group",
			run:  func() error { return b.DeleteConsumerGroup(hubRef(), "nope") },
			want: azureeventhubs.ErrConsumerGroupNotFound,
		},
		{name: "root rule protected", run: func() error {
			return b.DeleteRule(testNS, "", azureeventhubs.DefaultKeyName)
		}, want: azureeventhubs.ErrProtectedEntity},
		{name: "append to missing partition", run: func() error {
			_, err := b.Append(hubRef(), 9, []azureeventhubs.NewEvent{ev("x")})

			return err
		}, want: azureeventhubs.ErrPartitionNotFound},
		{name: "oversized event", run: func() error {
			_, err := b.Append(
				hubRef(),
				0,
				[]azureeventhubs.NewEvent{{Raw: make([]byte, azureeventhubs.MaxEventBytes+1)}},
			)

			return err
		}, want: azureeventhubs.ErrEventTooLarge},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			require.ErrorIs(t, tt.run(), tt.want)
		})
	}

	t.Run("growth keeps events", func(t *testing.T) {
		t.Parallel()

		b2 := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 2})
		_, err := b2.Append(hubRef(), 1, []azureeventhubs.NewEvent{ev("kept")})
		require.NoError(t, err)

		info, err := b2.CreateHub(hubRef(), azureeventhubs.HubConfig{PartitionCount: 6})
		require.NoError(t, err)
		assert.Len(t, info.PartitionIDs, 6)

		p, err := b2.PartitionInfo(hubRef(), 1)
		require.NoError(t, err)
		assert.False(t, p.Empty)
	})

	t.Run("hub scoped rule removed with hub", func(t *testing.T) {
		t.Parallel()

		b2 := newBackend(t, azureeventhubs.HubConfig{})
		_, err := b2.PutRule(testNS, testHub, "send", []string{azureeventhubs.RightSend})
		require.NoError(t, err)
		require.NoError(t, b2.DeleteHub(hubRef()))

		_, err = b2.GetRule(testNS, testHub, "send")
		require.ErrorIs(t, err, azureeventhubs.ErrRuleNotFound)
	})
}

func TestBackend_PartitionSelection(t *testing.T) {
	t.Parallel()

	b := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 8})

	first, err := b.Append(hubRef(), -1, []azureeventhubs.NewEvent{{Key: "customer-42", Raw: []byte("a")}})
	require.NoError(t, err)

	for range 5 {
		got, appendErr := b.Append(hubRef(), -1, []azureeventhubs.NewEvent{{Key: "customer-42", Raw: []byte("b")}})
		require.NoError(t, appendErr)
		assert.Equal(t, first, got, "same key, same partition")
	}

	seen := map[int]bool{}

	for range 16 {
		pid, appendErr := b.Append(hubRef(), -1, []azureeventhubs.NewEvent{ev("rr")})
		require.NoError(t, appendErr)

		seen[pid] = true
	}

	assert.Len(t, seen, 8, "keyless events spread across partitions")
}

func TestBackend_EpochArbitration(t *testing.T) {
	t.Parallel()

	epoch := func(n int64) *int64 { return &n }

	tests := []struct {
		wantErr   error
		first     *int64
		second    *int64
		name      string
		wantKicks bool
	}{
		{name: "higher epoch kicks lower", first: epoch(1), second: epoch(2), wantKicks: true},
		{name: "equal epoch kicks", first: epoch(3), second: epoch(3), wantKicks: true},
		{name: "lower epoch rejected", first: epoch(5), second: epoch(4), wantErr: azureeventhubs.ErrReceiverStolen},
		{name: "epoch kicks plain", first: nil, second: epoch(1), wantKicks: true},
		{name: "plain rejected by epoch", first: epoch(1), second: nil, wantErr: azureeventhubs.ErrReceiverStolen},
		{name: "plain coexist", first: nil, second: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 1})
			a, err := b.RegisterReceiver(hubRef(), azureeventhubs.DefaultConsumerGroup, 0, "a", tt.first)
			require.NoError(t, err)

			_, err = b.RegisterReceiver(hubRef(), azureeventhubs.DefaultConsumerGroup, 0, "b", tt.second)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			select {
			case <-a.Kicked():
				assert.True(t, tt.wantKicks)
				assert.Contains(t, a.Reason(), "disconnected")
			default:
				assert.False(t, tt.wantKicks)
			}
		})
	}

	t.Run("max plain receivers", func(t *testing.T) {
		t.Parallel()

		b := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 1})

		for i := range 5 {
			_, err := b.RegisterReceiver(hubRef(), azureeventhubs.DefaultConsumerGroup, 0, strconv.Itoa(i), nil)
			require.NoError(t, err)
		}

		_, err := b.RegisterReceiver(hubRef(), azureeventhubs.DefaultConsumerGroup, 0, "extra", nil)
		require.ErrorIs(t, err, azureeventhubs.ErrTooManyReceivers)
	})

	t.Run("unknown consumer group", func(t *testing.T) {
		t.Parallel()

		b := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 1})
		_, err := b.RegisterReceiver(hubRef(), "nope", 0, "a", nil)
		require.ErrorIs(t, err, azureeventhubs.ErrConsumerGroupNotFound)
	})
}

func TestBackend_NextBlocksUntilAppend(t *testing.T) {
	t.Parallel()

	b := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 1})

	type result struct {
		err error
		raw string
	}

	done := make(chan result, 1)

	go func() {
		e, err := b.Next(t.Context(), hubRef(), 0, 0, nil)
		if err != nil {
			done <- result{err: err}

			return
		}

		done <- result{raw: string(e.Raw)}
	}()

	_, err := b.Append(hubRef(), 0, []azureeventhubs.NewEvent{ev("wake")})
	require.NoError(t, err)

	r := <-done
	require.NoError(t, r.err)
	assert.Equal(t, "wake", r.raw)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	_, err = b.Next(ctx, hubRef(), 0, 1, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	b := newBackend(t, azureeventhubs.HubConfig{PartitionCount: 2, Retention: 48 * time.Hour})
	_, err := b.CreateConsumerGroup(hubRef(), "cg1")
	require.NoError(t, err)

	rule, err := b.PutRule(testNS, testHub, "send", []string{azureeventhubs.RightSend})
	require.NoError(t, err)

	_, err = b.Append(hubRef(), 1, []azureeventhubs.NewEvent{{Key: "k", Raw: []byte("persisted")}})
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())

	restored := azureeventhubs.NewInMemoryBackend()
	require.NoError(t, restored.Restore(t.Context(), snap))

	info, err := restored.Hub(hubRef())
	require.NoError(t, err)
	assert.Equal(t, 48*time.Hour, info.Config.Retention)
	assert.Len(t, info.PartitionIDs, 2)

	groups, err := restored.ListConsumerGroups(hubRef())
	require.NoError(t, err)
	assert.Equal(t, []string{"$Default", "cg1"}, groups)

	got, err := restored.GetRule(testNS, testHub, "send")
	require.NoError(t, err)
	assert.Equal(t, rule.PrimaryKey, got.PrimaryKey)

	e, err := restored.Next(t.Context(), hubRef(), 1, 0, nil)
	require.NoError(t, err)
	assert.Equal(t, "persisted", string(e.Raw))

	_, err = restored.Append(hubRef(), 1, []azureeventhubs.NewEvent{ev("after")})
	require.NoError(t, err, "restored partitions accept new events and wake channels work")

	require.NoError(t, restored.Restore(t.Context(), []byte(`{"version":99}`)))
	assert.False(t, restored.NamespaceExists(testNS))
}

func TestClaimRights(t *testing.T) {
	t.Parallel()

	b := newBackend(t, azureeventhubs.HubConfig{})
	rule, err := b.PutRule(testNS, "", "listen", []string{azureeventhubs.RightListen})
	require.NoError(t, err)

	token := func(name, key string, expiry int64, resource string) string {
		sig := azureeventhubs.SignSAS(resource, expiry, key)

		return "SharedAccessSignature sr=" + resource + "&sig=" + urlEscape(
			sig,
		) + "&se=" + strconv.FormatInt(
			expiry,
			10,
		) +
			"&skn=" + name
	}

	future := time.Now().Add(time.Hour).Unix()
	past := time.Now().Add(-time.Hour).Unix()

	tests := []struct {
		name       string
		token      string
		wantRights []string
		wantOK     bool
	}{
		{
			name:       "root",
			token:      token(azureeventhubs.DefaultKeyName, azureeventhubs.DefaultKeyValue, future, "res"),
			wantOK:     true,
			wantRights: []string{"Listen", "Send", "Manage"},
		},
		{
			name:       "listen rule",
			token:      token("listen", rule.PrimaryKey, future, "res"),
			wantOK:     true,
			wantRights: []string{"Listen"},
		},
		{
			name:       "secondary key",
			token:      token("listen", rule.SecondaryKey, future, "res"),
			wantOK:     true,
			wantRights: []string{"Listen"},
		},
		{name: "expired", token: token("listen", rule.PrimaryKey, past, "res")},
		{name: "wrong key", token: token("listen", azureeventhubs.DefaultKeyValue, future, "res")},
		{name: "unknown rule", token: token("ghost", rule.PrimaryKey, future, "res")},
		{name: "garbage", token: "SharedAccessSignature nonsense"},
		{name: "oversized", token: "SharedAccessSignature " + string(make([]byte, 10000))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rights, ok := azureeventhubs.ClaimRights(b, tt.token)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantRights, rights)
		})
	}
}

func urlEscape(s string) string { return url.QueryEscape(s) }
