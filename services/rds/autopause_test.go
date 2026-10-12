package rds_test

import (
	"net/url"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/blackbirdworks/gopherstack/services/rdsdata"
)

const (
	pauseClusterID = "pause1"
	pauseWriterID  = "pause1-w"
	availableState = "available"
)

type pauseOpts struct {
	class    string
	cfg      rds.AutoPauseConfig
	scaling  rds.ServerlessV2ScalingConfiguration
	httpEnab bool
}

func defaultPauseOpts() pauseOpts {
	return pauseOpts{
		scaling: rds.ServerlessV2ScalingConfiguration{MinCapacity: 0, MaxCapacity: 4},
		class:   "db.serverless",
	}
}

// newPauseBackend builds a metadata-only Aurora cluster with one writer and waits for it to settle; call
// it inside a synctest bubble.
func newPauseBackend(t *testing.T, o pauseOpts) *rds.InMemoryBackend {
	t.Helper()

	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(b.Close)
	b.SetAutoPause(o.cfg)

	scaling := o.scaling

	_, err := b.CreateDBCluster(pauseClusterID, "aurora-postgresql", "admin", "", "", 0, &scaling,
		rds.DBClusterOptions{EnableHTTPEndpoint: o.httpEnab})
	require.NoError(t, err)

	_, err = b.CreateDBInstance(pauseWriterID, "aurora-postgresql", o.class, "", "admin", "", 0,
		rds.DBInstanceOptions{DBClusterIdentifier: pauseClusterID})
	require.NoError(t, err)

	time.Sleep(time.Second)
	synctest.Wait()

	return b
}

func describeCluster(t *testing.T, b *rds.InMemoryBackend) rds.DBCluster {
	t.Helper()

	got, err := b.DescribeDBClusters(pauseClusterID)
	require.NoError(t, err)
	require.Len(t, got, 1)

	return got[0]
}

func eventMessages(t *testing.T, b *rds.InMemoryBackend, source string) []string {
	t.Helper()

	evs, err := b.DescribeEvents(source, "db-instance", 0)
	require.NoError(t, err)

	msgs := make([]string, 0, len(evs))
	for _, ev := range evs {
		msgs = append(msgs, ev.Message)
	}

	return msgs
}

func TestServerlessV2AutoPauseAfterIdle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate     func(*pauseOpts)
		name       string
		before     time.Duration
		after      time.Duration
		wantPaused bool
	}{
		{name: "default_window", before: 290 * time.Second, after: 20 * time.Second, wantPaused: true},
		{
			name:   "custom_window",
			mutate: func(o *pauseOpts) { o.scaling.SecondsUntilAutoPause = 600 },
			before: 590 * time.Second, after: 20 * time.Second, wantPaused: true,
		},
		{
			name:   "idle_scale",
			mutate: func(o *pauseOpts) { o.cfg.IdleScale = 0.1 },
			before: 25 * time.Second, after: 10 * time.Second, wantPaused: true,
		},
		{
			name:   "disabled",
			mutate: func(o *pauseOpts) { o.cfg.Disabled = true },
			before: 290 * time.Second, after: 200 * time.Second,
		},
		{
			name:   "nonzero_min",
			mutate: func(o *pauseOpts) { o.scaling.MinCapacity = 0.5 },
			before: 290 * time.Second, after: 200 * time.Second,
		},
		{
			name:   "provisioned_member",
			mutate: func(o *pauseOpts) { o.class = "db.r5.large" },
			before: 290 * time.Second, after: 200 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				o := defaultPauseOpts()
				if tt.mutate != nil {
					tt.mutate(&o)
				}

				b := newPauseBackend(t, o)

				time.Sleep(tt.before)
				synctest.Wait()
				assert.False(t, b.IsClusterAutoPaused(pauseClusterID), "paused before the idle window elapsed")

				time.Sleep(tt.after)
				synctest.Wait()
				assert.Equal(t, tt.wantPaused, b.IsClusterAutoPaused(pauseClusterID))

				c := describeCluster(t, b)
				assert.Equal(t, availableState, c.Status, "a paused cluster still reports available")
				assert.Equal(t, tt.wantPaused, rds.ClusterAutoPaused(&c))

				msgs := eventMessages(t, b, pauseWriterID)
				if tt.wantPaused {
					assert.Contains(t, msgs, "Initiated pause for the DB instance.")
					assert.Contains(t, msgs, "Successfully paused the DB instance.")
				} else {
					assert.NotContains(t, msgs, "Successfully paused the DB instance.")
				}

				insts, err := b.DescribeDBInstances(pauseWriterID)
				require.NoError(t, err)
				assert.Equal(t, availableState, insts[0].DBInstanceStatus)
			})
		})
	}
}

func TestServerlessV2AutoPauseActivity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		httpEnable bool
		wantPaused bool
	}{
		{name: "data_api_resets_timer", httpEnable: true},
		{name: "http_endpoint_off_is_not_activity", wantPaused: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				o := defaultPauseOpts()
				o.httpEnab = tt.httpEnable
				b := newPauseBackend(t, o)
				arn := describeCluster(t, b).DBClusterArn

				time.Sleep(200 * time.Second)
				synctest.Wait()

				_, err := b.DataAPITarget(arn)
				require.ErrorIs(t, err, rdsdata.ErrNotRealCluster)

				time.Sleep(250 * time.Second)
				synctest.Wait()
				assert.Equal(t, tt.wantPaused, b.IsClusterAutoPaused(pauseClusterID))

				time.Sleep(70 * time.Second)
				synctest.Wait()
				assert.True(t, b.IsClusterAutoPaused(pauseClusterID), "idle again, must pause")
			})
		})
	}
}

func TestServerlessV2DataAPIResume(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		checks []time.Duration
		want   []bool
		delay  time.Duration
	}{
		{name: "immediate", checks: []time.Duration{0}, want: []bool{false}},
		{
			name: "delayed", delay: 10 * time.Second,
			checks: []time.Duration{5 * time.Second, 6 * time.Second}, want: []bool{true, false},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				o := defaultPauseOpts()
				o.httpEnab = true
				o.cfg.ResumeDelay = tt.delay
				b := newPauseBackend(t, o)
				arn := describeCluster(t, b).DBClusterArn

				time.Sleep(320 * time.Second)
				synctest.Wait()
				require.True(t, b.IsClusterAutoPaused(pauseClusterID))

				_, err := b.DataAPITarget(arn)
				require.ErrorIs(t, err, rdsdata.ErrDatabaseResuming, "first call on a paused cluster")
				assert.Contains(t, eventMessages(t, b, pauseWriterID), "Initiated resume for the DB instance.")

				for i, d := range tt.checks {
					time.Sleep(d)
					synctest.Wait()

					_, err = b.DataAPITarget(arn)
					if tt.want[i] {
						require.ErrorIs(t, err, rdsdata.ErrDatabaseResuming)
					} else {
						require.ErrorIs(t, err, rdsdata.ErrNotRealCluster, "resumed cluster serves the call")
					}
				}

				synctest.Wait()
				assert.False(t, b.IsClusterAutoPaused(pauseClusterID))
				resumed := describeCluster(t, b)
				assert.False(t, rds.ClusterAutoPaused(&resumed))
				assert.Contains(t, eventMessages(t, b, pauseWriterID), "Successfully resumed the DB instance.")
			})
		})
	}
}

func TestServerlessV2AutoPauseMetrics(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newPauseBackend(t, defaultPauseOpts())

		var (
			mu     sync.Mutex
			points []cwmetric.Point
		)

		b.SetMetricEmitter(cwmetric.EmitterFunc(func(p cwmetric.Point) error {
			mu.Lock()
			defer mu.Unlock()

			points = append(points, p)

			return nil
		}))

		time.Sleep(290 * time.Second)
		synctest.Wait()

		mu.Lock()
		assert.Empty(t, points, "no zero-capacity metrics before the pause")
		mu.Unlock()

		time.Sleep(90 * time.Second)
		synctest.Wait()

		mu.Lock()
		defer mu.Unlock()

		names := map[string]float64{}

		for _, p := range points {
			assert.Equal(t, "AWS/RDS", p.Namespace)
			require.Len(t, p.Dimensions, 1)
			assert.Equal(t, pauseWriterID, p.Dimensions[0].Value)

			names[p.Name] += p.Value
		}

		assert.Contains(t, names, "ServerlessDatabaseCapacity")
		assert.Contains(t, names, "ACUUtilization")
		assert.Contains(t, names, "CPUUtilization")
		assert.InDelta(t, 0, names["ServerlessDatabaseCapacity"], 0)
	})
}

func TestServerlessV2ModifyWakesPausedCluster(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		b := newPauseBackend(t, defaultPauseOpts())

		time.Sleep(320 * time.Second)
		synctest.Wait()
		require.True(t, b.IsClusterAutoPaused(pauseClusterID))

		_, err := b.ModifyDBCluster(pauseClusterID, "", rds.DBClusterOptions{
			ServerlessV2Scaling: &rds.ServerlessV2ScalingConfiguration{MinCapacity: 2, MaxCapacity: 8},
		})
		require.NoError(t, err)
		synctest.Wait()

		assert.False(t, b.IsClusterAutoPaused(pauseClusterID), "changing the scaling range resumes the cluster")

		c := describeCluster(t, b)
		require.NotNil(t, c.ServerlessV2ScalingConfig)
		assert.InDelta(t, 2, c.ServerlessV2ScalingConfig.MinCapacity, 0)
		assert.Zero(t, c.ServerlessV2ScalingConfig.SecondsUntilAutoPause)

		time.Sleep(600 * time.Second)
		synctest.Wait()
		assert.False(t, b.IsClusterAutoPaused(pauseClusterID), "a nonzero minimum never pauses")
	})
}

func TestServerlessV2ScalingValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     rds.ServerlessV2ScalingConfiguration
		wantErr bool
		// zero MaxCapacity on modify means "unchanged", so only create rejects it.
		createOnly bool
	}{
		{name: "zero_min", cfg: rds.ServerlessV2ScalingConfiguration{MaxCapacity: 4}},
		{name: "half_step", cfg: rds.ServerlessV2ScalingConfiguration{MinCapacity: 0.5, MaxCapacity: 256}},
		{
			name: "pause_bounds",
			cfg:  rds.ServerlessV2ScalingConfiguration{MaxCapacity: 4, SecondsUntilAutoPause: 86400},
		},
		{
			name:    "negative_min",
			cfg:     rds.ServerlessV2ScalingConfiguration{MinCapacity: -1, MaxCapacity: 4},
			wantErr: true,
		},
		{
			name:    "min_off_step",
			cfg:     rds.ServerlessV2ScalingConfiguration{MinCapacity: 0.3, MaxCapacity: 4},
			wantErr: true,
		},
		{name: "max_off_step", cfg: rds.ServerlessV2ScalingConfiguration{MaxCapacity: 4.25}, wantErr: true},
		{name: "max_over_limit", cfg: rds.ServerlessV2ScalingConfiguration{MaxCapacity: 256.5}, wantErr: true},
		{
			name:       "max_zero",
			cfg:        rds.ServerlessV2ScalingConfiguration{MinCapacity: 0, MaxCapacity: 0},
			wantErr:    true,
			createOnly: true,
		},
		{
			name:    "min_above_max",
			cfg:     rds.ServerlessV2ScalingConfiguration{MinCapacity: 8, MaxCapacity: 4},
			wantErr: true,
		},
		{
			name: "pause_too_short",
			cfg:  rds.ServerlessV2ScalingConfiguration{MaxCapacity: 4, SecondsUntilAutoPause: 299}, wantErr: true,
		},
		{
			name: "pause_too_long",
			cfg:  rds.ServerlessV2ScalingConfiguration{MaxCapacity: 4, SecondsUntilAutoPause: 86401}, wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run("create_"+tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBatch3Backend(t)
			cfg := tt.cfg

			_, err := b.CreateDBCluster("val1", "aurora-postgresql", "admin", "", "", 0, &cfg, rds.DBClusterOptions{})
			if tt.wantErr {
				require.ErrorIs(t, err, rds.ErrInvalidParameter)

				_, derr := b.DescribeDBClusters("val1")
				require.ErrorIs(t, derr, rds.ErrClusterNotFound, "rejected create must not leave a cluster")

				return
			}

			require.NoError(t, err)
		})

		if tt.createOnly {
			continue
		}

		t.Run("modify_"+tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBatch3Backend(t)
			_, err := b.CreateDBCluster("val1", "aurora-postgresql", "admin", "", "", 0,
				&rds.ServerlessV2ScalingConfiguration{MinCapacity: 1, MaxCapacity: 4}, rds.DBClusterOptions{})
			require.NoError(t, err)

			cfg := tt.cfg

			_, err = b.ModifyDBCluster("val1", "", rds.DBClusterOptions{ServerlessV2Scaling: &cfg})
			if tt.wantErr {
				require.ErrorIs(t, err, rds.ErrInvalidParameter)

				got, derr := b.DescribeDBClusters("val1")
				require.NoError(t, derr)
				assert.InDelta(
					t,
					1,
					got[0].ServerlessV2ScalingConfig.MinCapacity,
					0,
					"rejected modify must not change the range",
				)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestServerlessV2ScalingWire(t *testing.T) {
	t.Parallel()

	form := func(action string, kv ...string) url.Values {
		v := url.Values{"Action": {action}, "Version": {"2014-10-31"}, "DBClusterIdentifier": {"wire1"}}
		for i := 0; i < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}

		return v
	}

	const scaling = "ServerlessV2ScalingConfiguration."

	tests := []struct {
		create     url.Values
		modify     url.Values
		name       string
		wantSecs   string
		wantCreate int
		wantModify int
		wantAbsent bool
	}{
		{
			name: "defaults_to_300",
			create: form(
				"CreateDBCluster",
				"Engine",
				"aurora-postgresql",
				"MasterUsername",
				"u",
				scaling+"MinCapacity",
				"0",
				scaling+"MaxCapacity",
				"4",
			),
			wantCreate: 200, wantSecs: "<SecondsUntilAutoPause>300</SecondsUntilAutoPause>",
		},
		{
			name: "explicit_seconds",
			create: form(
				"CreateDBCluster",
				"Engine",
				"aurora-postgresql",
				"MasterUsername",
				"u",
				scaling+"MinCapacity",
				"0",
				scaling+"MaxCapacity",
				"4",
				scaling+"SecondsUntilAutoPause",
				"900",
			),
			wantCreate: 200, wantSecs: "<SecondsUntilAutoPause>900</SecondsUntilAutoPause>",
		},
		{
			name: "hidden_when_min_nonzero",
			create: form(
				"CreateDBCluster",
				"Engine",
				"aurora-postgresql",
				"MasterUsername",
				"u",
				scaling+"MinCapacity",
				"1",
				scaling+"MaxCapacity",
				"4",
				scaling+"SecondsUntilAutoPause",
				"900",
			),
			wantCreate: 200, wantAbsent: true,
		},
		{
			name: "max_required",
			create: form(
				"CreateDBCluster",
				"Engine",
				"aurora-postgresql",
				"MasterUsername",
				"u",
				scaling+"MinCapacity",
				"0",
			),
			wantCreate: 400,
		},
		{
			name: "bad_seconds",
			create: form(
				"CreateDBCluster",
				"Engine",
				"aurora-postgresql",
				"MasterUsername",
				"u",
				scaling+"MinCapacity",
				"0",
				scaling+"MaxCapacity",
				"4",
				scaling+"SecondsUntilAutoPause",
				"60",
			),
			wantCreate: 400,
		},
		{
			name: "modify_partial_keeps_seconds",
			create: form(
				"CreateDBCluster",
				"Engine",
				"aurora-postgresql",
				"MasterUsername",
				"u",
				scaling+"MinCapacity",
				"0",
				scaling+"MaxCapacity",
				"4",
				scaling+"SecondsUntilAutoPause",
				"900",
			),
			modify:     form("ModifyDBCluster", scaling+"MaxCapacity", "8"),
			wantCreate: 200, wantModify: 200, wantSecs: "<SecondsUntilAutoPause>900</SecondsUntilAutoPause>",
		},
		{
			name: "modify_nonzero_removes_seconds",
			create: form(
				"CreateDBCluster",
				"Engine",
				"aurora-postgresql",
				"MasterUsername",
				"u",
				scaling+"MinCapacity",
				"0",
				scaling+"MaxCapacity",
				"4",
				scaling+"SecondsUntilAutoPause",
				"900",
			),
			modify:     form("ModifyDBCluster", scaling+"MinCapacity", "2"),
			wantCreate: 200, wantModify: 200, wantAbsent: true,
		},
		{
			name: "modify_bad_range",
			create: form(
				"CreateDBCluster",
				"Engine",
				"aurora-postgresql",
				"MasterUsername",
				"u",
				scaling+"MinCapacity",
				"0",
				scaling+"MaxCapacity",
				"4",
			),
			modify:     form("ModifyDBCluster", scaling+"MaxCapacity", "300"),
			wantCreate: 200, wantModify: 400,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newAccuracyRDSHandler(t)

			rec := doAccuracyRDS(t, h, tt.create)
			require.Equal(t, tt.wantCreate, rec.Code, rec.Body.String())

			body := rec.Body.String()

			if tt.modify != nil {
				rec = doAccuracyRDS(t, h, tt.modify)
				require.Equal(t, tt.wantModify, rec.Code, rec.Body.String())

				body = rec.Body.String()
			}

			if tt.wantCreate != 200 || tt.wantModify >= 400 {
				assert.Contains(t, body, "InvalidParameterValue")

				return
			}

			if tt.wantAbsent {
				assert.NotContains(t, body, "SecondsUntilAutoPause")
			} else {
				assert.Contains(t, body, tt.wantSecs)
			}
		})
	}
}

func TestServerlessV2AutoPauseRegionalBackends(t *testing.T) {
	t.Parallel()

	for _, region := range []string{"", "eu-west-1"} {
		t.Run("region_"+region, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				home := rds.NewInMemoryBackend("123456789012", "us-east-1")
				defer home.Close()

				home.SetAutoPause(rds.AutoPauseConfig{IdleScale: 0.5})

				h := rds.NewHandler(home)

				var (
					mu     sync.Mutex
					points int
				)

				h.SetMetricEmitter(cwmetric.EmitterFunc(func(cwmetric.Point) error {
					mu.Lock()
					defer mu.Unlock()

					points++

					return nil
				}))

				bk := h.BackendFor(region)
				if region != "" {
					defer bk.Close()
				}

				assert.InDelta(t, 0.5, bk.AutoPause().IdleScale, 0)

				_, err := bk.CreateDBCluster(pauseClusterID, "aurora-postgresql", "admin", "", "", 0,
					&rds.ServerlessV2ScalingConfiguration{MaxCapacity: 4}, rds.DBClusterOptions{})
				require.NoError(t, err)
				_, err = bk.CreateDBInstance(pauseWriterID, "aurora-postgresql", "db.serverless", "", "admin", "", 0,
					rds.DBInstanceOptions{DBClusterIdentifier: pauseClusterID})
				require.NoError(t, err)

				time.Sleep(200 * time.Second)
				synctest.Wait()

				assert.True(t, bk.IsClusterAutoPaused(pauseClusterID))

				mu.Lock()
				defer mu.Unlock()

				assert.Positive(t, points, "paused instances publish zero capacity metrics")
			})
		})
	}
}
