package gcpfunctions_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/functions/apiv2/functionspb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServiceConfig_ResourceValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		memory  string
		cpu     string
		wantErr bool
	}{
		{"defaults", "", "", false},
		{"256M with fractional cpu", "256M", "0.1666", false},
		{"1Gi with 0.5", "1Gi", "0.5", false},
		{"2Gi with 0.5", "2Gi", "0.5", true},
		{"1Gi with 1", "1Gi", "1", false},
		{"8Gi with 1", "8Gi", "1", true},
		{"8Gi with 2", "8Gi", "2", false},
		{"1Gi with 4", "1Gi", "4", true},
		{"2Gi with 4", "2Gi", "4", false},
		{"32Gi with 8", "32Gi", "8", false},
		{"32Gi with 6", "32Gi", "6", true},
		{"64Gi", "64Gi", "", true},
		{"64Mi", "64Mi", "", true},
		{"cpu 3", "4Gi", "3", true},
		{"cpu above 8", "32Gi", "16", true},
		{"cpu below 0.08", "128Mi", "0.01", true},
		{"cpu not a number", "", "lots", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			spec := fnSpec(e.upload(t, zipOf(t, nodeFilesFn())))
			spec.GetServiceConfig().AvailableMemory, spec.GetServiceConfig().AvailableCpu = tt.memory, tt.cpu

			err := e.createErr(t, "res", spec)
			if tt.wantErr {
				assert.Equal(t, codes.InvalidArgument, status.Code(err), "%v", err)

				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestServiceConfig_DerivedCPU(t *testing.T) {
	t.Parallel()

	tests := []struct{ memory, want string }{
		{"", "0.1666"}, {"128Mi", "0.083"}, {"512Mi", "0.333"}, {"1Gi", "0.583"}, {"2Gi", "1"},
		{"4Gi", "2"}, {"16Gi", "4"}, {"32Gi", "8"},
	}

	for _, tt := range tests {
		t.Run("memory "+tt.memory, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			spec := fnSpec(e.upload(t, zipOf(t, nodeFilesFn())))
			spec.GetServiceConfig().AvailableMemory = tt.memory

			fn := e.createFunction(t, "cpu", spec)
			assert.Equal(t, tt.want, fn.GetServiceConfig().GetAvailableCpu())
		})
	}
}

func scaled(t *testing.T, e *env, id string, mutate func(sc *functionspb.ServiceConfig)) *functionspb.Function {
	t.Helper()

	spec := fnSpec(e.upload(t, zipOf(t, nodeFilesFn())))
	mutate(spec.GetServiceConfig())

	return e.createFunction(t, id, spec)
}

// callAsync issues a request in the background and reports its status.
func (e *env) callAsync(t *testing.T, host string) <-chan int {
	t.Helper()

	out := make(chan int, 1)

	go func() {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			"http://"+e.handler.IngressAddr()+"/", nil)
		if err != nil {
			out <- 0

			return
		}

		req.Host = host

		resp, err := e.http.Do(req)
		if err != nil {
			out <- 0

			return
		}

		_ = resp.Body.Close()
		out <- resp.StatusCode
	}()

	return out
}

func waitRunning(t *testing.T, e *env, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return e.rt.running() == n }, 10*time.Second, 5*time.Millisecond)
}

func TestScaling_ConcurrencyAndMaxInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		concurrency int32
		max         int32
		requests    int
		wantRunning int
	}{
		{"one request per instance scales out", 1, 5, 3, 3},
		{"concurrency shares an instance", 3, 5, 3, 1},
		{"concurrency spills to a second instance", 2, 5, 3, 2},
		{"max instances caps scale-out", 1, 2, 2, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			fn := scaled(t, e, "scale", func(sc *functionspb.ServiceConfig) {
				sc.MaxInstanceRequestConcurrency, sc.MaxInstanceCount = tt.concurrency, tt.max
				sc.AvailableCpu, sc.AvailableMemory = "1", "512Mi"
			})
			release := e.rt.holdRequests()

			t.Cleanup(release)

			results := make([]<-chan int, 0, tt.requests)
			for range tt.requests {
				results = append(results, e.callAsync(t, hostOf(fn)))
			}

			waitRunning(t, e, tt.wantRunning)
			assert.Equal(t, tt.wantRunning, e.rt.running())
			release()

			for _, r := range results {
				assert.Equal(t, http.StatusOK, <-r)
			}
		})
	}
}

func TestScaling_QueueTimeoutIs429(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	clk := newFakeClock()
	e.handler.Backend.SetClock(clk)

	fn := scaled(t, e, "full", func(sc *functionspb.ServiceConfig) {
		sc.MaxInstanceRequestConcurrency, sc.MaxInstanceCount = 1, 1
	})
	release := e.rt.holdRequests()

	t.Cleanup(release)

	first := e.callAsync(t, hostOf(fn))
	require.Eventually(t, func() bool { return len(e.rt.requests()) == 1 }, 10*time.Second, 5*time.Millisecond)

	second := e.callAsync(t, hostOf(fn))
	clk.waitNew(t)
	clk.advance(10 * time.Second)

	assert.Equal(t, http.StatusTooManyRequests, <-second)
	assert.Equal(t, 1, e.rt.running())

	release()
	assert.Equal(t, http.StatusOK, <-first)
}

func TestScaling_MinInstancesAndIdleScaleDown(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	clk := newFakeClock()
	e.handler.Backend.SetClock(clk)
	e.handler.Backend.SetIdleTimeout(time.Minute)

	fn := scaled(t, e, "warm", func(sc *functionspb.ServiceConfig) {
		sc.MinInstanceCount, sc.MaxInstanceCount, sc.MaxInstanceRequestConcurrency = 2, 4, 1
	})
	assert.Equal(t, 2, e.rt.running(), "minimum instances start at deploy")

	release := e.rt.holdRequests()

	t.Cleanup(release)

	var wg sync.WaitGroup

	results := make([]<-chan int, 0, 3)
	for range 3 {
		results = append(results, e.callAsync(t, hostOf(fn)))
	}

	waitRunning(t, e, 3)
	release()

	for _, r := range results {
		assert.Equal(t, http.StatusOK, <-r)
	}

	wg.Wait()
	clk.waitTimers(t, 3)
	clk.advance(2 * time.Minute)
	waitRunning(t, e, 2)
}

func TestScaling_RequestTimeoutIs504(t *testing.T) {
	t.Parallel()

	e := newEnv(t, true)
	fn := scaled(t, e, "slow", func(sc *functionspb.ServiceConfig) { sc.TimeoutSeconds = 1 })
	release := e.rt.holdRequests()

	t.Cleanup(release)

	began := time.Now()
	code, _ := e.call(t, http.MethodGet, hostOf(fn), "/", "")
	assert.Equal(t, http.StatusGatewayTimeout, code)
	assert.Less(t, time.Since(began), 20*time.Second, "timeout_seconds=1 must cut the request, not the 60s default")
}

func TestIngressSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		headers map[string]string
		name    string
		want    int
		setting functionspb.ServiceConfig_IngressSettings
	}{
		{nil, "allow all external", http.StatusOK, functionspb.ServiceConfig_ALLOW_ALL},
		{nil, "unspecified allows external", http.StatusOK, functionspb.ServiceConfig_INGRESS_SETTINGS_UNSPECIFIED},
		{nil, "internal only blocks external", http.StatusNotFound, functionspb.ServiceConfig_ALLOW_INTERNAL_ONLY},
		{
			map[string]string{"Via": "1.1 google"},
			"internal only blocks gclb",
			http.StatusNotFound,
			functionspb.ServiceConfig_ALLOW_INTERNAL_ONLY,
		},
		{
			map[string]string{"X-Gopherstack-Network": "internal"},
			"internal only allows internal",
			http.StatusOK,
			functionspb.ServiceConfig_ALLOW_INTERNAL_ONLY,
		},
		{nil, "gclb blocks external", http.StatusNotFound, functionspb.ServiceConfig_ALLOW_INTERNAL_AND_GCLB},
		{
			map[string]string{"Via": "1.1 google"},
			"gclb allows load balancer",
			http.StatusOK,
			functionspb.ServiceConfig_ALLOW_INTERNAL_AND_GCLB,
		},
		{
			map[string]string{"X-Gopherstack-Network": "internal"},
			"gclb allows internal",
			http.StatusOK,
			functionspb.ServiceConfig_ALLOW_INTERNAL_AND_GCLB,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, true)
			fn := scaled(t, e, "ing", func(sc *functionspb.ServiceConfig) { sc.IngressSettings = tt.setting })

			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet,
				"http://"+e.handler.IngressAddr()+"/", nil)
			require.NoError(t, err)

			req.Host = hostOf(fn)

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			resp, err := e.http.Do(req)
			require.NoError(t, err)

			_ = resp.Body.Close()
			assert.Equal(t, tt.want, resp.StatusCode)
		})
	}
}
