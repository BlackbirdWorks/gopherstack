package autoscaling_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	assdk "github.com/aws/aws-sdk-go-v2/service/autoscaling"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/autoscaling"
)

type handlerTransport struct{ h http.Handler }

func (ht handlerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	ht.h.ServeHTTP(rec, r)

	return rec.Result(), nil
}

// newInMemClientWithBackend serves the handler through an in-memory transport so the client can
// run inside a synctest bubble (no real network).
func newInMemClientWithBackend(t *testing.T) (*assdk.Client, *autoscaling.InMemoryBackend) {
	t.Helper()

	backend := autoscaling.NewInMemoryBackend()
	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(autoscaling.NewHandler(backend)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	cfg := aws.Config{
		Region:       "us-east-1",
		BaseEndpoint: aws.String("http://autoscaling.invalid"),
		HTTPClient:   &http.Client{Transport: handlerTransport{h: e}},
		Credentials:  credentials.NewStaticCredentialsProvider("test", "test", ""),
	}

	return assdk.NewFromConfig(cfg), backend
}

func instanceStates(t *testing.T, c *assdk.Client) (int, int) {
	t.Helper()

	out, err := c.DescribeAutoScalingGroups(t.Context(), &assdk.DescribeAutoScalingGroupsInput{})
	require.NoError(t, err)
	require.Len(t, out.AutoScalingGroups, 1)

	var inService, pending int

	for _, inst := range out.AutoScalingGroups[0].Instances {
		switch string(inst.LifecycleState) {
		case "InService":
			inService++
		case "Pending":
			pending++
		}
	}

	return inService, pending
}

func inProgressLaunches(t *testing.T, c *assdk.Client) int {
	t.Helper()

	out, err := c.DescribeScalingActivities(t.Context(), &assdk.DescribeScalingActivitiesInput{})
	require.NoError(t, err)

	n := 0
	for _, a := range out.Activities {
		if string(a.StatusCode) == "InProgress" && strings.HasPrefix(aws.ToString(a.Description), "Launching") {
			n++
		}
	}

	return n
}

func TestLaunchDelay_PendingThenInService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		delay         time.Duration
		wantPending   int
		wantInService int
	}{
		{name: "delayed", delay: time.Minute, wantPending: 3},
		{name: "instant", delay: 0, wantInService: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				c, backend := newInMemClientWithBackend(t)
				backend.SetLaunchDelay(tt.delay)
				seedGroup(t, c, "g", 3, 5)

				inService, pending := instanceStates(t, c)
				assert.Equal(t, tt.wantPending, pending)
				assert.Equal(t, tt.wantInService, inService)

				if tt.delay == 0 {
					assert.Zero(t, inProgressLaunches(t, c))

					return
				}

				assert.Positive(t, inProgressLaunches(t, c))

				_, err := c.SetDesiredCapacity(t.Context(), &assdk.SetDesiredCapacityInput{
					AutoScalingGroupName: aws.String("g"), DesiredCapacity: aws.Int32(5),
				})
				require.NoError(t, err)

				time.Sleep(tt.delay)
				synctest.Wait()

				inSvc, pend := instanceStates(t, c)
				assert.Equal(t, 5, inSvc)
				assert.Zero(t, pend)
				assert.Zero(t, inProgressLaunches(t, c))
			})
		})
	}
}
