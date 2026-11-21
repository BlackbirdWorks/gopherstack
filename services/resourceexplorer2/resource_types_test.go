package resourceexplorer2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/resourceexplorer2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/resourceexplorer2"
)

func TestSearchResourceTypeOfARN(t *testing.T) {
	t.Parallel()

	a := testAccount
	src := fakeSource{
		{ARN: "arn:aws:elasticloadbalancing:us-east-1:" + a + ":loadbalancer/net/n1/abc"},
		{ARN: "arn:aws:elasticloadbalancing:us-east-1:" + a + ":loadbalancer/app/a1/def"},
		{ARN: "arn:aws:elasticloadbalancing:us-east-1:" + a + ":loadbalancer/classic1"},
		{ARN: "arn:aws:elasticloadbalancing:us-east-1:" + a + ":targetgroup/tg/123"},
		{ARN: "arn:aws:elasticloadbalancing:us-east-1:" + a + ":listener/net/n1/abc/ghi"},
		{ARN: "arn:aws:rds:us-east-1:" + a + ":db:orders"},
		{ARN: "arn:aws:ecs:us-east-1:" + a + ":service/c/s"},
		{ARN: "arn:aws:logs:us-east-1:" + a + ":log-group:/app"},
	}

	tests := []struct {
		name  string
		query string
		want  string
	}{
		{name: "network", query: "resourcetype:elasticloadbalancing:loadbalancer/net", want: "n1"},
		{name: "application", query: "resourcetype:elasticloadbalancing:loadbalancer/app", want: "a1"},
		{name: "classic", query: "resourcetype:elasticloadbalancing:loadbalancer", want: "classic1"},
		{name: "target group", query: "resourcetype:elasticloadbalancing:targetgroup", want: "tg"},
		{name: "network listener", query: "resourcetype:elasticloadbalancing:listener/net", want: "ghi"},
		{name: "rds", query: "resourcetype:rds:db", want: "orders"},
		{name: "ecs", query: "resourcetype:ecs:service", want: "service/c/s"},
		{name: "logs", query: "resourcetype:logs:log-group", want: "/app"},
	}

	b := resourceexplorer2.NewInMemoryBackend(testAccount, "us-east-1")
	b.SetClock(func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) })
	b.SetResourceSource(src)

	f := &fixture{backend: b, url: sdktest.Serve(t, resourceexplorer2.NewHandler(b))}
	c := f.client(t)
	setupViewed(t, c)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := c.Search(t.Context(), &sdk.SearchInput{QueryString: aws.String(tt.query)})
			require.NoError(t, err)
			require.Len(t, out.Resources, 1)
			assert.Contains(t, aws.ToString(out.Resources[0].Arn), tt.want)
		})
	}
}
