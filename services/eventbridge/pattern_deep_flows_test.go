package eventbridge_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	eventbridgetypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

func TestEventPatternMatrix(t *testing.T) {
	t.Parallel()

	const event = `{"source":"my.app","detail-type":"T","detail":{` +
		`"name":"foobar","n":5,"ip":"10.0.0.5","file":"a.txt",` +
		`"items":[{"id":2},{"id":1}],"tags":["x","y"]}}`

	tests := []struct {
		name    string
		pattern string
		want    bool
	}{
		{name: "prefix", pattern: `{"detail":{"name":[{"prefix":"foo"}]}}`, want: true},
		{name: "prefix miss", pattern: `{"detail":{"name":[{"prefix":"bar"}]}}`},
		{
			name:    "suffix ignore case",
			pattern: `{"detail":{"name":[{"suffix":{"equals-ignore-case":"BAR"}}]}}`,
			want:    true,
		},
		{name: "numeric range", pattern: `{"detail":{"n":[{"numeric":[">",0,"<=",5]}]}}`, want: true},
		{name: "numeric out of range", pattern: `{"detail":{"n":[{"numeric":[">",5]}]}}`},
		{name: "cidr", pattern: `{"detail":{"ip":[{"cidr":"10.0.0.0/24"}]}}`, want: true},
		{name: "wildcard", pattern: `{"detail":{"file":[{"wildcard":"*.txt"}]}}`, want: true},
		{name: "anything but wildcard", pattern: `{"detail":{"file":[{"anything-but":{"wildcard":"*.txt"}}]}}`},
		{name: "or", pattern: `{"$or":[{"detail":{"n":[1]}},{"detail":{"n":[5]}}]}`, want: true},
		{name: "array of objects any element", pattern: `{"detail":{"items":{"id":[1]}}}`, want: true},
		{name: "array of objects no element", pattern: `{"detail":{"items":{"id":[3]}}}`},
		{name: "array of scalars", pattern: `{"detail":{"tags":["y"]}}`, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))

			out, err := client.TestEventPattern(t.Context(), &eventbridgesdk.TestEventPatternInput{
				EventPattern: aws.String(
					tt.pattern,
				), Event: aws.String(`{"id":"1","account":"1","time":"2024-01-01T00:00:00Z",` +
					`"region":"us-east-1",` + event[1:]),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.want, out.Result)
		})
	}
}

func TestEventPatternMatcherValueValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pattern string
		valid   bool
	}{
		{name: "prefix string", pattern: `{"detail":{"x":[{"prefix":"a"}]}}`, valid: true},
		{name: "prefix number", pattern: `{"detail":{"x":[{"prefix":5}]}}`},
		{name: "suffix ignore case", pattern: `{"detail":{"x":[{"suffix":{"equals-ignore-case":"a"}}]}}`, valid: true},
		{name: "suffix bad object", pattern: `{"detail":{"x":[{"suffix":{"bogus":"a"}}]}}`},
		{name: "exists bool", pattern: `{"detail":{"x":[{"exists":true}]}}`, valid: true},
		{name: "exists string", pattern: `{"detail":{"x":[{"exists":"yes"}]}}`},
		{name: "numeric pair", pattern: `{"detail":{"x":[{"numeric":["=",5]}]}}`, valid: true},
		{name: "numeric range", pattern: `{"detail":{"x":[{"numeric":[">=",1,"<",9]}]}}`, valid: true},
		{name: "numeric missing value", pattern: `{"detail":{"x":[{"numeric":[">"]}]}}`},
		{name: "numeric bad operator", pattern: `{"detail":{"x":[{"numeric":["~",1]}]}}`},
		{name: "numeric non number", pattern: `{"detail":{"x":[{"numeric":["=","a"]}]}}`},
		{name: "numeric reversed range", pattern: `{"detail":{"x":[{"numeric":["<",9,">",1]}]}}`},
		{name: "cidr ok", pattern: `{"detail":{"x":[{"cidr":"10.0.0.0/8"}]}}`, valid: true},
		{name: "cidr malformed", pattern: `{"detail":{"x":[{"cidr":"bad"}]}}`},
		{name: "wildcard ok", pattern: `{"detail":{"x":[{"wildcard":"a*b"}]}}`, valid: true},
		{name: "wildcard consecutive", pattern: `{"detail":{"x":[{"wildcard":"a**b"}]}}`},
		{name: "equals ignore case number", pattern: `{"detail":{"x":[{"equals-ignore-case":1}]}}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))

			_, err := client.PutRule(t.Context(), &eventbridgesdk.PutRuleInput{
				Name: aws.String("r"), EventPattern: aws.String(tt.pattern),
			}, func(o *eventbridgesdk.Options) { o.RetryMaxAttempts = 1 })
			if tt.valid {
				require.NoError(t, err)

				return
			}

			var typed *eventbridgetypes.InvalidEventPatternException
			require.ErrorAs(t, err, &typed)
		})
	}
}

func TestDeleteRuleWithTargets(t *testing.T) {
	t.Parallel()

	const queueArn = "arn:aws:sqs:us-east-1:000000000000:q"

	noRetry := func(o *eventbridgesdk.Options) { o.RetryMaxAttempts = 1 }

	tests := []struct {
		call func(t *testing.T, c *eventbridgesdk.Client) error
		name string
	}{
		{
			name: "delete rule with targets",
			call: func(t *testing.T, c *eventbridgesdk.Client) error {
				t.Helper()

				_, err := c.DeleteRule(t.Context(), &eventbridgesdk.DeleteRuleInput{Name: aws.String("r")}, noRetry)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))

			_, err := client.PutRule(t.Context(), &eventbridgesdk.PutRuleInput{
				Name: aws.String("r"), EventPattern: aws.String(`{"source":["app"]}`),
			})
			require.NoError(t, err)

			_, err = client.PutTargets(t.Context(), &eventbridgesdk.PutTargetsInput{
				Rule:    aws.String("r"),
				Targets: []eventbridgetypes.Target{{Id: aws.String("t"), Arn: aws.String(queueArn)}},
			})
			require.NoError(t, err)

			callErr := tt.call(t, client)
			require.Error(t, callErr)
			assert.Contains(t, callErr.Error(), "ValidationException")
		})
	}
}

func TestStartReplayDestinationMustBeArchiveSourceBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		destBus string
		wantErr bool
	}{
		{name: "same bus", destBus: "src"},
		{name: "other bus", destBus: "other", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))
			buses := map[string]string{}

			for _, name := range []string{"src", "other"} {
				out, err := client.CreateEventBus(
					t.Context(),
					&eventbridgesdk.CreateEventBusInput{Name: aws.String(name)},
				)
				require.NoError(t, err)

				buses[name] = aws.ToString(out.EventBusArn)
			}

			arch, err := client.CreateArchive(t.Context(), &eventbridgesdk.CreateArchiveInput{
				ArchiveName: aws.String("a"), EventSourceArn: aws.String(buses["src"]),
			})
			require.NoError(t, err)

			_, err = client.StartReplay(t.Context(), &eventbridgesdk.StartReplayInput{
				ReplayName:     aws.String("rp"),
				EventSourceArn: arch.ArchiveArn,
				Destination:    &eventbridgetypes.ReplayDestination{Arn: aws.String(buses[tt.destBus])},
				EventStartTime: aws.Time(time.Now().Add(-time.Hour)),
				EventEndTime:   aws.Time(time.Now()),
			}, func(o *eventbridgesdk.Options) { o.RetryMaxAttempts = 1 })

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestPutTargetsRejectsMultipleInputSources(t *testing.T) {
	t.Parallel()

	client := newTestEventBridgeClient(t, eventbridge.NewHandler(newBackend()))

	_, err := client.PutRule(t.Context(), &eventbridgesdk.PutRuleInput{
		Name: aws.String("r"), EventPattern: aws.String(`{"source":["app"]}`),
	})
	require.NoError(t, err)

	out, err := client.PutTargets(t.Context(), &eventbridgesdk.PutTargetsInput{
		Rule: aws.String("r"),
		Targets: []eventbridgetypes.Target{{
			Id: aws.String("both"), Arn: aws.String("arn:aws:sqs:us-east-1:000000000000:q"),
			Input: aws.String(`{}`), InputPath: aws.String("$.detail"),
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), out.FailedEntryCount)
}
