package cloudwatchlogs_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwlsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

func newSeededLogsClient(t *testing.T) *cwlsdk.Client {
	t.Helper()

	c := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))

	for _, g := range []string{"/app/DataLogs", "/app/web", "/svc/DataLogs-b"} {
		_, err := c.CreateLogGroup(t.Context(), &cwlsdk.CreateLogGroupInput{LogGroupName: aws.String(g)})
		require.NoError(t, err)
	}

	_, err := c.PutRetentionPolicy(t.Context(), &cwlsdk.PutRetentionPolicyInput{
		LogGroupName: aws.String("/app/DataLogs"), RetentionInDays: aws.Int32(7),
	})
	require.NoError(t, err)

	_, err = c.CreateLogStream(t.Context(), &cwlsdk.CreateLogStreamInput{
		LogGroupName: aws.String("/app/web"), LogStreamName: aws.String("s1"),
	})
	require.NoError(t, err)
	_, err = c.PutLogEvents(t.Context(), &cwlsdk.PutLogEventsInput{
		LogGroupName: aws.String("/app/web"), LogStreamName: aws.String("s1"),
		LogEvents: []cwltypes.InputLogEvent{
			{Message: aws.String("hello"), Timestamp: aws.Int64(time.Now().UnixMilli())},
		},
	})
	require.NoError(t, err)

	return c
}

func TestDescribeLogGroups_PatternAndIdentifiers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      cwlsdk.DescribeLogGroupsInput
		want    []string
		wantErr bool
	}{
		{name: "pattern_substring", in: cwlsdk.DescribeLogGroupsInput{LogGroupNamePattern: aws.String("DataLogs")},
			want: []string{"/app/DataLogs", "/svc/DataLogs-b"}},
		{name: "pattern_case_sensitive", in: cwlsdk.DescribeLogGroupsInput{LogGroupNamePattern: aws.String("datalogs")},
			want: []string{}},
		{name: "identifiers_name_and_arn", in: cwlsdk.DescribeLogGroupsInput{
			LogGroupIdentifiers: []string{
				"/app/web",
				"arn:aws:logs:us-east-1:123456789012:log-group:/svc/DataLogs-b:*",
			},
		}, want: []string{"/app/web", "/svc/DataLogs-b"}},
		{name: "prefix_and_pattern_exclusive", in: cwlsdk.DescribeLogGroupsInput{
			LogGroupNamePrefix: aws.String("/app"), LogGroupNamePattern: aws.String("web"),
		}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newSeededLogsClient(t)
			out, err := c.DescribeLogGroups(t.Context(), &tt.in)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			got := make([]string, 0)
			for _, g := range out.LogGroups {
				got = append(got, aws.ToString(g.LogGroupName))
				if tt.in.LogGroupNamePattern != nil {
					assert.NotEmpty(t, aws.ToString(g.Arn))
					assert.Nil(t, g.RetentionInDays)
				}
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestLogGroupIdentifier_AcceptedOnStreamAndEventReads(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:logs:us-east-1:123456789012:log-group:/app/web:*"

	tests := []struct {
		call    func(t *testing.T, c *cwlsdk.Client) (int, error)
		name    string
		wantLen int
		wantErr bool
	}{
		{name: "describe_streams_arn", wantLen: 1, call: func(t *testing.T, c *cwlsdk.Client) (int, error) {
			t.Helper()

			out, err := c.DescribeLogStreams(
				t.Context(),
				&cwlsdk.DescribeLogStreamsInput{LogGroupIdentifier: aws.String(arn)},
			)
			if err != nil {
				return 0, err
			}

			return len(out.LogStreams), nil
		}},
		{name: "get_events_name_identifier", wantLen: 1, call: func(t *testing.T, c *cwlsdk.Client) (int, error) {
			t.Helper()

			out, err := c.GetLogEvents(t.Context(), &cwlsdk.GetLogEventsInput{
				LogGroupIdentifier: aws.String("/app/web"), LogStreamName: aws.String("s1"),
			})
			if err != nil {
				return 0, err
			}

			return len(out.Events), nil
		}},
		{name: "filter_events_arn", wantLen: 1, call: func(t *testing.T, c *cwlsdk.Client) (int, error) {
			t.Helper()

			out, err := c.FilterLogEvents(
				t.Context(),
				&cwlsdk.FilterLogEventsInput{LogGroupIdentifier: aws.String(arn)},
			)
			if err != nil {
				return 0, err
			}

			return len(out.Events), nil
		}},
		{
			name:    "both_name_and_identifier_rejected",
			wantErr: true,
			call: func(t *testing.T, c *cwlsdk.Client) (int, error) {
				t.Helper()

				_, err := c.DescribeLogStreams(t.Context(), &cwlsdk.DescribeLogStreamsInput{
					LogGroupName: aws.String("/app/web"), LogGroupIdentifier: aws.String(arn),
				})

				return 0, err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n, err := tt.call(t, newSeededLogsClient(t))

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantLen, n)
		})
	}
}

func TestPutDestination_AppliesTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags map[string]string
		name string
	}{
		{name: "tags_applied", tags: map[string]string{"env": "dev", "team": "a"}},
		{name: "no_tags", tags: map[string]string{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
			out, err := c.PutDestination(t.Context(), &cwlsdk.PutDestinationInput{
				DestinationName: aws.String("d"),
				TargetArn:       aws.String("arn:aws:kinesis:us-east-1:123456789012:stream/s"),
				RoleArn:         aws.String("arn:aws:iam::123456789012:role/r"),
				Tags:            tt.tags,
			})
			require.NoError(t, err)

			got, err := c.ListTagsForResource(t.Context(), &cwlsdk.ListTagsForResourceInput{
				ResourceArn: out.Destination.Arn,
			})
			require.NoError(t, err)
			assert.Equal(t, tt.tags, got.Tags)
		})
	}
}

func TestQueryDefinition_QueryLanguageRoundTripAndFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		filter   cwltypes.QueryLanguage
		wantName []string
	}{
		{name: "no_filter", wantName: []string{"cw", "pp", "sq"}},
		{name: "sql", filter: cwltypes.QueryLanguageSql, wantName: []string{"sq"}},
		{name: "ppl", filter: cwltypes.QueryLanguagePpl, wantName: []string{"pp"}},
		{name: "default_cwli", filter: cwltypes.QueryLanguageCwli, wantName: []string{"cw"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))

			for name, lang := range map[string]cwltypes.QueryLanguage{
				"cw": "", "sq": cwltypes.QueryLanguageSql, "pp": cwltypes.QueryLanguagePpl,
			} {
				_, err := c.PutQueryDefinition(t.Context(), &cwlsdk.PutQueryDefinitionInput{
					Name: aws.String(name), QueryString: aws.String("fields @message"), QueryLanguage: lang,
				})
				require.NoError(t, err)
			}

			out, err := c.DescribeQueryDefinitions(t.Context(), &cwlsdk.DescribeQueryDefinitionsInput{
				QueryLanguage: tt.filter,
			})
			require.NoError(t, err)

			got := make([]string, 0)
			for _, d := range out.QueryDefinitions {
				got = append(got, aws.ToString(d.Name))
			}

			assert.Equal(t, tt.wantName, got)
		})
	}

	t.Run("invalid_language", func(t *testing.T) {
		t.Parallel()

		c := newTestCloudWatchLogsClient(t, cloudwatchlogs.NewHandler(cloudwatchlogs.NewInMemoryBackend()))
		_, err := c.PutQueryDefinition(t.Context(), &cwlsdk.PutQueryDefinitionInput{
			Name: aws.String("x"), QueryString: aws.String("q"), QueryLanguage: "KQL",
		})
		require.Error(t, err)
	})
}
