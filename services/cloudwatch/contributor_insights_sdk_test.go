package cloudwatch_test

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	cwsdk "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudwatch"
	"github.com/blackbirdworks/gopherstack/services/cloudwatchlogs"
)

const insightPeriod = 60

type insightLogs struct {
	b *cloudwatchlogs.InMemoryBackend
}

func (l insightLogs) InsightEvents(
	region string, patterns []string, start, end time.Time,
) []cloudwatch.InsightLogEvent {
	evs := l.b.ContributorEvents(region, patterns, start.UnixMilli(), end.UnixMilli())
	out := make([]cloudwatch.InsightLogEvent, 0, len(evs))

	for _, ev := range evs {
		out = append(
			out,
			cloudwatch.InsightLogEvent{Timestamp: time.UnixMilli(ev.Timestamp).UTC(), Message: ev.Message},
		)
	}

	return out
}

type insightEvent struct {
	msg    string
	offset time.Duration
}

type insightEnv struct {
	client *cwsdk.Client
	logs   *cloudwatchlogs.InMemoryBackend
	start  time.Time
}

func newInsightEnv(t *testing.T) *insightEnv {
	t.Helper()

	client, bk := newTestHandlerAndClientWithBackend(t)
	logs := cloudwatchlogs.NewInMemoryBackend()
	bk.SetLogEventSource(insightLogs{b: logs})

	return &insightEnv{client: client, logs: logs, start: time.Now().UTC().Add(-30 * time.Minute).Truncate(time.Minute)}
}

func (e *insightEnv) putEvents(t *testing.T, group string, evs []insightEvent) {
	t.Helper()

	ctx := context.Background()
	_, err := e.logs.CreateLogGroup(ctx, group, "", "")
	require.NoError(t, err)
	_, err = e.logs.CreateLogStream(ctx, group, "s1")
	require.NoError(t, err)

	in := make([]cloudwatchlogs.InputLogEvent, 0, len(evs))
	for _, ev := range evs {
		in = append(in, cloudwatchlogs.InputLogEvent{Message: ev.msg, Timestamp: e.start.Add(ev.offset).UnixMilli()})
	}

	slices.SortStableFunc(
		in,
		func(a, b cloudwatchlogs.InputLogEvent) int { return cmp.Compare(a.Timestamp, b.Timestamp) },
	)

	_, err = e.logs.PutLogEvents(ctx, group, "s1", "", in)
	require.NoError(t, err)
}

func (e *insightEnv) report(
	t *testing.T,
	rule string,
	mutate func(*cwsdk.GetInsightRuleReportInput),
) *cwsdk.GetInsightRuleReportOutput {
	t.Helper()

	in := &cwsdk.GetInsightRuleReportInput{
		RuleName:  aws.String(rule),
		StartTime: aws.Time(e.start),
		EndTime:   aws.Time(e.start.Add(10 * time.Minute)),
		Period:    aws.Int32(insightPeriod),
	}
	if mutate != nil {
		mutate(in)
	}

	out, err := e.client.GetInsightRuleReport(t.Context(), in)
	require.NoError(t, err)

	return out
}

func (e *insightEnv) putRule(t *testing.T, name, def string, state *string) error {
	t.Helper()

	_, err := e.client.PutInsightRule(t.Context(), &cwsdk.PutInsightRuleInput{
		RuleName: aws.String(name), RuleDefinition: aws.String(def), RuleState: state,
	})

	return err
}

func jsonRule(keys string, extra string) string {
	return `{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogGroupNames":["app-*"],"LogFormat":"JSON",` +
		`"Contribution":{"Keys":[` + keys + `]` + extra + `}`
}

func contributorKeys(out *cwsdk.GetInsightRuleReportOutput) [][]string {
	keys := make([][]string, 0, len(out.Contributors))
	for _, c := range out.Contributors {
		keys = append(keys, c.Keys)
	}

	return keys
}

func ipEvents(ips ...string) []insightEvent {
	evs := make([]insightEvent, 0, len(ips))
	for i, ip := range ips {
		evs = append(evs, insightEvent{msg: fmt.Sprintf(`{"ip":%q}`, ip), offset: time.Duration(i) * time.Second})
	}

	return evs
}

func TestInsightRule_JSONCountRanking(t *testing.T) {
	t.Parallel()

	env := newInsightEnv(t)
	env.putEvents(
		t,
		"app-1",
		append(ipEvents("a", "b", "a", "c", "a", "b"), insightEvent{msg: "plain text", offset: time.Minute}),
	)
	env.putEvents(t, "web-1", ipEvents("zzz", "zzz"))
	require.NoError(t, env.putRule(t, "r", jsonRule(`"$.ip"`, "")+`,"AggregateOn":"Count"}`, nil))

	out := env.report(t, "r", nil)

	assert.Equal(t, [][]string{{"a"}, {"b"}, {"c"}}, contributorKeys(out))
	assert.InDelta(t, 3, aws.ToFloat64(out.Contributors[0].ApproximateAggregateValue), 0)
	assert.InDelta(t, 6, aws.ToFloat64(out.AggregateValue), 0)
	assert.EqualValues(t, 3, aws.ToInt64(out.ApproximateUniqueCount))
	assert.Equal(t, "COUNT", aws.ToString(out.AggregationStatistic))
	assert.Equal(t, []string{"$.ip"}, out.KeyLabels)
}

func TestInsightRule_SumWithFilterAndMultipleKeys(t *testing.T) {
	t.Parallel()

	env := newInsightEnv(t)
	env.putEvents(t, "app-1", []insightEvent{
		{msg: `{"ip":"a","m":"PUT","bytes":100,"req":{"id":"x"}}`},
		{msg: `{"ip":"a","m":"PUT","bytes":50,"req":{"id":"x"}}`, offset: time.Second},
		{msg: `{"ip":"a","m":"GET","bytes":999,"req":{"id":"x"}}`, offset: 2 * time.Second},
		{msg: `{"ip":"b","m":"PUT","bytes":10,"req":{"id":"y"}}`, offset: 3 * time.Second},
		{msg: `{"ip":"b","m":"PUT","bytes":"nan","req":{"id":"y"}}`, offset: 4 * time.Second},
	})
	def := jsonRule(`"$.ip","$.req.id"`, `,"ValueOf":"$.bytes","Filters":[{"Match":"$.m","In":["PUT"]}]`) +
		`,"AggregateOn":"Sum"}`
	require.NoError(t, env.putRule(t, "r", def, nil))

	out := env.report(t, "r", nil)

	assert.Equal(t, [][]string{{"a", "x"}, {"b", "y"}}, contributorKeys(out))
	assert.InDelta(t, 150, aws.ToFloat64(out.Contributors[0].ApproximateAggregateValue), 0)
	assert.InDelta(t, 160, aws.ToFloat64(out.AggregateValue), 0)
	assert.Equal(t, "SUM", aws.ToString(out.AggregationStatistic))
	assert.Equal(t, []string{"$.ip", "$.req.id"}, out.KeyLabels)
}

func TestInsightRule_FilterOperators(t *testing.T) {
	t.Parallel()

	events := []insightEvent{
		{msg: `{"ip":"a","status":200,"path":"/api/x"}`},
		{msg: `{"ip":"b","status":404,"path":"/api/y"}`, offset: time.Second},
		{msg: `{"ip":"c","status":500,"path":"/home"}`, offset: 2 * time.Second},
		{msg: `{"ip":"d","path":"/home"}`, offset: 3 * time.Second},
	}

	tests := []struct {
		name   string
		filter string
		want   []string
	}{
		{"in", `{"Match":"$.path","In":["/home"]}`, []string{"c", "d"}},
		{"notin", `{"Match":"$.path","NotIn":["/home"]}`, []string{"a", "b"}},
		{"startswith", `{"Match":"$.path","StartsWith":["/api"]}`, []string{"a", "b"}},
		{"notstartswith", `{"Match":"$.path","NotStartsWith":["/api"]}`, []string{"c", "d"}},
		{"greaterthan", `{"Match":"$.status","GreaterThan":300}`, []string{"b", "c"}},
		{"lessthan", `{"Match":"$.status","LessThan":300}`, []string{"a"}},
		{"equalto", `{"Match":"$.status","EqualTo":404}`, []string{"b"}},
		{"notequalto", `{"Match":"$.status","NotEqualTo":404}`, []string{"a", "c"}},
		{"ispresent true", `{"Match":"$.status","IsPresent":true}`, []string{"a", "b", "c"}},
		{"ispresent false", `{"Match":"$.status","IsPresent":false}`, []string{"d"}},
		{"anded", `{"Match":"$.path","In":["/home"]},{"Match":"$.status","IsPresent":true}`, []string{"c"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newInsightEnv(t)
			env.putEvents(t, "app-1", events)
			require.NoError(t, env.putRule(t, "r", jsonRule(`"$.ip"`, `,"Filters":[`+tc.filter+`]`)+`}`, nil))

			var got []string
			for _, k := range contributorKeys(env.report(t, "r", nil)) {
				got = append(got, k...)
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestInsightRule_CLFFields(t *testing.T) {
	t.Parallel()

	env := newInsightEnv(t)
	env.putEvents(t, "app-1", []insightEvent{
		{msg: `10.0.0.1 - - [10/Oct/2026:13:55:36 +0000] "GET /a HTTP/1.1" 200 512`},
		{msg: `10.0.0.1 - - [10/Oct/2026:13:55:37 +0000] "GET /b HTTP/1.1" 200 100`, offset: time.Second},
		{msg: `10.0.0.2 - - [10/Oct/2026:13:55:38 +0000] "GET /c HTTP/1.1" 500 900`, offset: 2 * time.Second},
	})
	def := `{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogGroupNames":["app-1"],"LogFormat":"CLF",` +
		`"Fields":{"1":"Ip","6":"Status"},"Contribution":{"Keys":["Ip"],"ValueOf":"7",` +
		`"Filters":[{"Match":"Status","EqualTo":200}]},"AggregateOn":"Sum"}`
	require.NoError(t, env.putRule(t, "clf", def, nil))

	out := env.report(t, "clf", nil)

	assert.Equal(t, [][]string{{"10.0.0.1"}}, contributorKeys(out))
	assert.InDelta(t, 612, aws.ToFloat64(out.AggregateValue), 0)
}

func TestInsightRule_OrderByMaxContributorsAndDatapoints(t *testing.T) {
	t.Parallel()

	minute := time.Minute
	events := []insightEvent{
		{msg: `{"ip":"a"}`}, {msg: `{"ip":"a"}`, offset: minute}, {msg: `{"ip":"a"}`, offset: 2 * minute},
		{msg: `{"ip":"b"}`, offset: minute}, {msg: `{"ip":"b"}`, offset: minute + time.Second},
		{msg: `{"ip":"c"}`, offset: 5 * minute},
	}

	tests := []struct {
		name     string
		orderBy  string
		want     [][]string
		maxCount int32
	}{
		{name: "sum default", orderBy: "", maxCount: 0, want: [][]string{{"a"}, {"b"}, {"c"}}},
		{name: "sum", orderBy: "Sum", maxCount: 2, want: [][]string{{"a"}, {"b"}}},
		{name: "maximum", orderBy: "Maximum", maxCount: 0, want: [][]string{{"b"}, {"a"}, {"c"}}},
		{name: "maximum capped", orderBy: "Maximum", maxCount: 1, want: [][]string{{"b"}}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newInsightEnv(t)
			env.putEvents(t, "app-1", events)
			require.NoError(t, env.putRule(t, "r", jsonRule(`"$.ip"`, "")+`}`, nil))

			out := env.report(t, "r", func(in *cwsdk.GetInsightRuleReportInput) {
				if tc.orderBy != "" {
					in.OrderBy = aws.String(tc.orderBy)
				}

				if tc.maxCount > 0 {
					in.MaxContributorCount = aws.Int32(tc.maxCount)
				}
			})

			assert.Equal(t, tc.want, contributorKeys(out))
			assert.EqualValues(t, 3, aws.ToInt64(out.ApproximateUniqueCount))
		})
	}

	env := newInsightEnv(t)
	env.putEvents(t, "app-1", events)
	require.NoError(t, env.putRule(t, "r", jsonRule(`"$.ip"`, "")+`}`, nil))

	out := env.report(t, "r", nil)
	require.Len(t, out.Contributors[0].Datapoints, 3)
	assert.Equal(t, env.start.Add(2*minute), aws.ToTime(out.Contributors[0].Datapoints[2].Timestamp).UTC())
}

func TestInsightRule_MetricDatapoints(t *testing.T) {
	t.Parallel()

	env := newInsightEnv(t)
	env.putEvents(t, "app-1", []insightEvent{
		{msg: `{"ip":"a","b":10}`}, {msg: `{"ip":"a","b":30}`, offset: time.Second},
		{msg: `{"ip":"b","b":5}`, offset: 2 * time.Second}, {msg: `{"ip":"b","b":7}`, offset: time.Minute},
	})
	def := jsonRule(`"$.ip"`, `,"ValueOf":"$.b"`) + `,"AggregateOn":"Sum"}`
	require.NoError(t, env.putRule(t, "r", def, nil))

	out := env.report(t, "r", func(in *cwsdk.GetInsightRuleReportInput) {
		in.Metrics = []string{
			"UniqueContributors", "MaxContributorValue", "SampleCount", "Sum", "Minimum", "Maximum", "Average",
		}
	})

	require.Len(t, out.MetricDatapoints, 2)

	dp := out.MetricDatapoints[0]
	assert.Equal(t, env.start, aws.ToTime(dp.Timestamp).UTC())
	assert.InDelta(t, 2, aws.ToFloat64(dp.UniqueContributors), 0)
	assert.InDelta(t, 40, aws.ToFloat64(dp.MaxContributorValue), 0)
	assert.InDelta(t, 3, aws.ToFloat64(dp.SampleCount), 0)
	assert.InDelta(t, 45, aws.ToFloat64(dp.Sum), 0)
	assert.InDelta(t, 5, aws.ToFloat64(dp.Minimum), 0)
	assert.InDelta(t, 30, aws.ToFloat64(dp.Maximum), 0)
	assert.InDelta(t, 15, aws.ToFloat64(dp.Average), 0)

	only := env.report(t, "r", func(in *cwsdk.GetInsightRuleReportInput) { in.Metrics = []string{"Sum"} })
	assert.NotNil(t, only.MetricDatapoints[0].Sum)
	assert.Nil(t, only.MetricDatapoints[0].SampleCount)
}

func TestInsightRule_StateAndTimeWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state     *string
		name      string
		window    time.Duration
		wantCount int
	}{
		{name: "enabled", state: aws.String("ENABLED"), window: 10 * time.Minute, wantCount: 1},
		{name: "disabled", state: aws.String("DISABLED"), window: 10 * time.Minute, wantCount: 0},
		{name: "outside window", state: nil, window: time.Second, wantCount: 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newInsightEnv(t)
			env.putEvents(t, "app-1", ipEvents("a"))
			require.NoError(t, env.putRule(t, "r", jsonRule(`"$.ip"`, "")+`}`, tc.state))

			out := env.report(t, "r", func(in *cwsdk.GetInsightRuleReportInput) {
				in.StartTime = aws.Time(env.start.Add(time.Second))
				in.EndTime = aws.Time(env.start.Add(time.Second + tc.window))
				if tc.window == 10*time.Minute {
					in.StartTime = aws.Time(env.start)
				}
			})

			assert.Len(t, out.Contributors, tc.wantCount)
			assert.Equal(t, []string{"$.ip"}, out.KeyLabels)
		})
	}
}

func TestInsightRule_ReportErrors(t *testing.T) {
	t.Parallel()

	env := newInsightEnv(t)
	require.NoError(t, env.putRule(t, "r", jsonRule(`"$.ip"`, "")+`}`, nil))

	type input = cwsdk.GetInsightRuleReportInput

	tests := []struct {
		mutate   func(*input)
		name     string
		notFound bool
	}{
		{name: "unknown rule", notFound: true, mutate: func(in *input) { in.RuleName = aws.String("nope") }},
		{name: "start after end", mutate: func(in *input) { in.StartTime, in.EndTime = in.EndTime, in.StartTime }},
		{name: "bad order by", mutate: func(in *input) { in.OrderBy = aws.String("Average") }},
		{name: "bad max count", mutate: func(in *input) { in.MaxContributorCount = aws.Int32(101) }},
		{name: "bad metric", mutate: func(in *input) { in.Metrics = []string{"P99"} }},
		{name: "negative period", mutate: func(in *input) { in.Period = aws.Int32(-1) }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &input{
				RuleName: aws.String("r"), StartTime: aws.Time(env.start),
				EndTime: aws.Time(env.start.Add(time.Hour)), Period: aws.Int32(insightPeriod),
			}
			tc.mutate(in)

			_, err := env.client.GetInsightRuleReport(t.Context(), in)
			require.Error(t, err)

			if tc.notFound {
				var nf *cwtypes.ResourceNotFoundException
				assert.ErrorAs(t, err, &nf)

				return
			}

			var bad *cwtypes.InvalidParameterValueException
			assert.ErrorAs(t, err, &bad)
		})
	}
}

func TestInsightRule_PutValidation(t *testing.T) {
	t.Parallel()

	manyKeys := jsonRule(`"$.a","$.b","$.c","$.d","$.e"`, "") + `}`
	fiveFilters := jsonRule(`"$.a"`, `,"Filters":[`+
		`{"Match":"$.a","IsPresent":true},{"Match":"$.b","IsPresent":true},{"Match":"$.c","IsPresent":true},`+
		`{"Match":"$.d","IsPresent":true},{"Match":"$.e","IsPresent":true}]`) + `}`
	elevenValues, err := json.Marshal([]string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"})
	require.NoError(t, err)

	invalid := &cwtypes.InvalidParameterValueException{}
	limit := &cwtypes.LimitExceededException{}

	type putCase struct {
		want  any
		state *string
		name  string
		def   string
	}

	c := func(name, def string, state *string, want any) putCase {
		return putCase{name: name, def: def, state: state, want: want}
	}

	tests := []putCase{
		c("too many keys", manyKeys, nil, limit),
		c("too many filters", fiveFilters, nil, limit),
		c(
			"too many filter values",
			jsonRule(`"$.a"`, `,"Filters":[{"Match":"$.a","In":`+string(elevenValues)+`}]`)+`}`,
			nil,
			limit,
		),
		c("bad json path", jsonRule(`"ip"`, "")+`}`, nil, invalid),
		c("two operators", jsonRule(`"$.a"`, `,"Filters":[{"Match":"$.a","In":["x"],"EqualTo":1}]`)+`}`, nil, invalid),
		c("no operator", jsonRule(`"$.a"`, `,"Filters":[{"Match":"$.a"}]`)+`}`, nil, invalid),
		c("no match", jsonRule(`"$.a"`, `,"Filters":[{"IsPresent":true}]`)+`}`, nil, invalid),
		c(
			"string for numeric operator",
			jsonRule(`"$.a"`, `,"Filters":[{"Match":"$.a","EqualTo":"x"}]`)+`}`,
			nil,
			invalid,
		),
		c("fields on json", jsonRule(`"$.a"`, "")+`,"Fields":{"1":"A"}}`, nil, invalid),
		c(
			"clf unknown alias",
			`{"Schema":{"Name":"CloudWatchLogRule","Version":1},"LogGroupNames":["g"],"LogFormat":"CLF",`+
				`"Contribution":{"Keys":["Ip"]}}`,
			nil,
			invalid,
		),
		c("bad state", jsonRule(`"$.a"`, "")+`}`, aws.String("PAUSED"), invalid),
		c("not json", "nope", nil, invalid),
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newInsightEnv(t)
			assert.ErrorAs(t, env.putRule(t, "r", tc.def, tc.state), &tc.want)
		})
	}
}

func TestInsightRule_RegionIsolation(t *testing.T) {
	t.Parallel()

	logs := cloudwatchlogs.NewInMemoryBackend()
	west := cloudwatch.NewInMemoryBackendWithConfig("000000000000", "us-west-2")
	west.SetLogEventSource(insightLogs{b: logs})

	start := time.Now().UTC().Add(-time.Hour).Truncate(time.Minute)

	for region, ip := range map[string]string{"us-west-2": "west", "eu-west-1": "eu"} {
		ctx := cloudwatchlogs.WithRegion(context.Background(), region)
		_, err := logs.CreateLogGroup(ctx, "app-1", "", "")
		require.NoError(t, err)
		_, err = logs.CreateLogStream(ctx, "app-1", "s")
		require.NoError(t, err)
		_, err = logs.PutLogEvents(ctx, "app-1", "s", "", []cloudwatchlogs.InputLogEvent{
			{Message: `{"ip":"` + ip + `"}`, Timestamp: start.UnixMilli()},
		})
		require.NoError(t, err)
	}

	require.NoError(
		t,
		west.PutInsightRule(&cloudwatch.InsightRule{Name: "r", Definition: jsonRule(`"$.ip"`, "") + `}`}),
	)

	report, err := west.GetInsightRuleReport(cloudwatch.InsightRuleReportRequest{
		RuleName: "r", StartTime: start, EndTime: start.Add(time.Hour), Period: insightPeriod,
	})
	require.NoError(t, err)
	require.Len(t, report.Contributors, 1)
	assert.Equal(t, []string{"west"}, report.Contributors[0].Keys)
}

func TestInsightRule_ManagedRuleRecordedOnly(t *testing.T) {
	t.Parallel()

	env := newInsightEnv(t)
	_, err := env.client.PutManagedInsightRules(t.Context(), &cwsdk.PutManagedInsightRulesInput{
		ManagedRules: []cwtypes.ManagedRule{{
			ResourceARN:  aws.String("arn:aws:dynamodb:us-east-1:000000000000:table/t"),
			TemplateName: aws.String("DynamoDBContributorInsights"),
		}},
	})
	require.NoError(t, err)

	list, err := env.client.ListManagedInsightRules(t.Context(), &cwsdk.ListManagedInsightRulesInput{
		ResourceARN: aws.String("arn:aws:dynamodb:us-east-1:000000000000:table/t"),
	})
	require.NoError(t, err)
	require.Len(t, list.ManagedRules, 1)

	out := env.report(t, aws.ToString(list.ManagedRules[0].RuleState.RuleName), nil)
	assert.Empty(t, out.Contributors)
}
