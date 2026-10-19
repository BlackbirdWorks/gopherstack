package rum_test

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrum "github.com/aws/aws-sdk-go-v2/service/rum"
	"github.com/aws/aws-sdk-go-v2/service/rum/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_AppMonitors(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T, f *fixture)
		name string
	}{
		{testMonitorLifecycle, "lifecycle"},
		{testMonitorValidation, "validation"},
		{testMonitorUpdate, "update"},
		{testMonitorList, "list"},
		{testMonitorLogGroup, "log_group"},
		{testMonitorLimit, "limit"},
		{testMonitorTags, "tags"},
		{testMonitorIsolation, "account_isolation"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newFixture(t))
		})
	}
}

func testMonitorLifecycle(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	id := f.createMonitor(t, "web", func(in *awsrum.CreateAppMonitorInput) {
		in.AppMonitorConfiguration = &types.AppMonitorConfiguration{
			AllowCookies:      aws.Bool(true),
			SessionSampleRate: 0.5,
			Telemetries:       []types.Telemetry{types.TelemetryErrors},
			IdentityPoolId: aws.String(
				"us-east-1:pool",
			),
			GuestRoleArn: aws.String("arn:aws:iam::123456789012:role/guest"),
		}
		in.CustomEvents = &types.CustomEvents{Status: types.CustomEventsStatusEnabled}
		in.Tags = map[string]string{"env": "test"}
	})
	assert.Regexp(t, `^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$`, id)

	got, err := f.client.GetAppMonitor(ctx, &awsrum.GetAppMonitorInput{Name: aws.String("web")})
	require.NoError(t, err)

	m := got.AppMonitor
	assert.Equal(t, id, aws.ToString(m.Id))
	assert.Equal(t, "web", aws.ToString(m.Name))
	assert.Equal(t, "example.com", aws.ToString(m.Domain))
	assert.Equal(t, types.AppMonitorPlatformWeb, m.Platform)
	assert.Equal(t, types.StateEnumCreated, m.State)
	assert.Equal(t, types.CustomEventsStatusEnabled, m.CustomEvents.Status)
	assert.InDelta(t, 0.5, m.AppMonitorConfiguration.SessionSampleRate, 0)
	assert.True(t, aws.ToBool(m.AppMonitorConfiguration.AllowCookies))
	assert.Equal(t, []types.Telemetry{types.TelemetryErrors}, m.AppMonitorConfiguration.Telemetries)
	assert.Equal(t, map[string]string{"env": "test"}, m.Tags)
	assert.False(t, aws.ToBool(m.DataStorage.CwLog.CwLogEnabled))
	assert.NotEmpty(t, aws.ToString(m.Created))

	_, err = f.client.DeleteAppMonitor(ctx, &awsrum.DeleteAppMonitorInput{Name: aws.String("web")})
	require.NoError(t, err)

	_, err = f.client.GetAppMonitor(ctx, &awsrum.GetAppMonitorInput{Name: aws.String("web")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	_, err = f.client.DeleteAppMonitor(ctx, &awsrum.DeleteAppMonitorInput{Name: aws.String("web")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func testMonitorValidation(t *testing.T, f *fixture) {
	t.Helper()

	f.createMonitor(t, "taken")

	cases := []struct {
		in   awsrum.CreateAppMonitorInput
		name string
		code string
	}{
		{
			awsrum.CreateAppMonitorInput{Name: aws.String("taken"), Domain: aws.String("a.com")},
			"duplicate",
			"ConflictException",
		},
		{
			awsrum.CreateAppMonitorInput{Name: aws.String("bad name!"), Domain: aws.String("a.com")},
			"bad_name",
			"ValidationException",
		},
		{awsrum.CreateAppMonitorInput{Name: aws.String("nodomain")}, "no_domain", "ValidationException"},
		{
			awsrum.CreateAppMonitorInput{
				Name:       aws.String("both"),
				Domain:     aws.String("a.com"),
				DomainList: []string{"b.com"},
			},
			"both_domains",
			"ValidationException",
		},
		{
			awsrum.CreateAppMonitorInput{
				Name:     aws.String("platform"),
				Domain:   aws.String("a.com"),
				Platform: "Symbian",
			},
			"platform",
			"ValidationException",
		},
		{
			awsrum.CreateAppMonitorInput{
				Name: aws.String("rate"), Domain: aws.String("a.com"),
				AppMonitorConfiguration: &types.AppMonitorConfiguration{SessionSampleRate: 1.5},
			},
			"sample_rate", "ValidationException",
		},
		{
			awsrum.CreateAppMonitorInput{
				Name:   aws.String("pages"),
				Domain: aws.String("a.com"),
				AppMonitorConfiguration: &types.AppMonitorConfiguration{
					IncludedPages: []string{"a"},
					ExcludedPages: []string{"b"},
				},
			},
			"pages",
			"ValidationException",
		},
		{
			awsrum.CreateAppMonitorInput{
				Name: aws.String("maps"), Domain: aws.String("a.com"),
				DeobfuscationConfiguration: &types.DeobfuscationConfiguration{
					JavaScriptSourceMaps: &types.JavaScriptSourceMaps{Status: types.DeobfuscationStatusEnabled},
				},
			},
			"source_maps", "ValidationException",
		},
		{
			awsrum.CreateAppMonitorInput{
				Name:   aws.String("tags"),
				Domain: aws.String("a.com"),
				Tags:   map[string]string{"aws:x": "1"},
			},
			"reserved_tag",
			"ValidationException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := f.client.CreateAppMonitor(t.Context(), &tc.in)
			require.Error(t, err)
			assert.Equal(t, tc.code, apiCode(err))
		})
	}

	assert.Equal(t, 1, f.backend.MonitorCount())

	mobile, err := f.client.CreateAppMonitor(t.Context(), &awsrum.CreateAppMonitorInput{
		Name: aws.String("mobile"), Platform: types.AppMonitorPlatformAndroid,
	})
	require.NoError(t, err, "mobile monitors need no domain")
	assert.NotEmpty(t, aws.ToString(mobile.Id))
}

func testMonitorUpdate(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	f.createMonitor(t, "web")

	_, err := f.client.UpdateAppMonitor(ctx, &awsrum.UpdateAppMonitorInput{
		Name: aws.String("web"), DomainList: []string{"a.com", "b.com"},
		CustomEvents:            &types.CustomEvents{Status: types.CustomEventsStatusEnabled},
		AppMonitorConfiguration: &types.AppMonitorConfiguration{SessionSampleRate: 1},
	})
	require.NoError(t, err)

	got, err := f.client.GetAppMonitor(ctx, &awsrum.GetAppMonitorInput{Name: aws.String("web")})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(got.AppMonitor.Domain))
	assert.Equal(t, []string{"a.com", "b.com"}, got.AppMonitor.DomainList)
	assert.Equal(t, types.CustomEventsStatusEnabled, got.AppMonitor.CustomEvents.Status)
	assert.InDelta(t, 1.0, got.AppMonitor.AppMonitorConfiguration.SessionSampleRate, 0)

	_, err = f.client.UpdateAppMonitor(
		ctx,
		&awsrum.UpdateAppMonitorInput{Name: aws.String("web"), CwLogEnabled: aws.Bool(true)},
	)
	require.NoError(t, err)

	got, err = f.client.GetAppMonitor(ctx, &awsrum.GetAppMonitorInput{Name: aws.String("web")})
	require.NoError(t, err)
	assert.Equal(t, []string{"a.com", "b.com"}, got.AppMonitor.DomainList, "omitted fields are kept")
	assert.True(t, aws.ToBool(got.AppMonitor.DataStorage.CwLog.CwLogEnabled))

	_, err = f.client.UpdateAppMonitor(
		ctx,
		&awsrum.UpdateAppMonitorInput{
			Name:       aws.String("web"),
			Domain:     aws.String("a.com"),
			DomainList: []string{"b.com"},
		},
	)
	assert.Equal(t, "ValidationException", apiCode(err))

	_, err = f.client.UpdateAppMonitor(
		ctx,
		&awsrum.UpdateAppMonitorInput{Name: aws.String("nope"), Domain: aws.String("a.com")},
	)
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))
}

func testMonitorList(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	for _, n := range []string{"c", "a", "b"} {
		f.createMonitor(t, n)
	}

	all, err := f.client.ListAppMonitors(ctx, &awsrum.ListAppMonitorsInput{})
	require.NoError(t, err)

	names := make([]string, 0, len(all.AppMonitorSummaries))
	for _, s := range all.AppMonitorSummaries {
		names = append(names, aws.ToString(s.Name))
		assert.Equal(t, types.StateEnumCreated, s.State)
	}

	assert.Equal(t, []string{"a", "b", "c"}, names)

	p1, err := f.client.ListAppMonitors(ctx, &awsrum.ListAppMonitorsInput{MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, p1.AppMonitorSummaries, 2)
	require.NotNil(t, p1.NextToken)

	p2, err := f.client.ListAppMonitors(
		ctx,
		&awsrum.ListAppMonitorsInput{MaxResults: aws.Int32(2), NextToken: p1.NextToken},
	)
	require.NoError(t, err)
	require.Len(t, p2.AppMonitorSummaries, 1)
	assert.Equal(t, "c", aws.ToString(p2.AppMonitorSummaries[0].Name))
	assert.Nil(t, p2.NextToken)

	_, err = f.client.ListAppMonitors(ctx, &awsrum.ListAppMonitorsInput{NextToken: aws.String("!!")})
	assert.Equal(t, "ValidationException", apiCode(err))
}

func testMonitorLogGroup(t *testing.T, f *fixture) {
	t.Helper()

	id := f.createMonitor(t, "logged", func(in *awsrum.CreateAppMonitorInput) { in.CwLogEnabled = aws.Bool(true) })

	got, err := f.client.GetAppMonitor(t.Context(), &awsrum.GetAppMonitorInput{Name: aws.String("logged")})
	require.NoError(t, err)

	group := "/aws/vendedlogs/RUMService_logged" + id[:8]
	assert.True(t, aws.ToBool(got.AppMonitor.DataStorage.CwLog.CwLogEnabled))
	assert.Equal(t, group, aws.ToString(got.AppMonitor.DataStorage.CwLog.CwLogGroup))
	assert.True(t, f.logs.groups[group], "the log group is created in CloudWatch Logs")
}

func testMonitorLimit(t *testing.T, f *fixture) {
	t.Helper()

	for i := range 10 {
		f.createMonitor(t, "m"+strconv.Itoa(i))
	}

	_, err := f.client.CreateAppMonitor(
		t.Context(),
		&awsrum.CreateAppMonitorInput{Name: aws.String("one-too-many"), Domain: aws.String("a.com")},
	)
	assert.Equal(t, "ServiceQuotaExceededException", apiCode(err))
}

func testMonitorTags(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	f.createMonitor(t, "tagged")

	arn := aws.String(monitorARN("tagged"))

	_, err := f.client.TagResource(
		ctx,
		&awsrum.TagResourceInput{ResourceArn: arn, Tags: map[string]string{"a": "1", "b": "2"}},
	)
	require.NoError(t, err)

	out, err := f.client.ListTagsForResource(ctx, &awsrum.ListTagsForResourceInput{ResourceArn: arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, out.Tags)
	assert.Equal(t, aws.ToString(arn), aws.ToString(out.ResourceArn))

	_, err = f.client.UntagResource(ctx, &awsrum.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"a", "zzz"}})
	require.NoError(t, err)

	out, err = f.client.ListTagsForResource(ctx, &awsrum.ListTagsForResourceInput{ResourceArn: arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"b": "2"}, out.Tags)

	got, err := f.client.GetAppMonitor(ctx, &awsrum.GetAppMonitorInput{Name: aws.String("tagged")})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"b": "2"}, got.AppMonitor.Tags)

	cases := []struct {
		arn  string
		name string
		code string
	}{
		{monitorARN("missing"), "missing", "ResourceNotFoundException"},
		{"arn:aws:rum:us-east-1:123456789012:other/thing", "wrong_kind", "ValidationException"},
		{"arn:aws:rum:us-east-1:999999999999:appmonitor/tagged", "other_account", "ResourceNotFoundException"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, terr := f.client.ListTagsForResource(
				ctx,
				&awsrum.ListTagsForResourceInput{ResourceArn: aws.String(tc.arn)},
			)
			assert.Equal(t, tc.code, apiCode(terr))
		})
	}
}

func testMonitorIsolation(t *testing.T, f *fixture) {
	t.Helper()

	f.createMonitor(t, "mine")

	other := clientFor(t, f.url, "210987654321")

	_, err := other.GetAppMonitor(t.Context(), &awsrum.GetAppMonitorInput{Name: aws.String("mine")})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	list, err := other.ListAppMonitors(t.Context(), &awsrum.ListAppMonitorsInput{})
	require.NoError(t, err)
	assert.Empty(t, list.AppMonitorSummaries)

	_, err = other.CreateAppMonitor(
		t.Context(),
		&awsrum.CreateAppMonitorInput{Name: aws.String("mine"), Domain: aws.String("a.com")},
	)
	require.NoError(t, err, "names are unique per account")
}
