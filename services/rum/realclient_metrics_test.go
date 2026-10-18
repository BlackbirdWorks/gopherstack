package rum_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsrum "github.com/aws/aws-sdk-go-v2/service/rum"
	"github.com/aws/aws-sdk-go-v2/service/rum/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
)

const (
	navPattern     = `{"event_type":["com.amazon.rum.performance_navigation_event"]}`
	evidentlyARN   = "arn:aws:evidently:us-east-1:123456789012:project/p/experiment/e"
	evidentlyRole  = "arn:aws:iam::123456789012:role/evidently"
	customPattern  = `{"event_type":["com.example.checkout"],"metadata":{"pageId":["/pay"]}}`
	customCheckout = "com.example.checkout"
)

func (f *fixture) putCloudWatchDestination(t *testing.T) {
	t.Helper()

	_, err := f.client.PutRumMetricsDestination(t.Context(), &awsrum.PutRumMetricsDestinationInput{
		AppMonitorName: aws.String("web"), Destination: types.MetricDestinationCloudWatch,
	})
	require.NoError(t, err)
}

func TestRealClient_MetricDestinations(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	f.createMonitor(t, "web")

	cases := []struct {
		in   awsrum.PutRumMetricsDestinationInput
		name string
		code string
	}{
		{
			awsrum.PutRumMetricsDestinationInput{
				AppMonitorName: aws.String("nope"),
				Destination:    types.MetricDestinationCloudWatch,
			},
			"missing_monitor",
			"ResourceNotFoundException",
		},
		{
			awsrum.PutRumMetricsDestinationInput{AppMonitorName: aws.String("web"), Destination: "Kinesis"},
			"bad_destination",
			"ValidationException",
		},
		{
			awsrum.PutRumMetricsDestinationInput{
				AppMonitorName: aws.String("web"),
				Destination:    types.MetricDestinationCloudWatch,
				DestinationArn: aws.String(evidentlyARN),
			},
			"arn_on_cloudwatch",
			"ValidationException",
		},
		{
			awsrum.PutRumMetricsDestinationInput{
				AppMonitorName: aws.String("web"),
				Destination:    types.MetricDestinationEvidently,
			},
			"evidently_no_arn",
			"ValidationException",
		},
		{
			awsrum.PutRumMetricsDestinationInput{
				AppMonitorName: aws.String("web"),
				Destination:    types.MetricDestinationEvidently,
				DestinationArn: aws.String(evidentlyARN),
			},
			"evidently_no_role",
			"ValidationException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := f.client.PutRumMetricsDestination(ctx, &tc.in)
			assert.Equal(t, tc.code, apiCode(err))
		})
	}

	f.putCloudWatchDestination(t)

	_, err := f.client.PutRumMetricsDestination(ctx, &awsrum.PutRumMetricsDestinationInput{
		AppMonitorName: aws.String("web"), Destination: types.MetricDestinationEvidently,
		DestinationArn: aws.String(evidentlyARN), IamRoleArn: aws.String(evidentlyRole),
	})
	require.NoError(t, err)

	list, err := f.client.ListRumMetricsDestinations(
		ctx,
		&awsrum.ListRumMetricsDestinationsInput{AppMonitorName: aws.String("web")},
	)
	require.NoError(t, err)
	require.Len(t, list.Destinations, 2)
	assert.Equal(t, types.MetricDestinationCloudWatch, list.Destinations[0].Destination)
	assert.Equal(t, types.MetricDestinationEvidently, list.Destinations[1].Destination)
	assert.Equal(t, evidentlyARN, aws.ToString(list.Destinations[1].DestinationArn))
	assert.Equal(t, evidentlyRole, aws.ToString(list.Destinations[1].IamRoleArn))

	page1, err := f.client.ListRumMetricsDestinations(
		ctx,
		&awsrum.ListRumMetricsDestinationsInput{AppMonitorName: aws.String("web"), MaxResults: aws.Int32(1)},
	)
	require.NoError(t, err)
	require.NotNil(t, page1.NextToken)

	_, err = f.client.DeleteRumMetricsDestination(ctx, &awsrum.DeleteRumMetricsDestinationInput{
		AppMonitorName: aws.String(
			"web",
		),
		Destination:    types.MetricDestinationEvidently,
		DestinationArn: aws.String(evidentlyARN),
	})
	require.NoError(t, err)

	_, err = f.client.DeleteRumMetricsDestination(ctx, &awsrum.DeleteRumMetricsDestinationInput{
		AppMonitorName: aws.String(
			"web",
		),
		Destination:    types.MetricDestinationEvidently,
		DestinationArn: aws.String(evidentlyARN),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	list, err = f.client.ListRumMetricsDestinations(
		ctx,
		&awsrum.ListRumMetricsDestinationsInput{AppMonitorName: aws.String("web")},
	)
	require.NoError(t, err)
	assert.Len(t, list.Destinations, 1)
}

func TestRealClient_MetricDefinitionValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.createMonitor(t, "web")
	f.putCloudWatchDestination(t)

	def := func(mut func(*types.MetricDefinitionRequest)) types.MetricDefinitionRequest {
		d := types.MetricDefinitionRequest{
			Name: aws.String("PerformanceNavigationDuration"), ValueKey: aws.String("event_details.duration"),
			EventPattern: aws.String(navPattern),
		}
		mut(&d)

		return d
	}

	cases := []struct {
		req  types.MetricDefinitionRequest
		name string
		ok   bool
	}{
		{def(func(*types.MetricDefinitionRequest) {}), "valid_extended", true},
		{def(func(d *types.MetricDefinitionRequest) { d.Name = aws.String("Bogus") }), "unknown_extended_name", false},
		{def(func(d *types.MetricDefinitionRequest) { d.EventPattern = nil }), "extended_needs_pattern", false},
		{
			def(func(d *types.MetricDefinitionRequest) { d.EventPattern = aws.String(`{"event_type":["other"]}`) }),
			"wrong_event_type",
			false,
		},
		{
			def(func(d *types.MetricDefinitionRequest) { d.ValueKey = aws.String("event_details.value") }),
			"wrong_value_key",
			false,
		},
		{
			def(func(d *types.MetricDefinitionRequest) { d.EventPattern = aws.String(`[1]`) }),
			"pattern_not_object",
			false,
		},
		{def(func(d *types.MetricDefinitionRequest) {
			d.EventPattern = aws.String(
				`{"event_type":["com.amazon.rum.performance_navigation_event"],"metadata":{"browserName":["Chrome"]}}`,
			)
			d.DimensionKeys = map[string]string{"metadata.browserName": "BrowserName"}
		}), "valid_dimension", true},
		{def(func(d *types.MetricDefinitionRequest) {
			d.DimensionKeys = map[string]string{"metadata.browserName": "BrowserName"}
		}), "dimension_not_in_pattern", false},
		{def(func(d *types.MetricDefinitionRequest) {
			d.EventPattern = aws.String(
				`{"event_type":["com.amazon.rum.performance_navigation_event"],"metadata":{"x":["1"]}}`,
			)
			d.DimensionKeys = map[string]string{"metadata.x": "X"}
		}), "invalid_dimension_name", false},
		{types.MetricDefinitionRequest{
			Name: aws.String("Checkout"), Namespace: aws.String("shop"), EventPattern: aws.String(customPattern),
		}, "valid_custom", true},
		{
			types.MetricDefinitionRequest{Name: aws.String("Checkout"), Namespace: aws.String("AWS/Shop")},
			"reserved_namespace",
			false,
		},
		{types.MetricDefinitionRequest{
			Name: aws.String("Checkout"), Namespace: aws.String("shop"), EventPattern: aws.String(`{"bogus":["x"]}`),
		}, "bad_top_level_key", false},
		{
			types.MetricDefinitionRequest{
				Name: aws.String(
					"Checkout",
				),
				Namespace:    aws.String("shop"),
				EventPattern: aws.String(`{"event_details":{"a":["b"]}}`),
			},
			"details_without_type",
			false,
		},
		{types.MetricDefinitionRequest{
			Name: aws.String("Checkout"), Namespace: aws.String("shop"), ValueKey: aws.String("nope.value"),
		}, "bad_value_key", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := f.client.BatchCreateRumMetricDefinitions(
				t.Context(),
				&awsrum.BatchCreateRumMetricDefinitionsInput{
					AppMonitorName: aws.String("web"), Destination: types.MetricDestinationCloudWatch,
					MetricDefinitions: []types.MetricDefinitionRequest{tc.req},
				},
			)
			require.NoError(t, err)

			if tc.ok {
				assert.Len(t, out.MetricDefinitions, 1)
				assert.Empty(t, out.Errors)

				return
			}

			assert.Empty(t, out.MetricDefinitions)
			require.Len(t, out.Errors, 1)
			assert.Equal(t, aws.ToString(tc.req.Name), aws.ToString(out.Errors[0].MetricDefinition.Name))
			assert.NotEmpty(t, aws.ToString(out.Errors[0].ErrorMessage))
		})
	}
}

func TestRealClient_MetricDefinitionLifecycle(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	f.createMonitor(t, "web")

	_, err := f.client.BatchCreateRumMetricDefinitions(ctx, &awsrum.BatchCreateRumMetricDefinitionsInput{
		AppMonitorName: aws.String("web"), Destination: types.MetricDestinationCloudWatch,
		MetricDefinitions: []types.MetricDefinitionRequest{{Name: aws.String("x"), Namespace: aws.String("shop")}},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "needs a destination first")

	f.putCloudWatchDestination(t)

	created, err := f.client.BatchCreateRumMetricDefinitions(ctx, &awsrum.BatchCreateRumMetricDefinitionsInput{
		AppMonitorName: aws.String("web"), Destination: types.MetricDestinationCloudWatch,
		MetricDefinitions: []types.MetricDefinitionRequest{
			{Name: aws.String("A"), Namespace: aws.String("shop"), UnitLabel: aws.String("Count")},
			{Name: aws.String("B"), Namespace: aws.String("shop")},
			{Name: aws.String("C"), Namespace: aws.String("AWS/bad")},
		},
	})
	require.NoError(t, err)
	require.Len(t, created.MetricDefinitions, 2)
	require.Len(t, created.Errors, 1)

	idA, idB := aws.ToString(
		created.MetricDefinitions[0].MetricDefinitionId,
	), aws.ToString(
		created.MetricDefinitions[1].MetricDefinitionId,
	)
	assert.NotEqual(t, idA, idB)

	get := func(limit *int32, token *string) *awsrum.BatchGetRumMetricDefinitionsOutput {
		out, gerr := f.client.BatchGetRumMetricDefinitions(ctx, &awsrum.BatchGetRumMetricDefinitionsInput{
			AppMonitorName: aws.String(
				"web",
			),
			Destination: types.MetricDestinationCloudWatch,
			MaxResults:  limit,
			NextToken:   token,
		})
		require.NoError(t, gerr)

		return out
	}

	all := get(nil, nil)
	require.Len(t, all.MetricDefinitions, 2)
	assert.Equal(t, "A", aws.ToString(all.MetricDefinitions[0].Name), "creation order")
	assert.Equal(t, "Count", aws.ToString(all.MetricDefinitions[0].UnitLabel))

	p1 := get(aws.Int32(1), nil)
	require.Len(t, p1.MetricDefinitions, 1)
	require.NotNil(t, p1.NextToken)
	assert.Equal(t, "B", aws.ToString(get(aws.Int32(1), p1.NextToken).MetricDefinitions[0].Name))

	_, err = f.client.UpdateRumMetricDefinition(ctx, &awsrum.UpdateRumMetricDefinitionInput{
		AppMonitorName: aws.String(
			"web",
		),
		Destination:        types.MetricDestinationCloudWatch,
		MetricDefinitionId: aws.String(idA),
		MetricDefinition: &types.MetricDefinitionRequest{
			Name:      aws.String("A2"),
			Namespace: aws.String("shop"),
			UnitLabel: aws.String("Seconds"),
		},
	})
	require.NoError(t, err)

	all = get(nil, nil)
	assert.Equal(t, "A2", aws.ToString(all.MetricDefinitions[0].Name))
	assert.Equal(t, "Seconds", aws.ToString(all.MetricDefinitions[0].UnitLabel))

	_, err = f.client.UpdateRumMetricDefinition(ctx, &awsrum.UpdateRumMetricDefinitionInput{
		AppMonitorName: aws.String(
			"web",
		),
		Destination:        types.MetricDestinationCloudWatch,
		MetricDefinitionId: aws.String("missing"),
		MetricDefinition:   &types.MetricDefinitionRequest{Name: aws.String("A2"), Namespace: aws.String("shop")},
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	del, err := f.client.BatchDeleteRumMetricDefinitions(ctx, &awsrum.BatchDeleteRumMetricDefinitionsInput{
		AppMonitorName: aws.String(
			"web",
		),
		Destination:         types.MetricDestinationCloudWatch,
		MetricDefinitionIds: []string{idA, "ghost"},
	})
	require.NoError(t, err)
	assert.Equal(t, []string{idA}, del.MetricDefinitionIds)
	require.Len(t, del.Errors, 1)
	assert.Equal(t, "ghost", aws.ToString(del.Errors[0].MetricDefinitionId))

	assert.Len(t, get(nil, nil).MetricDefinitions, 1)

	_, err = f.client.DeleteRumMetricsDestination(ctx, &awsrum.DeleteRumMetricsDestinationInput{
		AppMonitorName: aws.String("web"), Destination: types.MetricDestinationCloudWatch,
	})
	require.NoError(t, err)

	_, err = f.client.BatchGetRumMetricDefinitions(ctx, &awsrum.BatchGetRumMetricDefinitionsInput{
		AppMonitorName: aws.String("web"), Destination: types.MetricDestinationCloudWatch,
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err), "deleting a destination drops its definitions")
}

func TestRealClient_MetricEmission(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	id := f.createMonitor(t, "web")
	f.putCloudWatchDestination(t)
	base := f.base()

	_, err := f.client.BatchCreateRumMetricDefinitions(t.Context(), &awsrum.BatchCreateRumMetricDefinitionsInput{
		AppMonitorName: aws.String("web"), Destination: types.MetricDestinationCloudWatch,
		MetricDefinitions: []types.MetricDefinitionRequest{
			{
				Name: aws.String("Checkout"), Namespace: aws.String("shop"), UnitLabel: aws.String("Count"),
				EventPattern:  aws.String(customPattern),
				DimensionKeys: map[string]string{"metadata.pageId": "Page"},
			},
			{
				Name: aws.String("CheckoutValue"), Namespace: aws.String("shop"), UnitLabel: aws.String("None"),
				EventPattern: aws.String(customPattern), ValueKey: aws.String("event_details.amount"),
			},
			{
				Name:     aws.String("PerformanceNavigationDuration"),
				ValueKey: aws.String("event_details.duration"),
				EventPattern: aws.String(
					`{"event_type":["com.amazon.rum.performance_navigation_event"],"metadata":{"browserName":["Chrome"]}}`,
				),
				DimensionKeys: map[string]string{"metadata.browserName": "BrowserName"},
			},
		},
	})
	require.NoError(t, err)

	require.NoError(t, f.putEvents(t, id, "web",
		eventSpec{id: "e1", typ: customCheckout, details: `{"amount":42.5}`, meta: `{"pageId":"/pay"}`, ts: base},
		eventSpec{id: "e2", typ: customCheckout, details: `{"amount":1}`, meta: `{"pageId":"/other"}`, ts: base},
		eventSpec{id: "e3", typ: evtNav, details: `{"duration":800}`, meta: chromeMeta("/home"), ts: base},
		eventSpec{id: "e4", typ: evtNav, details: `{"duration":9000}`, meta: `{"browserName":"Firefox"}`, ts: base},
		eventSpec{id: "e5", typ: evtSession, details: `{}`, ts: base},
		eventSpec{id: "e6", typ: evtHTTP, details: `{"response":{"status":503}}`, ts: base},
		eventSpec{id: "e7", typ: evtJSError, details: `{"message":"boom"}`, ts: base},
	))

	checkout := f.metrics.named("RUM/CustomMetrics/shop", "Checkout")
	require.Len(t, checkout, 1, "only the /pay event matches the pattern")
	assert.Equal(t, "Count", checkout[0].Unit)
	assert.InDelta(t, 1, checkout[0].Value, 0)
	assert.Equal(t, testRegion, checkout[0].Region)
	assert.Equal(
		t,
		[]cwmetric.Dimension{{Name: "application_name", Value: "web"}, {Name: "Page", Value: "/pay"}},
		checkout[0].Dimensions,
	)

	value := f.metrics.named("RUM/CustomMetrics/shop", "CheckoutValue")
	require.Len(t, value, 1)
	assert.InDelta(t, 42.5, value[0].Value, 0)

	ext := f.metrics.named("AWS/RUM", "PerformanceNavigationDuration")
	require.Len(t, ext, 3, "two default points and the Chrome-only extended definition")

	var withBrowser int

	for _, p := range ext {
		for _, d := range p.Dimensions {
			if d.Name == "BrowserName" && d.Value == "Chrome" {
				withBrowser++

				assert.InDelta(t, 800, p.Value, 0)
				assert.Equal(t, "Milliseconds", p.Unit)
			}
		}
	}

	assert.Equal(t, 1, withBrowser)

	counts := map[string]int{
		"SessionCount": 1, "JsErrorCount": 1, "Http5xxCount": 1, "Http4xxCount": 0, "HttpErrorCount": 1,
	}

	for name, want := range counts {
		assert.Len(t, f.metrics.named("AWS/RUM", name), want, name)
	}

	sessionPoint := f.metrics.named("AWS/RUM", "SessionCount")[0]
	assert.Equal(t, []cwmetric.Dimension{{Name: "application_name", Value: "web"}}, sessionPoint.Dimensions)
}
