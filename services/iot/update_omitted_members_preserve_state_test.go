package iot_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	iotsdk "github.com/aws/aws-sdk-go-v2/service/iot"
	iottypes "github.com/aws/aws-sdk-go-v2/service/iot/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUpdate_OmittedMembersPreserveState locks in the zeroguard census fix
// (gopherstack-101r follow-up): an Update op's optional *string member must
// be applied only when the caller sends it. Before the fix these fields
// decoded as plain strings, so a second update that simply omitted a field
// silently blanked it instead of leaving the stored value alone. Each case
// creates a resource, sets a field, sends a second update that omits it
// and asserts the earlier value survived, then sends an explicit empty
// string and asserts it is applied (not treated as omitted).
func TestUpdate_OmittedMembersPreserveState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "job_description", run: testJobDescriptionPreserved},
		{name: "fleet_metric_fields", run: testFleetMetricFieldsPreserved},
		{name: "certificate_provider_lambda_arn", run: testCertificateProviderLambdaARNPreserved},
		{name: "thing_type_name", run: testThingThingTypeNamePreserved},
		{name: "thing_group_description", run: testThingGroupDescriptionPreserved},
		{name: "dynamic_thing_group_fields", run: testDynamicThingGroupFieldsPreserved},
		{name: "thing_type_description", run: testThingTypeDescriptionPreserved},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func testJobDescriptionPreserved(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)
	ctx := t.Context()

	_, err := client.CreateJob(ctx, &iotsdk.CreateJobInput{
		JobId:       aws.String("omit-job"),
		Targets:     []string{"arn:aws:iot:us-east-1:000000000000:thing/omit-thing"},
		Description: aws.String("first description"),
	})
	require.NoError(t, err)

	// Omits Description but touches an unrelated field -- must survive.
	_, err = client.UpdateJob(ctx, &iotsdk.UpdateJobInput{
		JobId: aws.String("omit-job"),
		PresignedUrlConfig: &iottypes.PresignedUrlConfig{
			RoleArn: aws.String("arn:aws:iam::000000000000:role/presign"),
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeJob(ctx, &iotsdk.DescribeJobInput{JobId: aws.String("omit-job")})
	require.NoError(t, err)
	assert.Equal(t, "first description", aws.ToString(desc.Job.Description),
		"Description must survive an update that omits it")

	// Explicit empty Description clears it.
	_, err = client.UpdateJob(ctx, &iotsdk.UpdateJobInput{
		JobId:       aws.String("omit-job"),
		Description: aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeJob(ctx, &iotsdk.DescribeJobInput{JobId: aws.String("omit-job")})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Job.Description))
}

func testFleetMetricFieldsPreserved(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)
	ctx := t.Context()

	_, err := client.CreateFleetMetric(ctx, &iotsdk.CreateFleetMetricInput{
		MetricName:       aws.String("omit-fm"),
		QueryString:      aws.String("*"),
		AggregationField: aws.String("thingTypeName.keyword"),
		AggregationType:  &iottypes.AggregationType{Name: iottypes.AggregationTypeNameStatistics},
		Period:           aws.Int32(300),
	})
	require.NoError(t, err)

	_, err = client.UpdateFleetMetric(ctx, &iotsdk.UpdateFleetMetricInput{
		MetricName:       aws.String("omit-fm"),
		QueryString:      aws.String("thingName:*"),
		IndexName:        aws.String("AWS_Things"),
		QueryVersion:     aws.String("2017-09-30"),
		Description:      aws.String("first description"),
		AggregationField: aws.String("thingName.keyword"),
	})
	require.NoError(t, err)

	// Omits all optional fields (IndexName is a required member on every
	// UpdateFleetMetric call, so it is resent with its current value) -- the
	// rest must survive.
	_, err = client.UpdateFleetMetric(ctx, &iotsdk.UpdateFleetMetricInput{
		MetricName: aws.String("omit-fm"),
		IndexName:  aws.String("AWS_Things"),
		Period:     aws.Int32(600),
	})
	require.NoError(t, err)

	desc, err := client.DescribeFleetMetric(ctx, &iotsdk.DescribeFleetMetricInput{
		MetricName: aws.String("omit-fm"),
	})
	require.NoError(t, err)
	assert.Equal(t, "thingName:*", aws.ToString(desc.QueryString), "QueryString must survive")
	assert.Equal(t, "AWS_Things", aws.ToString(desc.IndexName), "IndexName must survive")
	assert.Equal(t, "2017-09-30", aws.ToString(desc.QueryVersion), "QueryVersion must survive")
	assert.Equal(t, "first description", aws.ToString(desc.Description), "Description must survive")
	assert.Equal(t, "thingName.keyword", aws.ToString(desc.AggregationField), "AggregationField must survive")

	// Explicit empty Description clears it, others untouched.
	_, err = client.UpdateFleetMetric(ctx, &iotsdk.UpdateFleetMetricInput{
		MetricName:  aws.String("omit-fm"),
		IndexName:   aws.String("AWS_Things"),
		Description: aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeFleetMetric(ctx, &iotsdk.DescribeFleetMetricInput{
		MetricName: aws.String("omit-fm"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.Description))
	assert.Equal(t, "thingName:*", aws.ToString(cleared.QueryString))
}

func testCertificateProviderLambdaARNPreserved(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)
	ctx := t.Context()

	_, err := client.CreateCertificateProvider(ctx, &iotsdk.CreateCertificateProviderInput{
		CertificateProviderName: aws.String("omit-cert-provider"),
		LambdaFunctionArn:       aws.String("arn:aws:lambda:us-east-1:000000000000:function:first"),
		AccountDefaultForOperations: []iottypes.CertificateProviderOperation{
			iottypes.CertificateProviderOperationCreateCertificateFromCsr,
		},
	})
	require.NoError(t, err)

	// Omits LambdaFunctionArn but changes AccountDefaultForOperations -- must survive.
	_, err = client.UpdateCertificateProvider(ctx, &iotsdk.UpdateCertificateProviderInput{
		CertificateProviderName: aws.String("omit-cert-provider"),
		AccountDefaultForOperations: []iottypes.CertificateProviderOperation{
			iottypes.CertificateProviderOperationCreateCertificateFromCsr,
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeCertificateProvider(ctx, &iotsdk.DescribeCertificateProviderInput{
		CertificateProviderName: aws.String("omit-cert-provider"),
	})
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:lambda:us-east-1:000000000000:function:first", aws.ToString(desc.LambdaFunctionArn),
		"LambdaFunctionArn must survive an update that omits it")

	// Explicit empty LambdaFunctionArn clears it.
	_, err = client.UpdateCertificateProvider(ctx, &iotsdk.UpdateCertificateProviderInput{
		CertificateProviderName: aws.String("omit-cert-provider"),
		LambdaFunctionArn:       aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeCertificateProvider(ctx, &iotsdk.DescribeCertificateProviderInput{
		CertificateProviderName: aws.String("omit-cert-provider"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.LambdaFunctionArn))
}

func testThingThingTypeNamePreserved(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)
	ctx := t.Context()

	_, err := client.CreateThingType(ctx, &iotsdk.CreateThingTypeInput{
		ThingTypeName: aws.String("omit-tt"),
	})
	require.NoError(t, err)

	_, err = client.CreateThing(ctx, &iotsdk.CreateThingInput{
		ThingName: aws.String("omit-thing-type-thing"),
	})
	require.NoError(t, err)

	_, err = client.UpdateThing(ctx, &iotsdk.UpdateThingInput{
		ThingName:     aws.String("omit-thing-type-thing"),
		ThingTypeName: aws.String("omit-tt"),
	})
	require.NoError(t, err)

	// Omits ThingTypeName but touches AttributePayload -- must survive.
	_, err = client.UpdateThing(ctx, &iotsdk.UpdateThingInput{
		ThingName: aws.String("omit-thing-type-thing"),
		AttributePayload: &iottypes.AttributePayload{
			Attributes: map[string]string{"env": "prod"},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeThing(ctx, &iotsdk.DescribeThingInput{
		ThingName: aws.String("omit-thing-type-thing"),
	})
	require.NoError(t, err)
	assert.Equal(t, "omit-tt", aws.ToString(desc.ThingTypeName),
		"ThingTypeName must survive an update that omits it")
}

func testThingGroupDescriptionPreserved(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)
	ctx := t.Context()

	_, err := client.CreateThingGroup(ctx, &iotsdk.CreateThingGroupInput{
		ThingGroupName: aws.String("omit-tg"),
	})
	require.NoError(t, err)

	_, err = client.UpdateThingGroup(ctx, &iotsdk.UpdateThingGroupInput{
		ThingGroupName: aws.String("omit-tg"),
		ThingGroupProperties: &iottypes.ThingGroupProperties{
			ThingGroupDescription: aws.String("first description"),
		},
	})
	require.NoError(t, err)

	// ThingGroupProperties is a required member on every UpdateThingGroup
	// call, but sending it empty (no ThingGroupDescription) still means
	// "don't change the description".
	_, err = client.UpdateThingGroup(ctx, &iotsdk.UpdateThingGroupInput{
		ThingGroupName:       aws.String("omit-tg"),
		ThingGroupProperties: &iottypes.ThingGroupProperties{},
	})
	require.NoError(t, err)

	desc, err := client.DescribeThingGroup(ctx, &iotsdk.DescribeThingGroupInput{
		ThingGroupName: aws.String("omit-tg"),
	})
	require.NoError(t, err)
	assert.Equal(
		t,
		"first description",
		aws.ToString(desc.ThingGroupProperties.ThingGroupDescription),
		"ThingGroupDescription must survive an update that omits it",
	)

	// Explicit empty ThingGroupDescription clears it.
	_, err = client.UpdateThingGroup(ctx, &iotsdk.UpdateThingGroupInput{
		ThingGroupName: aws.String("omit-tg"),
		ThingGroupProperties: &iottypes.ThingGroupProperties{
			ThingGroupDescription: aws.String(""),
		},
	})
	require.NoError(t, err)

	cleared, err := client.DescribeThingGroup(ctx, &iotsdk.DescribeThingGroupInput{
		ThingGroupName: aws.String("omit-tg"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.ThingGroupProperties.ThingGroupDescription))
}

func testDynamicThingGroupFieldsPreserved(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)
	ctx := t.Context()

	_, err := client.CreateDynamicThingGroup(ctx, &iotsdk.CreateDynamicThingGroupInput{
		ThingGroupName: aws.String("omit-dtg"),
		QueryString:    aws.String("thingTypeName:foo"),
	})
	require.NoError(t, err)

	_, err = client.UpdateDynamicThingGroup(ctx, &iotsdk.UpdateDynamicThingGroupInput{
		ThingGroupName: aws.String("omit-dtg"),
		QueryString:    aws.String("thingTypeName:bar"),
		IndexName:      aws.String("AWS_Things"),
		QueryVersion:   aws.String("2017-09-30"),
		ThingGroupProperties: &iottypes.ThingGroupProperties{
			ThingGroupDescription: aws.String("first description"),
		},
	})
	require.NoError(t, err)

	// ThingGroupProperties is a required member on every UpdateDynamicThingGroup
	// call, but sending it empty still means "don't change the description".
	_, err = client.UpdateDynamicThingGroup(ctx, &iotsdk.UpdateDynamicThingGroupInput{
		ThingGroupName:       aws.String("omit-dtg"),
		ThingGroupProperties: &iottypes.ThingGroupProperties{},
	})
	require.NoError(t, err)

	desc, err := client.DescribeThingGroup(ctx, &iotsdk.DescribeThingGroupInput{
		ThingGroupName: aws.String("omit-dtg"),
	})
	require.NoError(t, err)
	assert.Equal(t, "thingTypeName:bar", aws.ToString(desc.QueryString), "QueryString must survive")
	assert.Equal(t, "AWS_Things", aws.ToString(desc.IndexName), "IndexName must survive")
	assert.Equal(t, "2017-09-30", aws.ToString(desc.QueryVersion), "QueryVersion must survive")
	assert.Equal(
		t,
		"first description",
		aws.ToString(desc.ThingGroupProperties.ThingGroupDescription),
		"ThingGroupDescription must survive",
	)

	// Explicit empty QueryString clears it, others untouched.
	_, err = client.UpdateDynamicThingGroup(ctx, &iotsdk.UpdateDynamicThingGroupInput{
		ThingGroupName:       aws.String("omit-dtg"),
		ThingGroupProperties: &iottypes.ThingGroupProperties{},
		QueryString:          aws.String(""),
	})
	require.NoError(t, err)

	cleared, err := client.DescribeThingGroup(ctx, &iotsdk.DescribeThingGroupInput{
		ThingGroupName: aws.String("omit-dtg"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.QueryString))
	assert.Equal(t, "AWS_Things", aws.ToString(cleared.IndexName))
}

func testThingTypeDescriptionPreserved(t *testing.T) {
	t.Helper()

	client := newIoTTestClient(t)
	ctx := t.Context()

	_, err := client.CreateThingType(ctx, &iotsdk.CreateThingTypeInput{
		ThingTypeName: aws.String("omit-thingtype"),
		ThingTypeProperties: &iottypes.ThingTypeProperties{
			ThingTypeDescription: aws.String("first description"),
		},
	})
	require.NoError(t, err)

	// Omits ThingTypeDescription but adds SearchableAttributes -- must survive.
	_, err = client.UpdateThingType(ctx, &iotsdk.UpdateThingTypeInput{
		ThingTypeName: aws.String("omit-thingtype"),
		ThingTypeProperties: &iottypes.ThingTypeProperties{
			SearchableAttributes: []string{"model"},
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeThingType(ctx, &iotsdk.DescribeThingTypeInput{
		ThingTypeName: aws.String("omit-thingtype"),
	})
	require.NoError(t, err)
	assert.Equal(t, "first description", aws.ToString(desc.ThingTypeProperties.ThingTypeDescription),
		"ThingTypeDescription must survive an update that omits it")

	// Explicit empty ThingTypeDescription clears it.
	_, err = client.UpdateThingType(ctx, &iotsdk.UpdateThingTypeInput{
		ThingTypeName: aws.String("omit-thingtype"),
		ThingTypeProperties: &iottypes.ThingTypeProperties{
			ThingTypeDescription: aws.String(""),
		},
	})
	require.NoError(t, err)

	cleared, err := client.DescribeThingType(ctx, &iotsdk.DescribeThingTypeInput{
		ThingTypeName: aws.String("omit-thingtype"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(cleared.ThingTypeProperties.ThingTypeDescription))
}
