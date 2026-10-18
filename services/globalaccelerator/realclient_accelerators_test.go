package globalaccelerator_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	gasdk "github.com/aws/aws-sdk-go-v2/service/globalaccelerator"
	gatypes "github.com/aws/aws-sdk-go-v2/service/globalaccelerator/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealClient_StandardAccelerator(t *testing.T) {
	t.Parallel()

	cases := []struct {
		fn   func(t *testing.T, f *fixture)
		name string
	}{
		{testAcceleratorLifecycle, "lifecycle"},
		{testAcceleratorIPAllocation, "ip_allocation"},
		{testAcceleratorAttributes, "attributes"},
		{testAcceleratorListPagination, "pagination"},
		{testAcceleratorIdempotency, "idempotency"},
		{testAcceleratorErrors, "errors"},
		{testAcceleratorTags, "tags"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.fn(t, newFixture(t))
		})
	}
}

func testAcceleratorLifecycle(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()

	created, err := f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{
		Name:          aws.String("web"),
		IpAddressType: gatypes.IpAddressTypeDualStack,
		Tags:          []gatypes.Tag{{Key: aws.String("env"), Value: aws.String("test")}},
	})
	require.NoError(t, err)

	acc := created.Accelerator
	arn := aws.ToString(acc.AcceleratorArn)

	assert.Regexp(t, `^arn:aws:globalaccelerator::123456789012:accelerator/[0-9a-f-]{36}$`, arn)
	assert.Equal(t, gatypes.AcceleratorStatusInProgress, acc.Status)
	assert.True(t, aws.ToBool(acc.Enabled))
	assert.Regexp(t, `^a[0-9a-f]{16}\.awsglobalaccelerator\.com$`, aws.ToString(acc.DnsName))
	assert.Regexp(t, `^a[0-9a-f]{16}\.dualstack\.awsglobalaccelerator\.com$`, aws.ToString(acc.DualStackDnsName))
	require.Len(t, acc.IpSets, 2)
	assert.Equal(t, gatypes.IpAddressFamilyIPv4, acc.IpSets[0].IpAddressFamily)
	assert.Len(t, acc.IpSets[0].IpAddresses, 2)
	assert.Equal(t, gatypes.IpAddressFamilyIPv6, acc.IpSets[1].IpAddressFamily)
	assert.Equal(t, f.clock.now().Unix(), acc.CreatedTime.Unix())

	f.clock.advance(time.Minute)

	got, err := f.client.DescribeAccelerator(ctx, &gasdk.DescribeAcceleratorInput{AcceleratorArn: acc.AcceleratorArn})
	require.NoError(t, err)
	assert.Equal(t, gatypes.AcceleratorStatusDeployed, got.Accelerator.Status)

	updated, err := f.client.UpdateAccelerator(ctx, &gasdk.UpdateAcceleratorInput{
		AcceleratorArn: acc.AcceleratorArn,
		Name:           aws.String("web2"),
		IpAddressType:  gatypes.IpAddressTypeIpv4,
		Enabled:        aws.Bool(false),
	})
	require.NoError(t, err)
	assert.Equal(t, "web2", aws.ToString(updated.Accelerator.Name))
	assert.False(t, aws.ToBool(updated.Accelerator.Enabled))
	assert.Nil(t, updated.Accelerator.DualStackDnsName)
	assert.Len(t, updated.Accelerator.IpSets, 1)
	assert.Equal(t, gatypes.AcceleratorStatusInProgress, updated.Accelerator.Status)
	require.Len(t, updated.Accelerator.Events, 1)
	assert.Contains(t, aws.ToString(updated.Accelerator.Events[0].Message), "DUAL_STACK")

	list, err := f.client.ListAccelerators(ctx, &gasdk.ListAcceleratorsInput{})
	require.NoError(t, err)
	require.Len(t, list.Accelerators, 1)

	_, err = f.client.DeleteAccelerator(ctx, &gasdk.DeleteAcceleratorInput{AcceleratorArn: acc.AcceleratorArn})
	require.NoError(t, err)

	_, err = f.client.DescribeAccelerator(ctx, &gasdk.DescribeAcceleratorInput{AcceleratorArn: acc.AcceleratorArn})
	assert.Equal(t, "AcceleratorNotFoundException", apiCode(err))
}

func testAcceleratorIPAllocation(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	seen := map[string]bool{}

	for range 3 {
		out, err := f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{
			Name: aws.String("ips"), IpAddressType: gatypes.IpAddressTypeDualStack,
		})
		require.NoError(t, err)

		for _, set := range out.Accelerator.IpSets {
			for _, ip := range set.IpAddresses {
				assert.False(t, seen[ip], "address %s handed out twice", ip)
				seen[ip] = true
			}
		}
	}

	first, err := f.client.ListAccelerators(ctx, &gasdk.ListAcceleratorsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"75.2.0.1", "75.2.0.2"}, first.Accelerators[0].IpSets[0].IpAddresses)
	assert.Equal(t, []string{"75.2.0.3", "75.2.0.4"}, first.Accelerators[1].IpSets[0].IpAddresses)
	assert.Equal(t, []string{"2600:9000:a000::0:1", "2600:9000:a000::0:2"}, first.Accelerators[0].IpSets[1].IpAddresses)

	_, err = f.client.UpdateAccelerator(ctx, &gasdk.UpdateAcceleratorInput{
		AcceleratorArn: first.Accelerators[0].AcceleratorArn, Enabled: aws.Bool(false),
	})
	require.NoError(t, err)
	_, err = f.client.DeleteAccelerator(
		ctx,
		&gasdk.DeleteAcceleratorInput{AcceleratorArn: first.Accelerators[0].AcceleratorArn},
	)
	require.NoError(t, err)

	reused, err := f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{Name: aws.String("reuse")})
	require.NoError(t, err)
	assert.Equal(t, []string{"75.2.0.1", "75.2.0.2"}, reused.Accelerator.IpSets[0].IpAddresses)
}

func testAcceleratorAttributes(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()

	created, err := f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{Name: aws.String("attrs")})
	require.NoError(t, err)

	arn := created.Accelerator.AcceleratorArn

	attrs, err := f.client.DescribeAcceleratorAttributes(
		ctx,
		&gasdk.DescribeAcceleratorAttributesInput{AcceleratorArn: arn},
	)
	require.NoError(t, err)
	assert.False(t, aws.ToBool(attrs.AcceleratorAttributes.FlowLogsEnabled))

	_, err = f.client.UpdateAcceleratorAttributes(ctx, &gasdk.UpdateAcceleratorAttributesInput{
		AcceleratorArn: arn, FlowLogsEnabled: aws.Bool(true),
	})
	assert.Equal(t, "InvalidArgumentException", apiCode(err), "flow logs need a bucket and prefix")

	upd, err := f.client.UpdateAcceleratorAttributes(ctx, &gasdk.UpdateAcceleratorAttributesInput{
		AcceleratorArn: arn, FlowLogsEnabled: aws.Bool(true),
		FlowLogsS3Bucket: aws.String("logs"), FlowLogsS3Prefix: aws.String("ga/"),
	})
	require.NoError(t, err)
	assert.True(t, aws.ToBool(upd.AcceleratorAttributes.FlowLogsEnabled))

	got, err := f.client.DescribeAcceleratorAttributes(
		ctx,
		&gasdk.DescribeAcceleratorAttributesInput{AcceleratorArn: arn},
	)
	require.NoError(t, err)
	assert.Equal(t, "logs", aws.ToString(got.AcceleratorAttributes.FlowLogsS3Bucket))
	assert.Equal(t, "ga/", aws.ToString(got.AcceleratorAttributes.FlowLogsS3Prefix))
}

func testAcceleratorListPagination(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()

	for i := range 7 {
		_, err := f.client.CreateAccelerator(
			ctx,
			&gasdk.CreateAcceleratorInput{Name: aws.String(strings.Repeat("a", i+1))},
		)
		require.NoError(t, err)
	}

	var names []string

	var token *string

	pages := 0

	for {
		out, err := f.client.ListAccelerators(
			ctx,
			&gasdk.ListAcceleratorsInput{MaxResults: aws.Int32(3), NextToken: token},
		)
		require.NoError(t, err)

		pages++

		for _, a := range out.Accelerators {
			names = append(names, aws.ToString(a.Name))
		}

		if out.NextToken == nil {
			break
		}

		token = out.NextToken
	}

	assert.Equal(t, 3, pages)
	assert.Len(t, names, 7)
	assert.Equal(t, "a", names[0])

	_, err := f.client.ListAccelerators(ctx, &gasdk.ListAcceleratorsInput{NextToken: aws.String("!!not-a-token")})
	assert.Equal(t, "InvalidNextTokenException", apiCode(err))

	_, err = f.client.ListAccelerators(ctx, &gasdk.ListAcceleratorsInput{MaxResults: aws.Int32(101)})
	assert.Equal(t, "InvalidArgumentException", apiCode(err))
}

func testAcceleratorIdempotency(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()
	in := &gasdk.CreateAcceleratorInput{Name: aws.String("once"), IdempotencyToken: aws.String("tok-1")}

	first, err := f.client.CreateAccelerator(ctx, in)
	require.NoError(t, err)

	second, err := f.client.CreateAccelerator(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.Accelerator.AcceleratorArn), aws.ToString(second.Accelerator.AcceleratorArn))
	assert.Equal(t, 1, f.backend.AcceleratorCount())

	_, err = f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{
		Name: aws.String("other"), IdempotencyToken: aws.String("tok-1"),
	})
	assert.Equal(t, "InvalidArgumentException", apiCode(err))
}

func testAcceleratorErrors(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()

	cases := []struct {
		call func() error
		name string
		code string
	}{
		{func() error {
			_, err := f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{Name: aws.String("-bad-")})

			return err
		}, "bad_name", "InvalidArgumentException"},
		{func() error {
			_, err := f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{
				Name: aws.String("ok"), IpAddresses: []string{"198.51.100.1"},
			})

			return err
		}, "ip_outside_byoip", "InvalidArgumentException"},
		{func() error {
			_, err := f.client.DescribeAccelerator(ctx, &gasdk.DescribeAcceleratorInput{
				AcceleratorArn: aws.String("arn:aws:globalaccelerator::123456789012:accelerator/nope"),
			})

			return err
		}, "describe_missing", "AcceleratorNotFoundException"},
		{func() error {
			_, err := f.client.DeleteAccelerator(ctx, &gasdk.DeleteAcceleratorInput{
				AcceleratorArn: aws.String("arn:aws:globalaccelerator::123456789012:accelerator/nope"),
			})

			return err
		}, "delete_missing", "AcceleratorNotFoundException"},
		{func() error {
			_, err := f.client.UpdateAcceleratorAttributes(ctx, &gasdk.UpdateAcceleratorAttributesInput{
				AcceleratorArn: aws.String("arn:aws:globalaccelerator::123456789012:accelerator/nope"),
			})

			return err
		}, "attributes_missing", "AcceleratorNotFoundException"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.code, apiCode(tc.call()))
		})
	}

	created, err := f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{Name: aws.String("guarded")})
	require.NoError(t, err)

	_, err = f.client.DeleteAccelerator(
		ctx,
		&gasdk.DeleteAcceleratorInput{AcceleratorArn: created.Accelerator.AcceleratorArn},
	)
	assert.Equal(t, "AcceleratorNotDisabledException", apiCode(err))

	_, err = f.client.CreateCustomRoutingAccelerator(ctx, &gasdk.CreateCustomRoutingAcceleratorInput{
		Name: aws.String("c"), IpAddressType: gatypes.IpAddressTypeDualStack,
	})
	assert.Equal(t, "InvalidArgumentException", apiCode(err), "custom routing accelerators are IPv4 only")

	_, err = f.client.DescribeCustomRoutingAccelerator(ctx, &gasdk.DescribeCustomRoutingAcceleratorInput{
		AcceleratorArn: created.Accelerator.AcceleratorArn,
	})
	assert.Equal(
		t,
		"AcceleratorNotFoundException",
		apiCode(err),
		"a standard accelerator is not a custom routing accelerator",
	)
}

func testAcceleratorTags(t *testing.T, f *fixture) {
	t.Helper()

	ctx := t.Context()

	created, err := f.client.CreateAccelerator(ctx, &gasdk.CreateAcceleratorInput{Name: aws.String("tagged")})
	require.NoError(t, err)

	arn := created.Accelerator.AcceleratorArn

	_, err = f.client.TagResource(ctx, &gasdk.TagResourceInput{ResourceArn: arn, Tags: []gatypes.Tag{
		{Key: aws.String("b"), Value: aws.String("2")}, {Key: aws.String("a"), Value: aws.String("1")},
	}})
	require.NoError(t, err)

	got, err := f.client.ListTagsForResource(ctx, &gasdk.ListTagsForResourceInput{ResourceArn: arn})
	require.NoError(t, err)
	require.Len(t, got.Tags, 2)
	assert.Equal(t, "a", aws.ToString(got.Tags[0].Key))

	_, err = f.client.UntagResource(ctx, &gasdk.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"a"}})
	require.NoError(t, err)

	got, err = f.client.ListTagsForResource(ctx, &gasdk.ListTagsForResourceInput{ResourceArn: arn})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "b", aws.ToString(got.Tags[0].Key))

	_, err = f.client.TagResource(ctx, &gasdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:globalaccelerator::123456789012:accelerator/nope"),
		Tags:        []gatypes.Tag{{Key: aws.String("x"), Value: aws.String("y")}},
	})
	assert.Equal(t, "AcceleratorNotFoundException", apiCode(err))

	_, err = f.client.TagResource(ctx, &gasdk.TagResourceInput{
		ResourceArn: arn, Tags: []gatypes.Tag{{Key: aws.String("aws:reserved"), Value: aws.String("y")}},
	})
	assert.Equal(t, "InvalidArgumentException", apiCode(err))
}
