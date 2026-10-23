package app

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/servicequotas"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	servicequotasbackend "github.com/blackbirdworks/gopherstack/services/servicequotas"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
)

const quotaTestAccount = "000000000000"

type quotaRig struct {
	sq     *servicequotas.Client
	s3     *s3.Client
	ec2    *ec2.Client
	iam    *iam.Client
	ddb    *dynamodb.Client
	sqBack *servicequotasbackend.InMemoryBackend
	byName map[string]service.Registerable
}

func newQuotaRig(t *testing.T, enforce bool) *quotaRig {
	t.Helper()

	sqBack := servicequotasbackend.NewInMemoryBackend(quotaTestAccount, "us-east-1")
	sqBack.SetLifecycleDelay(time.Millisecond)

	byName := map[string]service.Registerable{
		"ServiceQuotas": servicequotasbackend.NewHandler(sqBack),
		"S3":            s3backend.NewHandler(s3backend.NewInMemoryBackend(nil)),
		"EC2":           ec2backend.NewHandler(ec2backend.NewInMemoryBackend(quotaTestAccount, "us-east-1")),
		"IAM":           iambackend.NewHandler(iambackend.NewInMemoryBackendWithConfig(quotaTestAccount)),
		"DynamoDB":      ddbbackend.NewHandler(ddbbackend.NewInMemoryDB()),
		"SNS":           snsbackend.NewHandler(snsbackend.NewInMemoryBackend()),
	}

	wireServiceQuotas(enforce, byName)

	regs := make([]service.Registerable, 0, len(byName))
	for _, r := range byName {
		regs = append(regs, r)
	}

	url := sdktest.Serve(t, regs...)
	cfg, err := awscfg.LoadDefaultConfig(t.Context(), awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")))
	require.NoError(t, err)

	return &quotaRig{
		sqBack: sqBack, byName: byName,
		sq:  servicequotas.NewFromConfig(cfg, func(o *servicequotas.Options) { o.BaseEndpoint = aws.String(url) }),
		s3:  s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = aws.String(url); o.UsePathStyle = true }),
		ec2: ec2.NewFromConfig(cfg, func(o *ec2.Options) { o.BaseEndpoint = aws.String(url) }),
		iam: iam.NewFromConfig(cfg, func(o *iam.Options) { o.BaseEndpoint = aws.String(url) }),
		ddb: dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String(url) }),
	}
}

func quotaErrCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

type quotaCase struct {
	create func(ctx context.Context, r *quotaRig, i int) error
	name   string
	code   string
	limit  int
	// existing counts resources that already exist before the test creates any.
	existing int
}

func quotaCases() []quotaCase {
	return []quotaCase{
		{
			name: "s3 buckets", code: "TooManyBuckets", limit: 100,
			create: func(ctx context.Context, r *quotaRig, i int) error {
				_, err := r.s3.CreateBucket(ctx,
					&s3.CreateBucketInput{Bucket: aws.String("quota-bkt-" + strconv.Itoa(i))})

				return err
			},
		},
		{
			name: "ec2 elastic ips", code: "AddressLimitExceeded", limit: 5,
			create: func(ctx context.Context, r *quotaRig, _ int) error {
				_, err := r.ec2.AllocateAddress(ctx, &ec2.AllocateAddressInput{})

				return err
			},
		},
		{
			name: "ec2 vpcs", code: "VpcLimitExceeded", limit: 5, existing: 1,
			create: func(ctx context.Context, r *quotaRig, i int) error {
				cidr := "10." + strconv.Itoa(i) + ".0.0/16"
				_, err := r.ec2.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String(cidr)})

				return err
			},
		},
		{
			name: "iam groups", code: "LimitExceeded", limit: 300,
			create: func(ctx context.Context, r *quotaRig, i int) error {
				name := "quota-grp-" + strconv.Itoa(i)
				_, err := r.iam.CreateGroup(ctx, &iam.CreateGroupInput{GroupName: aws.String(name)})

				return err
			},
		},
		{
			name: "dynamodb tables", code: "LimitExceededException", limit: 2500,
			create: func(ctx context.Context, r *quotaRig, i int) error {
				_, err := r.ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
					TableName: aws.String(
						"quota-tbl-" + strconv.Itoa(i),
					), BillingMode: ddbtypes.BillingModePayPerRequest,
					AttributeDefinitions: []ddbtypes.AttributeDefinition{
						{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
					},
					KeySchema: []ddbtypes.KeySchemaElement{
						{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
					},
				})

				return err
			},
		},
	}
}

func TestQuotaEnforcement(t *testing.T) {
	t.Parallel()

	for _, tc := range quotaCases() {
		for _, enforce := range []bool{true, false} {
			t.Run(tc.name+map[bool]string{true: " enforced", false: " open"}[enforce], func(t *testing.T) {
				t.Parallel()

				r := newQuotaRig(t, enforce)

				for i := range tc.limit - tc.existing {
					require.NoError(t, tc.create(t.Context(), r, i))
				}

				err := tc.create(t.Context(), r, tc.limit)

				if !enforce {
					require.NoError(t, err)

					return
				}

				require.Error(t, err)
				assert.Equal(t, tc.code, quotaErrCode(err))
			})
		}
	}
}

func TestQuotaEnforcement_FollowsApprovedIncrease(t *testing.T) {
	t.Parallel()

	r := newQuotaRig(t, true)

	for i := range 5 {
		_, err := r.ec2.AllocateAddress(t.Context(), &ec2.AllocateAddressInput{})
		require.NoError(t, err, i)
	}

	_, err := r.ec2.AllocateAddress(t.Context(), &ec2.AllocateAddressInput{})
	require.Equal(t, "AddressLimitExceeded", quotaErrCode(err))

	_, err = r.sq.RequestServiceQuotaIncrease(t.Context(), &servicequotas.RequestServiceQuotaIncreaseInput{
		ServiceCode: aws.String("ec2"), QuotaCode: aws.String("L-0263D0A3"), DesiredValue: aws.Float64(6),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, aerr := r.ec2.AllocateAddress(t.Context(), &ec2.AllocateAddressInput{})

		return aerr == nil
	}, 5*time.Second, 5*time.Millisecond)
}

func TestQuotaUtilizationFromBackends(t *testing.T) {
	t.Parallel()

	r := newQuotaRig(t, false)

	for i := range 3 {
		require.NoError(t, quotaCases()[0].create(t.Context(), r, i))
	}

	for range 5 {
		_, err := r.ec2.AllocateAddress(t.Context(), &ec2.AllocateAddressInput{})
		require.NoError(t, err)
	}

	start, err := r.sq.StartQuotaUtilizationReport(t.Context(), &servicequotas.StartQuotaUtilizationReportInput{})
	require.NoError(t, err)

	util := map[string]float64{}

	require.Eventually(t, func() bool {
		out, gerr := r.sq.GetQuotaUtilizationReport(t.Context(), &servicequotas.GetQuotaUtilizationReportInput{
			ReportId: start.ReportId, MaxResults: aws.Int32(100),
		})
		if gerr != nil || len(out.Quotas) == 0 {
			return false
		}

		for _, q := range out.Quotas {
			util[aws.ToString(q.ServiceCode)+"/"+aws.ToString(q.QuotaCode)] = aws.ToFloat64(q.Utilization)
		}

		return true
	}, 5*time.Second, 5*time.Millisecond)

	assert.InDelta(t, 3, util["s3/L-DC2B2D3D"], 0.001)
	assert.InDelta(t, 100, util["ec2/L-0263D0A3"], 0.001)
	assert.InDelta(t, 20, util["vpc/L-F678F1CE"], 0.001)
}

func TestQuotaSNSNotifier(t *testing.T) {
	t.Parallel()

	bk := snsbackend.NewInMemoryBackend()
	topic, err := bk.CreateTopic("alerts", nil)
	require.NoError(t, err)

	tests := []struct {
		name    string
		arn     string
		wantErr bool
	}{
		{name: "existing topic", arn: topic.TopicArn},
		{name: "missing topic", arn: "arn:aws:sns:us-east-1:000000000000:nope", wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			nerr := quotaSNSNotifier{sns: bk}.Notify(t.Context(), tc.arn, "subject", "{}")
			assert.Equal(t, tc.wantErr, nerr != nil)
		})
	}
}

func TestQuotaEnforceFlagDefaultsOff(t *testing.T) {
	t.Parallel()

	assert.False(t, (&CLI{}).EnforceServiceQuotas)
}
