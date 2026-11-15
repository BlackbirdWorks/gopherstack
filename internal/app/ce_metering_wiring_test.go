package app

import (
	"bytes"
	"compress/gzip"
	"encoding/csv"
	"io"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	bcmsdk "github.com/aws/aws-sdk-go-v2/service/bcmdataexports"
	bcmtypes "github.com/aws/aws-sdk-go-v2/service/bcmdataexports/types"
	cursdk "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice"
	curtypes "github.com/aws/aws-sdk-go-v2/service/costandusagereportservice/types"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	bcmbackend "github.com/blackbirdworks/gopherstack/services/bcmdataexports"
	budgetsbackend "github.com/blackbirdworks/gopherstack/services/budgets"
	cebackend "github.com/blackbirdworks/gopherstack/services/ce"
	curbackend "github.com/blackbirdworks/gopherstack/services/cur"
	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

const meteringAccount = "123456789012"

type meteringClock struct{ now atomic.Pointer[time.Time] }

func (c *meteringClock) Now() time.Time { return *c.now.Load() }

func (c *meteringClock) Advance(d time.Duration) {
	n := c.Now().Add(d)
	c.now.Store(&n)
}

type meteringFixture struct {
	clock   *meteringClock
	ce      *cebackend.InMemoryBackend
	ec2     *ec2backend.InMemoryBackend
	s3      *s3backend.InMemoryBackend
	s3H     *s3backend.S3Handler
	ddb     *ddbbackend.InMemoryDB
	lambda  *lambdabackend.InMemoryBackend
	day     string
	nextDay string
}

func newMeteringFixture(t *testing.T) *meteringFixture {
	t.Helper()

	return newMeteringFixtureAt(t, time.Date(2031, 5, 15, 10, 0, 0, 0, time.UTC))
}

func newMeteringFixtureAt(t *testing.T, start time.Time) *meteringFixture {
	t.Helper()

	clock := &meteringClock{}
	clock.now.Store(&start)

	ce := cebackend.NewInMemoryBackend(meteringAccount, "us-east-1")
	ce.SetClock(clock.Now)

	ec2Bk := ec2backend.NewInMemoryBackend(meteringAccount, "us-east-1")
	s3Bk := s3backend.NewInMemoryBackend(nil)
	s3H := s3backend.NewHandler(s3Bk)
	ddb := ddbbackend.NewInMemoryDB()
	lambdaBk := lambdabackend.NewInMemoryBackend(
		nil,
		nil,
		lambdabackend.DefaultSettings(),
		meteringAccount,
		"us-east-1",
	)
	t.Cleanup(func() { lambdaBk.Close(t.Context()) })

	wireCostMetering(map[string]service.Registerable{
		"Ce":       cebackend.NewHandler(ce),
		"EC2":      ec2backend.NewHandler(ec2Bk),
		"S3":       s3H,
		"DynamoDB": ddbbackend.NewHandler(ddb),
		"Lambda":   lambdabackend.NewHandler(lambdaBk),
	}, true)

	return &meteringFixture{
		clock: clock, ce: ce, ec2: ec2Bk, s3: s3Bk, s3H: s3H, ddb: ddb, lambda: lambdaBk,
		day: start.Format("2006-01-02"), nextDay: start.AddDate(0, 0, 1).Format("2006-01-02"),
	}
}

type usageTotal struct {
	op        string
	cost, qty float64
}

func (f *meteringFixture) totals(service string) map[string]usageTotal {
	out := map[string]usageTotal{}

	for _, e := range f.ce.LedgerEntries(f.day, "2032-01-01") {
		if e.ResourceID == "" || e.Service != service {
			continue
		}

		t := out[e.UsageType]
		t.cost += e.UnblendedCost
		t.qty += e.UsageQuantity
		t.op = e.Operation
		out[e.UsageType] = t
	}

	return out
}

func (f *meteringFixture) rows(service string) []cebackend.CostEntry {
	var out []cebackend.CostEntry

	for _, e := range f.ce.LedgerEntries(f.day, f.nextDay) {
		if e.ResourceID != "" && e.Service == service {
			out = append(out, e)
		}
	}

	return out
}

func (f *meteringFixture) runInstance(t *testing.T) string {
	t.Helper()

	f.ec2.StartLifecycleReconciler(t.Context())

	instances, err := f.ec2.RunInstances("ami-0123456789abcdef0", "t3.medium", "", 1)
	require.NoError(t, err)
	require.Len(t, instances, 1)
	require.NoError(t, f.ec2.CreateTags([]string{instances[0].ID}, map[string]string{"Team": "core"}))

	id := instances[0].ID
	require.Eventually(t, func() bool {
		return len(f.ec2.DescribeInstances([]string{id}, "running")) == 1
	}, 5*time.Second, time.Millisecond)

	return id
}

func TestWireCostMeteringEC2InstanceHoursStopWhenStopped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stopAfter time.Duration
		wantHours float64
	}{
		{name: "running throughout", stopAfter: 0, wantHours: 6},
		{name: "stopped after two hours", stopAfter: 2 * time.Hour, wantHours: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newMeteringFixture(t)
			id := f.runInstance(t)

			if tc.stopAfter > 0 {
				f.clock.Advance(tc.stopAfter)
				f.ce.Accrue()

				_, err := f.ec2.StopInstances([]string{id})
				require.NoError(t, err)
				require.Eventually(t, func() bool {
					return len(f.ec2.DescribeInstances([]string{id}, "stopped")) == 1
				}, 5*time.Second, time.Millisecond)
			}

			f.clock.Advance(6*time.Hour - tc.stopAfter)

			rows := f.rows(ceServiceEC2)
			require.Len(t, rows, 1)
			assert.Equal(t, id, rows[0].ResourceID)
			assert.Equal(t, "t3.medium", rows[0].InstanceType)
			assert.Equal(t, "us-east-1a", rows[0].AvailabilityZone)
			assert.Equal(t, "core", rows[0].Tags["Team"])
			assert.InDelta(t, tc.wantHours, rows[0].UsageQuantity, 1e-6)
			assert.InDelta(t, tc.wantHours*0.0416, rows[0].UnblendedCost, 1e-6)
		})
	}
}

func TestWireCostMeteringEBSVolumeGBMonths(t *testing.T) {
	t.Parallel()

	f := newMeteringFixture(t)
	f.ec2.StartLifecycleReconciler(t.Context())

	vol, err := f.ec2.CreateVolume("us-east-1a", "gp3", 100, "")
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		v := f.ec2.DescribeVolumes([]string{vol.ID})

		return len(v) == 1 && v[0].State == "available"
	}, 5*time.Second, time.Millisecond)

	f.clock.Advance(2 * time.Hour)

	rows := f.rows(ceServiceEC2Other)
	require.Len(t, rows, 1)
	assert.Equal(t, vol.ID, rows[0].ResourceID)
	assert.Equal(t, "EBS:VolumeUsage.gp3", rows[0].UsageType)
	assert.InDelta(t, 100.0/730*2, rows[0].UsageQuantity, 1e-9)
	assert.InDelta(t, 100.0/730*2*0.08, rows[0].UnblendedCost, 1e-9)
}

func TestWireCostMeteringS3StorageAndRequests(t *testing.T) {
	t.Parallel()

	f := newMeteringFixture(t)
	httpClient := sdktest.ServeInMemory(t, f.s3H)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithHTTPClient(httpClient),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := sdk_s3.NewFromConfig(cfg, func(o *sdk_s3.Options) {
		o.BaseEndpoint = aws.String(sdktest.InMemoryEndpoint)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("meter-bkt")})
	require.NoError(t, err)

	body := bytes.Repeat([]byte("a"), 1<<20)
	_, err = client.PutObject(t.Context(), &sdk_s3.PutObjectInput{
		Bucket: aws.String("meter-bkt"), Key: aws.String("k"), Body: bytes.NewReader(body),
	})
	require.NoError(t, err)

	for range 3 {
		out, getErr := client.GetObject(t.Context(), &sdk_s3.GetObjectInput{
			Bucket: aws.String("meter-bkt"), Key: aws.String("k"),
		})
		require.NoError(t, getErr)
		_, _ = io.Copy(io.Discard, out.Body)
		require.NoError(t, out.Body.Close())
	}

	f.clock.Advance(730 * time.Hour / 24)

	byUsage := f.totals(ceServiceS3)

	tier2 := byUsage[s3Tier2UsageType]
	assert.InDelta(t, 3, tier2.qty, 1e-9)
	assert.InDelta(t, 3*0.0004/1000, tier2.cost, 1e-12)
	assert.Equal(t, "GetObject", tier2.op)

	assert.InDelta(t, 2, byUsage[s3Tier1UsageType].qty, 1e-9, "CreateBucket and PutObject")

	storage := byUsage["TimedStorage-ByteHrs"]
	assert.InDelta(t, (1.0/1024)*(730.0/24)/730, storage.qty, 1e-9)
	assert.InDelta(t, storage.qty*0.023, storage.cost, 1e-12)
}

func TestWireCostMeteringDynamoDBOnDemandUnits(t *testing.T) {
	t.Parallel()

	f := newMeteringFixture(t)

	_, err := f.ddb.CreateTable(t.Context(), &ddbsdk.CreateTableInput{
		TableName:   aws.String("meter-tbl"),
		BillingMode: ddbtypes.BillingModePayPerRequest,
		KeySchema:   []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		Tags: []ddbtypes.Tag{{Key: aws.String("Team"), Value: aws.String("data")}},
	})
	require.NoError(t, err)

	for range 4 {
		_, err = f.ddb.PutItem(t.Context(), &ddbsdk.PutItemInput{
			TableName: aws.String("meter-tbl"),
			Item:      map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "a"}},
		})
		require.NoError(t, err)
	}

	_, err = f.ddb.GetItem(t.Context(), &ddbsdk.GetItemInput{
		TableName: aws.String("meter-tbl"),
		Key:       map[string]ddbtypes.AttributeValue{"id": &ddbtypes.AttributeValueMemberS{Value: "a"}},
	})
	require.NoError(t, err)

	rows := f.rows(ceServiceDynamoDB)
	require.NotEmpty(t, rows)

	byUsage := f.totals(ceServiceDynamoDB)
	assert.InDelta(t, 4, byUsage[ddbWriteUnits].qty, 1e-9)
	assert.InDelta(t, 4*0.00000125, byUsage[ddbWriteUnits].cost, 1e-12)
	assert.Positive(t, byUsage[ddbReadUnits].qty)

	for _, r := range rows {
		assert.Contains(t, r.ResourceID, ":table/meter-tbl")
		assert.Equal(t, "data", r.Tags["Team"])
	}
}

func TestWireCostMeteringLambdaRequestsAndDuration(t *testing.T) {
	t.Parallel()

	f := newMeteringFixture(t)

	require.NoError(t, f.lambda.CreateFunction(&lambdabackend.FunctionConfiguration{
		FunctionName: "meter-fn", PackageType: lambdabackend.PackageTypeImage, ImageURI: "x:latest",
		MemorySize: 512, Tags: map[string]string{"Team": "fn"},
		FunctionArn: "arn:aws:lambda:us-east-1:123456789012:function:meter-fn",
	}))

	for range 2 {
		_, _, _, _, err := f.lambda.InvokeFunctionWithQualifier(
			t.Context(), "meter-fn", "", "", "", lambdabackend.InvocationTypeRequestResponse, []byte(`{}`),
		)
		require.Error(t, err, "the image runtime is unavailable in unit tests")
	}

	recordLambda(f.ce, lambdabackend.InvocationUsage{
		FunctionName: "meter-fn", FunctionARN: "arn:aws:lambda:us-east-1:123456789012:function:meter-fn",
		Region: "us-east-1", Account: meteringAccount, Architecture: "x86_64", MemoryMB: 512, Duration: 2 * time.Second,
		Tags: map[string]string{"Team": "fn"},
	})

	byUsage := f.totals(ceServiceLambda)
	assert.InDelta(t, 3, byUsage["Request"].qty, 1e-9, "two failed invocations plus the recorded one")
	assert.InDelta(t, 3*0.0000002, byUsage["Request"].cost, 1e-12)
	assert.InDelta(t, 0.5*2, byUsage["Lambda-GB-Second"].qty, 1e-9)
	assert.InDelta(t, 0.5*2*0.0000166667, byUsage["Lambda-GB-Second"].cost, 1e-12)

	for _, r := range f.rows(ceServiceLambda) {
		assert.Contains(t, r.ResourceID, ":function:meter-fn")
		assert.Equal(t, "fn", r.Tags["Team"])
	}
}

func TestMeteredResourceAndTagReachCostAndUsageReport(t *testing.T) {
	t.Parallel()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	f := newMeteringFixtureAt(t, today)

	instanceID := f.runInstance(t)

	f.clock.Advance(2 * time.Hour)

	from, to := today, today.AddDate(0, 0, 1)

	spend, ok := budgetsCESpend{ce: f.ce}.Spend(from, to, map[string][]string{
		"Service": {ceServiceEC2}, "TagKeyValue": {"user:Team$core"},
	}, "UnblendedCost")
	assert.True(t, ok)
	assert.InDelta(t, 2*0.0416, spend, 1e-9)

	cur := curbackend.NewInMemoryBackend(meteringAccount, "us-east-1")
	store := s3backend.NewInMemoryBackend(nil)
	_, err := store.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("cur-bucket")})
	require.NoError(t, err)

	cur.SetObjectStore(store)
	cur.SetCostSource(ceCostSource{ce: f.ce})

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := cursdk.NewFromConfig(cfg, func(o *cursdk.Options) {
		o.BaseEndpoint = aws.String(sdktest.Serve(t, curbackend.NewHandler(cur)))
	})

	_, err = client.PutReportDefinition(t.Context(), &cursdk.PutReportDefinitionInput{
		ReportDefinition: &curtypes.ReportDefinition{
			ReportName: aws.String("daily"), TimeUnit: curtypes.TimeUnitDaily, Format: curtypes.ReportFormatCsv,
			Compression: curtypes.CompressionFormatGzip, S3Bucket: aws.String("cur-bucket"),
			S3Prefix: aws.String("reports"), S3Region: curtypes.AWSRegionUsStandard,
			AdditionalSchemaElements: []curtypes.SchemaElement{curtypes.SchemaElementResources},
		},
	})
	require.NoError(t, err)

	cur.DeliverDue(t.Context())

	list, err := store.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{Bucket: aws.String("cur-bucket")})
	require.NoError(t, err)

	var rows [][]string

	for _, o := range list.Contents {
		if key := aws.ToString(o.Key); len(key) > 7 && key[len(key)-7:] == ".csv.gz" {
			obj, getErr := store.GetObject(
				t.Context(),
				&sdk_s3.GetObjectInput{Bucket: aws.String("cur-bucket"), Key: o.Key},
			)
			require.NoError(t, getErr)

			raw, readErr := io.ReadAll(obj.Body)
			require.NoError(t, readErr)

			zr, zerr := gzip.NewReader(bytes.NewReader(raw))
			require.NoError(t, zerr)

			rows, err = csv.NewReader(zr).ReadAll()
			require.NoError(t, err)
		}
	}

	require.NotEmpty(t, rows)

	resCol := slices.Index(rows[0], "lineItem/ResourceId")
	tagCol := slices.Index(rows[0], "resourceTags/user:Team")
	require.GreaterOrEqual(t, resCol, 0)
	require.GreaterOrEqual(t, tagCol, 0)

	found := false

	for _, r := range rows[1:] {
		if r[resCol] == instanceID {
			found = true

			assert.Equal(t, "core", r[tagCol])
		}
	}

	assert.True(t, found, "no CUR row carries the metered instance id")
}

func TestBudgetsRICommitmentsFromMeteredInstanceHours(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		riType       string
		riAZ         string
		budgetType   string
		count        int
		want         float64
		wantReported bool
	}{
		{
			name:         "utilization partly used",
			riType:       "t3.medium",
			count:        2,
			budgetType:   "RI_UTILIZATION",
			want:         50,
			wantReported: true,
		},
		{
			name:         "utilization fully used",
			riType:       "t3.medium",
			count:        1,
			budgetType:   "RI_UTILIZATION",
			want:         100,
			wantReported: true,
		},
		{name: "coverage", riType: "t3.medium", count: 1, budgetType: "RI_COVERAGE", want: 100, wantReported: true},
		{
			name:         "zonal match",
			riType:       "t3.medium",
			riAZ:         "us-east-1a",
			count:        2,
			budgetType:   "RI_UTILIZATION",
			want:         50,
			wantReported: true,
		},
		{
			name:         "zonal mismatch",
			riType:       "t3.medium",
			riAZ:         "us-east-1b",
			count:        1,
			budgetType:   "RI_UTILIZATION",
			wantReported: true,
		},
		{name: "other type", riType: "m5.large", count: 1, budgetType: "RI_COVERAGE", wantReported: true},
		{name: "no reservations", count: 0, budgetType: "RI_UTILIZATION"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			start := time.Now().UTC()
			f := newMeteringFixtureAt(t, start)
			f.runInstance(t)
			f.clock.Advance(4 * time.Hour)

			var ris []*ec2backend.ReservedInstance
			if tc.count > 0 {
				ris = append(ris, &ec2backend.ReservedInstance{
					State: "active", InstanceType: tc.riType, AvailabilityZone: tc.riAZ, InstanceCount: tc.count,
					Start: start, End: start.AddDate(1, 0, 0),
				})
			}

			src := budgetsRICommitments{ce: f.ce, reservations: func() []*ec2backend.ReservedInstance { return ris }}
			got, ok := src.Commitment(start, start.Add(4*time.Hour), tc.budgetType, nil)

			assert.Equal(t, tc.wantReported, ok)
			assert.InDelta(t, tc.want, got, 1e-6)
		})
	}
}

func TestMeteredResourceAndTagReachDataExportsRows(t *testing.T) {
	t.Parallel()

	today := time.Now().UTC().Truncate(24 * time.Hour)
	f := newMeteringFixtureAt(t, today)
	instanceID := f.runInstance(t)

	f.clock.Advance(2 * time.Hour)

	store := s3backend.NewInMemoryBackend(nil)
	_, err := store.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("exports-bucket")})
	require.NoError(t, err)

	bcm := bcmbackend.NewInMemoryBackend(meteringAccount, "us-east-1")
	bcm.SetObjectStore(store)
	bcm.SetCostSource(ceCostSource{ce: f.ce})

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	client := bcmsdk.NewFromConfig(cfg, func(o *bcmsdk.Options) {
		o.BaseEndpoint = aws.String(sdktest.Serve(t, bcmbackend.NewHandler(bcm)))
	})

	_, err = client.CreateExport(t.Context(), &bcmsdk.CreateExportInput{Export: &bcmtypes.Export{
		Name: aws.String("metered"),
		DataQuery: &bcmtypes.DataQuery{
			TableConfigurations: map[string]map[string]string{"COST_AND_USAGE_REPORT": {"INCLUDE_RESOURCES": "TRUE"}},
			QueryStatement: aws.String(
				"SELECT line_item_resource_id, resource_tags FROM COST_AND_USAGE_REPORT WHERE line_item_resource_id = '" +
					instanceID + "'",
			),
		},
		DestinationConfigurations: &bcmtypes.DestinationConfigurations{S3Destination: &bcmtypes.S3Destination{
			S3Bucket: aws.String("exports-bucket"), S3Prefix: aws.String("out"), S3Region: aws.String("us-east-1"),
			S3OutputConfigurations: &bcmtypes.S3OutputConfigurations{
				OutputType: bcmtypes.S3OutputTypeCustom, Format: bcmtypes.FormatOptionTextOrCsv,
				Compression: bcmtypes.CompressionOptionGzip, Overwrite: bcmtypes.OverwriteOptionOverwriteReport,
			},
		}},
		RefreshCadence: &bcmtypes.RefreshCadence{Frequency: bcmtypes.FrequencyOptionSynchronous},
	}})
	require.NoError(t, err)

	for range 5 {
		bcm.ProcessExecutions(t.Context())
	}

	list, err := store.ListObjectsV2(t.Context(), &sdk_s3.ListObjectsV2Input{Bucket: aws.String("exports-bucket")})
	require.NoError(t, err)

	var rows [][]string

	for _, o := range list.Contents {
		if !strings.HasSuffix(aws.ToString(o.Key), ".csv.gz") {
			continue
		}

		obj, getErr := store.GetObject(
			t.Context(),
			&sdk_s3.GetObjectInput{Bucket: aws.String("exports-bucket"), Key: o.Key},
		)
		require.NoError(t, getErr)

		raw, readErr := io.ReadAll(obj.Body)
		require.NoError(t, readErr)

		zr, zerr := gzip.NewReader(bytes.NewReader(raw))
		require.NoError(t, zerr)

		rows, err = csv.NewReader(zr).ReadAll()
		require.NoError(t, err)
	}

	require.Greater(t, len(rows), 1)

	for _, r := range rows[1:] {
		assert.Equal(t, instanceID, r[0])
		assert.JSONEq(t, `{"user:Team":"core"}`, r[1])
	}
}

func TestBudgetsExpressionFiltersOnMeteredTagsAndResources(t *testing.T) {
	t.Parallel()

	svc := func(v string) budgetsbackend.Expression {
		return budgetsbackend.Expression{Dimensions: &budgetsbackend.ValuesFilter{Key: "SERVICE", Values: []string{v}}}
	}
	tag := func(v string) *budgetsbackend.Expression {
		return &budgetsbackend.Expression{Tags: &budgetsbackend.ValuesFilter{Key: "Team", Values: []string{v}}}
	}
	res := func(v string) budgetsbackend.Expression {
		return budgetsbackend.Expression{
			Dimensions: &budgetsbackend.ValuesFilter{Key: "RESOURCE_ID", Values: []string{v}},
		}
	}

	tests := []struct {
		expr     func(id string) *budgetsbackend.Expression
		name     string
		want     float64
		relative bool
	}{
		{
			name: "and tag", want: 2 * 0.0416,
			expr: func(string) *budgetsbackend.Expression {
				return &budgetsbackend.Expression{And: []budgetsbackend.Expression{svc(ceServiceEC2), *tag("core")}}
			},
		},
		{
			name: "not tag", want: -2 * 0.0416, relative: true,
			expr: func(string) *budgetsbackend.Expression {
				return &budgetsbackend.Expression{And: []budgetsbackend.Expression{
					svc(ceServiceEC2), {Not: tag("core")},
				}}
			},
		},
		{
			name: "or resource", want: 2 * 0.0416,
			expr: func(id string) *budgetsbackend.Expression {
				return &budgetsbackend.Expression{And: []budgetsbackend.Expression{
					svc(ceServiceEC2), {Or: []budgetsbackend.Expression{res(id), res("i-none")}},
				}}
			},
		},
		{
			name: "other resource", want: 0,
			expr: func(string) *budgetsbackend.Expression {
				return &budgetsbackend.Expression{And: []budgetsbackend.Expression{svc(ceServiceEC2), res("i-none")}}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			today := time.Now().UTC().Truncate(24 * time.Hour)
			f := newMeteringFixtureAt(t, today)
			id := f.runInstance(t)

			f.clock.Advance(2 * time.Hour)

			got, ok := budgetsCESpend{ce: f.ce}.SpendExpression(
				today, today.AddDate(0, 0, 1), tc.expr(id), nil, "UnblendedCost",
			)
			assert.True(t, ok)

			want := tc.want

			if tc.relative {
				all, _ := budgetsCESpend{ce: f.ce}.SpendExpression(
					today, today.AddDate(0, 0, 1), &budgetsbackend.Expression{Dimensions: svc(ceServiceEC2).Dimensions},
					nil, "UnblendedCost",
				)
				want += all
			}

			assert.InDelta(t, want, got, 1e-9)
		})
	}
}
