package main

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	asgbackend "github.com/blackbirdworks/gopherstack/services/autoscaling"
	cfnbackend "github.com/blackbirdworks/gopherstack/services/cloudformation"
	elbbackend "github.com/blackbirdworks/gopherstack/services/elb"
	ebbackend "github.com/blackbirdworks/gopherstack/services/eventbridge"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

var errFakeRuntime = errors.New("fake container runtime")

// startRecorder is a container runtime that records which region and function tried to start.
type startRecorder struct {
	container.Runtime

	starts []string
	mu     sync.Mutex
}

func (r *startRecorder) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	var region, fn string

	for _, e := range spec.Env {
		if v, ok := strings.CutPrefix(e, "AWS_REGION="); ok {
			region = v
		}

		if v, ok := strings.CutPrefix(e, "AWS_LAMBDA_FUNCTION_NAME="); ok {
			fn = v
		}
	}

	r.mu.Lock()
	r.starts = append(r.starts, region+"/"+fn)
	r.mu.Unlock()

	return "", errFakeRuntime
}

func (r *startRecorder) started() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.starts...)
}

func (r *startRecorder) has(entry string) bool {
	return slices.Contains(r.started(), entry)
}

// regionalLambda builds a multi-region Lambda handler whose containers are recorded instead of started.
func regionalLambda(t *testing.T) (*lambdabackend.Handler, *startRecorder) {
	t.Helper()

	rec := &startRecorder{}

	pa, err := portalloc.New(19701, 19799)
	require.NoError(t, err)

	bk := lambdabackend.NewInMemoryBackendWithContext(
		t.Context(), rec, pa, lambdabackend.DefaultSettings(), crossAcct, crossHome)
	h := lambdabackend.NewHandler(bk)
	h.DefaultRegion = crossHome
	h.AccountID = crossAcct
	h.EnableRegions()

	t.Cleanup(func() {
		ctx := context.Background()
		h.CloseRegions(ctx)
		bk.Close(ctx)
	})

	return h, rec
}

func regionalBackend(t *testing.T, h *lambdabackend.Handler, region string) *lambdabackend.InMemoryBackend {
	t.Helper()

	bk, ok := h.BackendFor(region).(*lambdabackend.InMemoryBackend)
	require.True(t, ok)

	return bk
}

func createImageFunction(t *testing.T, h *lambdabackend.Handler, region, name string) string {
	t.Helper()

	fnARN := arn.Build("lambda", region, crossAcct, "function:"+name)
	require.NoError(t, regionalBackend(t, h, region).CreateFunction(&lambdabackend.FunctionConfiguration{
		FunctionName: name, FunctionArn: fnARN, PackageType: lambdabackend.PackageTypeImage, ImageURI: "img:latest",
	}))

	return fnARN
}

func TestInitializeServices_SNSInvokesLambdaInARNRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	lambdaH, rec := regionalLambda(t)
	wireSNSToLambdaFirehose(byName["SNS"], lambdaH, byName["Firehose"], byName["SQS"])

	snsH, ok := byName["SNS"].(*snsbackend.Handler)
	require.True(t, ok)

	snsBk, ok := snsH.Backend.(*snsbackend.InMemoryBackend)
	require.True(t, ok)

	tests := []struct {
		name   string
		region string
		other  string
	}{
		{name: "eu-west-1", region: euRegion, other: crossHome},
		{name: "us-west-2", region: usWest2, other: euRegion},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fn := "sns-" + tc.region
			fnARN := createImageFunction(t, lambdaH, tc.region, fn)
			createImageFunction(t, lambdaH, tc.other, fn)

			topic, err := snsBk.CreateTopicInRegion("t-"+tc.region, tc.region, nil)
			require.NoError(t, err)

			_, err = snsBk.Subscribe(topic.TopicArn, "lambda", fnARN, "")
			require.NoError(t, err)

			_, err = snsBk.Publish(topic.TopicArn, "hello", "", "", nil)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				return rec.has(tc.region + "/" + fn)
			}, 5*time.Second, 20*time.Millisecond)
			assert.False(t, rec.has(tc.other+"/"+fn))
		})
	}
}

func TestInitializeServices_SQSEventSourceMappingInvokesLambdaInQueueRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)
	lambdaH, rec := regionalLambda(t)
	wireKinesisLambda(byName["Kinesis"], lambdaH)
	wireSQSLambda(byName["SQS"], lambdaH)
	require.NoError(t, lambdaH.StartWorker(t.Context()))

	sqsH, ok := byName["SQS"].(*sqsbackend.Handler)
	require.True(t, ok)

	sqsBk, ok := sqsH.Backend.(*sqsbackend.InMemoryBackend)
	require.True(t, ok)

	tests := []struct {
		name   string
		region string
		other  string
	}{
		{name: "eu-west-1", region: euRegion, other: crossHome},
		{name: "us-west-2", region: usWest2, other: euRegion},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fn := "esm-" + tc.region
			createImageFunction(t, lambdaH, tc.region, fn)
			createImageFunction(t, lambdaH, tc.other, fn)

			q, err := sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "q-" + tc.region, Region: tc.region})
			require.NoError(t, err)

			_, err = regionalBackend(t, lambdaH, tc.region).CreateEventSourceMapping(
				&lambdabackend.CreateEventSourceMappingInput{
					FunctionName:   fn,
					EventSourceARN: arn.Build("sqs", tc.region, crossAcct, "q-"+tc.region),
					Enabled:        true,
					BatchSize:      1,
				})
			require.NoError(t, err)

			_, err = sqsBk.SendMessage(&sqsbackend.SendMessageInput{
				QueueURL: q.QueueURL, Region: tc.region, MessageBody: "hi",
			})
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				return rec.has(tc.region + "/" + fn)
			}, 10*time.Second, 50*time.Millisecond)
			assert.False(t, rec.has(tc.other+"/"+fn))
		})
	}
}

func TestInitializeServices_InProcessInvokersResolveFunctionByARNRegion(t *testing.T) {
	t.Parallel()

	lambdaH, rec := regionalLambda(t)

	home := regionalBackend(t, lambdaH, crossHome)
	dt := &ebbackend.DeliveryTargets{}
	wireEventBridgeCoreTargets(dt, lambdaH, nil, nil)

	invokers := map[string]func(ctx context.Context, fnARN string) error{
		"eventbridge": func(ctx context.Context, fnARN string) error {
			_, _, err := dt.Lambda.InvokeFunction(ctx, fnARN, "Event", nil)

			return err
		},
		"cognito": func(ctx context.Context, fnARN string) error {
			_, err := (&cognitoLambdaTriggerAdapter{backend: home}).InvokeTrigger(ctx, fnARN, map[string]any{})

			return err
		},
		"scheduler": func(ctx context.Context, fnARN string) error {
			_, _, err := (&schedulerLambdaAdapter{backend: home}).InvokeFunction(ctx, fnARN, "Event", nil)

			return err
		},
		"cloudwatch": func(ctx context.Context, fnARN string) error {
			_, _, err := (&cwLambdaInvokerAdapter{backend: home}).InvokeFunction(ctx, fnARN, "Event", nil)

			return err
		},
		"async-destination": func(ctx context.Context, fnARN string) error {
			return (&lambdaAsyncDeliveryAdapter{lambda: home}).DeliverToTarget(ctx, fnARN, []byte("{}"), nil)
		},
		"logs-subscription": func(ctx context.Context, fnARN string) error {
			return (&cwlogsSubscriptionDeliverer{lambda: home}).DeliverLogEvents(ctx, fnARN, []byte("{}"))
		},
		"iot-rule": func(ctx context.Context, fnARN string) error {
			return (&iotRuleDispatcher{lambda: home}).InvokeLambda(ctx, fnARN, nil)
		},
	}

	for name, invoke := range invokers {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			fn := "inproc-" + name
			euARN := createImageFunction(t, lambdaH, euRegion, fn)
			createImageFunction(t, lambdaH, crossHome, fn)

			ctx := awsmeta.Set(context.Background(), &awsmeta.Metadata{Region: crossHome, Account: crossAcct})
			require.ErrorIs(t, invoke(ctx, euARN), lambdabackend.ErrLambdaUnavailable)

			assert.True(t, rec.has(euRegion+"/"+fn))
			assert.False(t, rec.has(crossHome+"/"+fn))
		})
	}
}

func TestInitializeServices_LambdaPolicyAdapterReadsARNRegion(t *testing.T) {
	t.Parallel()

	lambdaH, _ := regionalLambda(t)
	euARN := createImageFunction(t, lambdaH, euRegion, "policy-fn")
	createImageFunction(t, lambdaH, crossHome, "policy-fn")

	_, err := regionalBackend(t, lambdaH, euRegion).AddPermission("policy-fn", "", &lambdabackend.AddPermissionInput{
		StatementID: "eu-only", Action: "lambda:InvokeFunction", Principal: "s3.amazonaws.com",
	})
	require.NoError(t, err)

	adapter := &lambdaPolicyAdapter{handler: lambdaH}

	policy, err := adapter.GetResourcePolicy(t.Context(), euARN)
	require.NoError(t, err)
	assert.Contains(t, policy, "eu-only")

	homeARN := arn.Build("lambda", crossHome, crossAcct, "function:policy-fn")
	policy, err = adapter.GetResourcePolicy(t.Context(), homeARN)
	assert.NotContains(t, policy, "eu-only")
	_ = err
}

const cfnLambdaTemplate = `{"Resources":{
"F":{"Type":"AWS::Lambda::Function","Properties":{"FunctionName":"cfn-fn","Runtime":"nodejs20.x",
"Handler":"index.handler","Role":"arn:aws:iam::000000000000:role/r"}}}}`

func TestInitializeServices_CloudFormationCreatesLambdaInStackRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	cfnH, ok := byName["CloudFormation"].(*cfnbackend.Handler)
	require.True(t, ok)

	lambdaH, ok := byName["Lambda"].(*lambdabackend.Handler)
	require.True(t, ok)

	regionFormCall(t, cfnH.Handler(), euRegion, url.Values{
		"Action": {"CreateStack"}, "Version": {"2010-05-15"}, "StackName": {"eu-lambda"},
		"TemplateBody": {cfnLambdaTemplate},
	})

	tests := []struct {
		name   string
		region string
		want   bool
	}{
		{name: "eu-west-1", region: euRegion, want: true},
		{name: "us-east-1", region: crossHome},
		{name: "us-west-2", region: usWest2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := lambdaH.BackendFor(tc.region).GetFunction("cfn-fn")
			assert.Equal(t, tc.want, err == nil)
		})
	}
}

func TestInitializeServices_AutoScalingRegistersClassicELBInGroupRegion(t *testing.T) {
	t.Parallel()

	byName := crossRegionServices(t)

	asgH, ok := byName["Autoscaling"].(*asgbackend.Handler)
	require.True(t, ok)

	elbH, ok := byName["ELB"].(*elbbackend.Handler)
	require.True(t, ok)

	elbBk, ok := elbH.Backend.(*elbbackend.InMemoryBackend)
	require.True(t, ok)

	tests := []struct {
		name   string
		region string
		other  string
	}{
		{name: "eu-west-1", region: euRegion, other: crossHome},
		{name: "us-west-2", region: usWest2, other: euRegion},
	}

	instances := func(region, lb string) []elbbackend.Instance {
		ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: region, Account: crossAcct})

		lbs, err := elbBk.DescribeLoadBalancers(ctx, []string{lb})
		require.NoError(t, err)
		require.Len(t, lbs, 1)

		return lbs[0].Instances
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			lb, groupName := "web-"+tc.region, "asg-"+tc.region

			for _, r := range []string{tc.region, tc.other} {
				ctx := awsmeta.Set(t.Context(), &awsmeta.Metadata{Region: r, Account: crossAcct})
				_, err := elbBk.CreateLoadBalancer(ctx, elbbackend.CreateLoadBalancerInput{
					LoadBalancerName:  lb,
					AvailabilityZones: []string{r + "a"},
					Listeners: []elbbackend.Listener{
						{Protocol: "HTTP", InstanceProtocol: "HTTP", LoadBalancerPort: 80, InstancePort: 8080},
					},
				})
				require.NoError(t, err)
			}

			asgBk, isASG := asgH.BackendFor(tc.region).(*asgbackend.InMemoryBackend)
			require.True(t, isASG)

			_, err := asgBk.CreateLaunchConfiguration(asgbackend.CreateLaunchConfigurationInput{
				LaunchConfigurationName: "lc-" + tc.region, ImageID: "ami-0123456789abcdef0", InstanceType: "t3.micro",
			})
			require.NoError(t, err)

			group, err := asgBk.CreateAutoScalingGroup(asgbackend.CreateAutoScalingGroupInput{
				AutoScalingGroupName:    groupName,
				LaunchConfigurationName: "lc-" + tc.region,
				MaxSize:                 2,
				DesiredCapacity:         1,
				LoadBalancerNames:       []string{lb},
			})
			require.NoError(t, err)
			require.Len(t, group.Instances, 1)

			registered := instances(tc.region, lb)
			require.Len(t, registered, 1)
			assert.Equal(t, group.Instances[0].InstanceID, registered[0].InstanceID)
			assert.Empty(t, instances(tc.other, lb))
		})
	}
}
