package app

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"net/http/httptest"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
	cloudtrailbackend "github.com/blackbirdworks/gopherstack/services/cloudtrail"
)

const (
	soakRestartCycles = 5
	// soakRestartHeapBudget bounds retained heap after the warm-up cycle;
	// a service leaking its backend per restart retains tens of MB per cycle.
	soakRestartHeapBudget = 24 << 20
	soakWarmupIters       = 200
	soakMeasureIters      = 600
	soakHeapBudget        = 16 << 20
	soakObjBudget         = 150_000
)

// listeningSockets returns "proto:port" for every TCP listener and bound UDP
// socket owned by this process, read from /proc.
func listeningSockets(t *testing.T) map[string]bool {
	t.Helper()

	owned := map[string]bool{}

	fds, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)

	for _, fd := range fds {
		target, linkErr := os.Readlink(filepath.Join("/proc/self/fd", fd.Name()))
		if linkErr == nil && strings.HasPrefix(target, "socket:[") {
			owned[strings.TrimSuffix(strings.TrimPrefix(target, "socket:["), "]")] = true
		}
	}

	out := map[string]bool{}

	for _, tbl := range []struct{ file, state string }{
		{"tcp", "0A"}, {"tcp6", "0A"}, {"udp", "07"}, {"udp6", "07"},
	} {
		f, openErr := os.Open("/proc/net/" + tbl.file)
		require.NoError(t, openErr)

		sc := bufio.NewScanner(f)
		sc.Scan()

		for sc.Scan() {
			cols := strings.Fields(sc.Text())
			if len(cols) < 10 || cols[3] != tbl.state || !owned[cols[9]] {
				continue
			}

			_, hexPort, _ := strings.Cut(cols[1], ":")
			port, _ := strconv.ParseUint(hexPort, 16, 32)
			out[strings.TrimSuffix(tbl.file, "6")+":"+strconv.FormatUint(port, 10)] = true
		}

		require.NoError(t, f.Close())
	}

	return out
}

func soakConfig(t *testing.T, endpoint string) aws.Config {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		awscfg.WithBaseEndpoint(endpoint),
		awscfg.WithRetryMaxAttempts(1),
	)
	require.NoError(t, err)

	return cfg
}

type soakClients struct {
	sts *sts.Client
	sqs *sqs.Client
	sns *sns.Client
	kms *kms.Client
	sm  *secretsmanager.Client
	ssm *ssm.Client
	eb  *eventbridge.Client
	cw  *cloudwatch.Client
	cwl *cloudwatchlogs.Client
	iam *iam.Client
	sfn *sfn.Client
	cfn *cloudformation.Client
}

// newSoakClients builds one client per service; SDK clients own their
// transport, so building them per iteration would leak idle connections.
func newSoakClients(cfg aws.Config) *soakClients {
	return &soakClients{
		sts: sts.NewFromConfig(cfg), sqs: sqs.NewFromConfig(cfg), sns: sns.NewFromConfig(cfg),
		kms: kms.NewFromConfig(cfg), sm: secretsmanager.NewFromConfig(cfg), ssm: ssm.NewFromConfig(cfg),
		eb: eventbridge.NewFromConfig(cfg), cw: cloudwatch.NewFromConfig(cfg),
		cwl: cloudwatchlogs.NewFromConfig(cfg), iam: iam.NewFromConfig(cfg),
		sfn: sfn.NewFromConfig(cfg), cfn: cloudformation.NewFromConfig(cfg),
	}
}

// soakTraffic drives one create/use/delete round across many unrelated
// services (S3 and DynamoDB are covered by the dedicated leak suites).
// Names carry "leak" so leakOwnedSeries counts the metric series they create.
func soakTraffic(ctx context.Context, c *soakClients, i int) error {
	n := "leak" + strconv.Itoa(i)

	steps := []func() error{
		func() error {
			_, err := c.sts.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})

			return err
		},
		func() error { return soakSQS(ctx, c.sqs, n) },
		func() error { return soakSNS(ctx, c.sns, n) },
		func() error { return soakKMS(ctx, c.kms) },
		func() error { return soakSecrets(ctx, c.sm, n) },
		func() error { return soakSSM(ctx, c.ssm, n) },
		func() error { return soakEventBridge(ctx, c.eb, n) },
		func() error { return soakCloudWatch(ctx, c.cw, n) },
		func() error { return soakLogs(ctx, c.cwl, n) },
		func() error { return soakIAM(ctx, c.iam, n) },
		func() error { return soakStepFunctions(ctx, c.sfn, n) },
		func() error { return soakCloudFormation(ctx, c.cfn, n) },
	}

	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}

	return nil
}

func soakStepFunctions(ctx context.Context, c *sfn.Client, n string) error {
	sm, err := c.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{
		Name:       aws.String(n),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/leak"),
		Definition: aws.String(`{"StartAt":"P","States":{"P":{"Type":"Pass","End":true}}}`),
	})
	if err != nil {
		return err
	}

	if _, err = c.StartExecution(ctx, &sfn.StartExecutionInput{StateMachineArn: sm.StateMachineArn}); err != nil {
		return err
	}

	_, err = c.DeleteStateMachine(ctx, &sfn.DeleteStateMachineInput{StateMachineArn: sm.StateMachineArn})

	return err
}

func soakCloudFormation(ctx context.Context, c *cloudformation.Client, n string) error {
	const tpl = `{"Resources":{"Q":{"Type":"AWS::SQS::Queue"}}}`

	if _, err := c.CreateStack(ctx, &cloudformation.CreateStackInput{
		StackName: aws.String(n), TemplateBody: aws.String(tpl),
	}); err != nil {
		return err
	}

	_, err := c.DeleteStack(ctx, &cloudformation.DeleteStackInput{StackName: aws.String(n)})

	return err
}

func soakSQS(ctx context.Context, c *sqs.Client, n string) error {
	q, err := c.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(n)})
	if err != nil {
		return err
	}

	if _, err = c.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: q.QueueUrl, MessageBody: aws.String("m")}); err != nil {
		return err
	}

	rcv, err := c.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: q.QueueUrl, MaxNumberOfMessages: 1})
	if err != nil {
		return err
	}

	for _, m := range rcv.Messages {
		if _, err = c.DeleteMessage(ctx, &sqs.DeleteMessageInput{
			QueueUrl: q.QueueUrl, ReceiptHandle: m.ReceiptHandle,
		}); err != nil {
			return err
		}
	}

	_, err = c.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: q.QueueUrl})

	return err
}

func soakSNS(ctx context.Context, c *sns.Client, n string) error {
	topic, err := c.CreateTopic(ctx, &sns.CreateTopicInput{Name: aws.String(n)})
	if err != nil {
		return err
	}

	if _, err = c.Publish(ctx, &sns.PublishInput{TopicArn: topic.TopicArn, Message: aws.String("m")}); err != nil {
		return err
	}

	_, err = c.DeleteTopic(ctx, &sns.DeleteTopicInput{TopicArn: topic.TopicArn})

	return err
}

func soakKMS(ctx context.Context, c *kms.Client) error {
	key, err := c.CreateKey(ctx, &kms.CreateKeyInput{})
	if err != nil {
		return err
	}

	enc, err := c.Encrypt(ctx, &kms.EncryptInput{KeyId: key.KeyMetadata.KeyId, Plaintext: []byte("p")})
	if err != nil {
		return err
	}

	if _, err = c.Decrypt(ctx, &kms.DecryptInput{CiphertextBlob: enc.CiphertextBlob}); err != nil {
		return err
	}

	_, err = c.ScheduleKeyDeletion(ctx, &kms.ScheduleKeyDeletionInput{
		KeyId: key.KeyMetadata.KeyId, PendingWindowInDays: aws.Int32(7),
	})

	return err
}

func soakSecrets(ctx context.Context, c *secretsmanager.Client, n string) error {
	if _, err := c.CreateSecret(ctx, &secretsmanager.CreateSecretInput{
		Name: aws.String(n), SecretString: aws.String("s"),
	}); err != nil {
		return err
	}

	if _, err := c.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: aws.String(n)}); err != nil {
		return err
	}

	_, err := c.DeleteSecret(ctx, &secretsmanager.DeleteSecretInput{
		SecretId: aws.String(n), ForceDeleteWithoutRecovery: aws.Bool(true),
	})

	return err
}

func soakSSM(ctx context.Context, c *ssm.Client, n string) error {
	name := aws.String("/" + n)

	if _, err := c.PutParameter(ctx, &ssm.PutParameterInput{
		Name: name, Value: aws.String("v"), Type: ssmtypes.ParameterTypeString,
	}); err != nil {
		return err
	}

	if _, err := c.GetParameter(ctx, &ssm.GetParameterInput{Name: name}); err != nil {
		return err
	}

	_, err := c.DeleteParameter(ctx, &ssm.DeleteParameterInput{Name: name})

	return err
}

func soakEventBridge(ctx context.Context, c *eventbridge.Client, n string) error {
	if _, err := c.PutRule(ctx, &eventbridge.PutRuleInput{
		Name: aws.String(n), EventPattern: aws.String(`{"source":["leak"]}`),
	}); err != nil {
		return err
	}

	if _, err := c.PutEvents(ctx, &eventbridge.PutEventsInput{Entries: []ebtypes.PutEventsRequestEntry{{
		Source: aws.String("leak"), DetailType: aws.String("t"), Detail: aws.String("{}"),
	}}}); err != nil {
		return err
	}

	_, err := c.DeleteRule(ctx, &eventbridge.DeleteRuleInput{Name: aws.String(n)})

	return err
}

func soakCloudWatch(ctx context.Context, c *cloudwatch.Client, n string) error {
	_, err := c.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace: aws.String("Leak/" + n),
		MetricData: []cwtypes.MetricDatum{{
			MetricName: aws.String("m"), Value: aws.Float64(1), Unit: cwtypes.StandardUnitCount,
		}},
	})

	return err
}

func soakLogs(ctx context.Context, c *cloudwatchlogs.Client, n string) error {
	if _, err := c.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String(n)}); err != nil {
		return err
	}

	if _, err := c.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName: aws.String(n), LogStreamName: aws.String("s"),
	}); err != nil {
		return err
	}

	if _, err := c.PutLogEvents(ctx, &cloudwatchlogs.PutLogEventsInput{
		LogGroupName: aws.String(n), LogStreamName: aws.String("s"),
		LogEvents: []cwltypes.InputLogEvent{{Message: aws.String("e"), Timestamp: aws.Int64(time.Now().UnixMilli())}},
	}); err != nil {
		return err
	}

	_, err := c.DeleteLogGroup(ctx, &cloudwatchlogs.DeleteLogGroupInput{LogGroupName: aws.String(n)})

	return err
}

func soakIAM(ctx context.Context, c *iam.Client, n string) error {
	if _, err := c.CreateUser(ctx, &iam.CreateUserInput{UserName: aws.String(n)}); err != nil {
		return err
	}

	_, err := c.DeleteUser(ctx, &iam.DeleteUserInput{UserName: aws.String(n)})

	return err
}

// logTopGoroutines logs the largest goroutine groups, to attribute growth.
func logTopGoroutines(t *testing.T, top int) {
	t.Helper()

	var buf strings.Builder

	require.NoError(t, pprof.Lookup("goroutine").WriteTo(&buf, 1))

	groups := strings.Split(buf.String(), "\n\n")
	count := func(g string) int {
		n, _, _ := strings.Cut(strings.TrimSpace(g), " ")
		v, _ := strconv.Atoi(n)

		return v
	}

	slices.SortFunc(groups, func(a, b string) int { return count(b) - count(a) })

	for _, g := range groups[:min(top, len(groups))] {
		t.Logf("goroutine group:\n%s", g)
	}
}

func settledHeap() (uint64, int) {
	for range 3 {
		runtime.GC()
	}

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	return ms.HeapAlloc, runtime.NumGoroutine()
}

// runServerCycle boots the full composition through run(), drives light
// traffic, shuts it down and checks the listeners it opened are released.
func runServerCycle(t *testing.T) {
	t.Helper()

	before := listeningSockets(t)

	port := freeTCPPort(t)
	cli := parseCLI(t, map[string]string{"PORT": strconv.Itoa(port)})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- run(ctx, cli) }()

	waitForServerReady(t, port)

	assert.True(t, listeningSockets(t)["tcp:"+strconv.Itoa(port)], "main listener not bound")

	clients := newSoakClients(soakConfig(t, fmt.Sprintf("http://127.0.0.1:%d", port)))
	for i := range 3 {
		require.NoError(t, soakTraffic(ctx, clients, i))
	}

	cancel()

	select {
	case err := <-errCh:
		require.NoError(t, err, "server should shut down cleanly")
	case <-time.After(shutdownWaitTimeout):
		require.FailNow(t, "server did not shut down within timeout")
	}

	after := listeningSockets(t)
	for sock := range after {
		assert.True(t, before[sock], "listener %s still bound after shutdown", sock)
	}

	conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), time.Second)
	if err == nil {
		_ = conn.Close()

		assert.Fail(t, "main port still accepting connections after shutdown")
	}
}

// TestServerRestart_NoLeak starts and stops the full server (every service)
// repeatedly and asserts goroutines, heap and listeners return to baseline.
//
//nolint:paralleltest // measures process-wide goroutine and heap counts
func TestServerRestart_NoLeak(t *testing.T) {
	skipLeakInShort(t)

	baseline := goleak.IgnoreCurrent()

	runServerCycle(t)

	heap0, gor0 := settledHeap()

	for range soakRestartCycles {
		runServerCycle(t)
	}

	var (
		heap1 uint64
		gor1  int
	)

	require.Eventually(t, func() bool {
		heap1, gor1 = settledHeap()

		return gor1-gor0 <= leakMaxGoroutines
	}, leakSettleTimeout, time.Second, "goroutines %d -> %d", gor0, gor1)

	t.Logf("after %d restarts: goroutines %d->%d heap %d->%d", soakRestartCycles, gor0, gor1, heap0, heap1)
	require.LessOrEqual(t, int64(heap1)-int64(heap0), int64(soakRestartHeapBudget), "heap retained across restarts")

	goleak.VerifyNone(t, append(testleak.DefaultIgnores(), baseline)...)
}

// TestServerSoak_BoundedGrowth drives a sustained create/use/delete mix across
// many services against the full composition with every background worker
// running, and asserts bounded heap, goroutine and metric-series growth.
//
//nolint:paralleltest // measures process-wide goroutine and heap counts
func TestServerSoak_BoundedGrowth(t *testing.T) {
	skipLeakInShort(t)

	log := buildLogger("")
	cli := CLI{AccountID: "000000000000", Region: "us-east-1"}
	cli.S3.JanitorInterval = leakJanitorInterval
	cli.DynamoDB.JanitorInterval = leakJanitorInterval
	cli.portAlloc = setupPortAllocatorWithReservations(t.Context(), log, cli)
	cli.faultStore = chaos.NewFaultStore()

	services, err := initializeServicesT(t, &service.AppContext{
		Logger: log, Config: &cli, JanitorCtx: t.Context(), PortAlloc: cli.portAlloc,
	})
	require.NoError(t, err)

	e := buildEchoServer(t.Context(), log, nil, services, cli)
	require.NoError(t, setupChaosAndRegistry(e, log, &cli, services))

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	startBackgroundWorkers(t.Context(), services)

	ct, ok := serviceByName(services)["CloudTrail"].(*cloudtrailbackend.Handler)
	require.True(t, ok)

	env := &leakEnv{cfg: soakConfig(t, srv.URL), resetBounded: ct.Reset}

	clients := newSoakClients(env.cfg)

	var runErr error

	op := func(i int) {
		if err := soakTraffic(t.Context(), clients, i); err != nil && runErr == nil {
			runErr = fmt.Errorf("iteration %d: %w", i, err)
		}
	}

	for i := range soakWarmupIters {
		op(i)
	}

	require.NoError(t, runErr)

	before := takeLeakSnapshot(t, env)

	for i := range soakMeasureIters {
		op(soakWarmupIters + i)
	}

	require.NoError(t, runErr)

	after := takeLeakSnapshot(t, env)
	require.Eventually(t, func() bool {
		after = takeLeakSnapshot(t, env)

		return after.series-before.series <= 0
	}, leakSettleTimeout, time.Second, "series never returned to baseline: %d -> %d", before.series, after.series)

	growth := int64(after.heapAlloc) - int64(before.heapAlloc)
	objs := int64(after.heapObjs) - int64(before.heapObjs)
	t.Logf("goroutines %d->%d heapAlloc %+d B heapObjs %+d series %d->%d",
		before.goroutines, after.goroutines, growth, objs, before.series, after.series)

	if after.goroutines-before.goroutines > leakMaxGoroutines {
		logTopGoroutines(t, 4)
	}

	require.LessOrEqual(t, after.goroutines-before.goroutines, leakMaxGoroutines, "goroutine growth")
	require.LessOrEqual(t, growth, int64(soakHeapBudget), "live heap growth")
	require.LessOrEqual(t, objs, int64(soakObjBudget), "live heap objects growth")
}
