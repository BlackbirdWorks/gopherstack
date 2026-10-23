package integration_test

import (
	"context"
	"io"
	"net"
	"runtime"
	"strconv"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	redis "cloud.google.com/go/redis/apiv1"
	"cloud.google.com/go/redis/apiv1/redispb"
	"github.com/googleapis/gax-go/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const redisLocation = "projects/" + gcpProject + "/locations/us-central1"

func newRedisClient(t *testing.T) *redis.CloudRedisClient {
	t.Helper()

	client, err := redis.NewCloudRedisClient(t.Context(), gcpGRPCOpts(t, "8133")...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return client
}

type redisPoller[T any] interface {
	Poll(ctx context.Context, opts ...gax.CallOption) (T, error)
	Done() bool
}

func awaitRedisOp[T any](t *testing.T, op redisPoller[T]) T {
	t.Helper()

	var (
		out T
		err error
	)

	require.Eventually(t, func() bool {
		out, err = op.Poll(t.Context())

		return op.Done()
	}, 30*time.Second, 20*time.Millisecond)
	require.NoError(t, err)

	return out
}

// dialRedisInstance connects to an instance's endpoint, which listens inside the container; it returns nil where the
// container network is not routable from the test process.
func dialRedisInstance(t *testing.T, inst *redispb.Instance, auth string) *goredis.Client {
	t.Helper()

	if sharedContainer == nil || runtime.GOOS != "linux" {
		return nil
	}

	ip, err := sharedContainer.ContainerIP(t.Context())
	require.NoError(t, err)

	c := goredis.NewClient(&goredis.Options{
		Addr: net.JoinHostPort(ip, strconv.Itoa(int(inst.GetPort()))), Password: auth,
	})
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func TestIntegration_GCP_Redis_Lifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newRedisClient(t)
	ctx := t.Context()
	id := gcpName("it-redis")

	op, err := client.CreateInstance(ctx, &redispb.CreateInstanceRequest{
		Parent: redisLocation, InstanceId: id,
		Instance: &redispb.Instance{
			Tier: redispb.Instance_STANDARD_HA, MemorySizeGb: 1, RedisVersion: "REDIS_6_X", AuthEnabled: true,
			Labels: map[string]string{"env": "it"},
		},
	})
	require.NoError(t, err)

	inst := awaitRedisOp(t, op)
	assert.Equal(t, redisLocation+"/instances/"+id, inst.GetName())
	assert.Equal(t, redispb.Instance_READY, inst.GetState())
	assert.NotEmpty(t, inst.GetHost())
	assert.Positive(t, inst.GetPort())
	assert.Len(t, inst.GetNodes(), 2)

	as, err := client.GetInstanceAuthString(ctx, &redispb.GetInstanceAuthStringRequest{Name: inst.GetName()})
	require.NoError(t, err)

	if rc := dialRedisInstance(t, inst, as.GetAuthString()); rc != nil {
		require.NoError(t, rc.Set(ctx, "it", "value", 0).Err())
		assert.Equal(t, "value", rc.Get(ctx, "it").Val())
	}

	uop, err := client.UpdateInstance(ctx, &redispb.UpdateInstanceRequest{
		Instance:   &redispb.Instance{Name: inst.GetName(), Labels: map[string]string{"env": "it2"}, MemorySizeGb: 2},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels", "memory_size_gb"}},
	})
	require.NoError(t, err)

	updated := awaitRedisOp(t, uop)
	assert.Equal(t, "it2", updated.GetLabels()["env"])
	assert.Equal(t, int32(2), updated.GetMemorySizeGb())

	gop, err := client.UpgradeInstance(ctx, &redispb.UpgradeInstanceRequest{
		Name: inst.GetName(), RedisVersion: "REDIS_7_2",
	})
	require.NoError(t, err)
	assert.Equal(t, "REDIS_7_2", awaitRedisOp(t, gop).GetRedisVersion())

	fop, err := client.FailoverInstance(ctx, &redispb.FailoverInstanceRequest{Name: inst.GetName()})
	require.NoError(t, err)
	assert.Equal(t, inst.GetAlternativeLocationId(), awaitRedisOp(t, fop).GetCurrentLocationId())

	found := false
	it := client.ListInstances(ctx, &redispb.ListInstancesRequest{Parent: redisLocation})

	for {
		got, nerr := it.Next()
		if nerr != nil {
			break
		}

		found = found || got.GetName() == inst.GetName()
	}

	assert.True(t, found)

	del, err := client.DeleteInstance(ctx, &redispb.DeleteInstanceRequest{Name: inst.GetName()})
	require.NoError(t, err)
	require.Eventually(
		t,
		func() bool { return del.Poll(ctx) == nil && del.Done() },
		30*time.Second,
		20*time.Millisecond,
	)

	_, err = client.GetInstance(ctx, &redispb.GetInstanceRequest{Name: inst.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestIntegration_GCP_Redis_ImportExport(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newRedisClient(t)
	gcs := newGCSClient(t)
	ctx := t.Context()
	bucketName := gcpName("it-redis-bkt")

	require.NoError(t, gcs.Bucket(bucketName).Create(ctx, gcpProject, nil))

	create := func(suffix string) *redispb.Instance {
		op, err := client.CreateInstance(ctx, &redispb.CreateInstanceRequest{
			Parent: redisLocation, InstanceId: gcpName("it-redis-" + suffix),
			Instance: &redispb.Instance{Tier: redispb.Instance_BASIC, MemorySizeGb: 1},
		})
		require.NoError(t, err)

		inst := awaitRedisOp(t, op)
		t.Cleanup(func() {
			_, _ = client.DeleteInstance(context.Background(), &redispb.DeleteInstanceRequest{Name: inst.GetName()})
		})

		return inst
	}

	src, dst := create("src"), create("dst")
	srcConn, dstConn := dialRedisInstance(t, src, ""), dialRedisInstance(t, dst, "")

	if srcConn != nil {
		require.NoError(t, srcConn.Set(ctx, "exported", "yes", 0).Err())
	}

	exp, err := client.ExportInstance(ctx, &redispb.ExportInstanceRequest{
		Name: src.GetName(),
		OutputConfig: &redispb.OutputConfig{Destination: &redispb.OutputConfig_GcsDestination{
			GcsDestination: &redispb.GcsDestination{Uri: "gs://" + bucketName + "/dump.rdb"},
		}},
	})
	require.NoError(t, err)
	awaitRedisOp(t, exp)

	r, err := gcs.Bucket(bucketName).Object("dump.rdb").NewReader(ctx)
	require.NoError(t, err)

	body, err := io.ReadAll(r)
	require.NoError(t, err)
	require.NoError(t, r.Close())
	assert.Equal(t, "REDIS0009", string(body[:9]))

	imp, err := client.ImportInstance(ctx, &redispb.ImportInstanceRequest{
		Name: dst.GetName(),
		InputConfig: &redispb.InputConfig{Source: &redispb.InputConfig_GcsSource{
			GcsSource: &redispb.GcsSource{Uri: "gs://" + bucketName + "/dump.rdb"},
		}},
	})
	require.NoError(t, err)
	assert.Equal(t, redispb.Instance_READY, awaitRedisOp(t, imp).GetState())

	if dstConn != nil {
		assert.Equal(t, "yes", dstConn.Get(ctx, "exported").Val())
	}

	missing, err := client.ImportInstance(ctx, &redispb.ImportInstanceRequest{
		Name: dst.GetName(),
		InputConfig: &redispb.InputConfig{Source: &redispb.InputConfig_GcsSource{
			GcsSource: &redispb.GcsSource{Uri: "gs://" + bucketName + "/absent.rdb"},
		}},
	})
	require.NoError(t, err)

	require.Eventually(
		t,
		func() bool {
			_, pErr := missing.Poll(ctx)

			return missing.Done() && pErr != nil
		},
		30*time.Second,
		20*time.Millisecond,
	)
}

func TestIntegration_GCP_Redis_Operations(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := newRedisClient(t)
	ctx := t.Context()

	_, err := client.GetInstance(ctx, &redispb.GetInstanceRequest{Name: redisLocation + "/instances/never-created"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = client.LROClient.GetOperation(
		ctx,
		&longrunningpb.GetOperationRequest{Name: redisLocation + "/operations/none"},
	)
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = client.CreateInstance(ctx, &redispb.CreateInstanceRequest{
		Parent:     redisLocation,
		InstanceId: "Bad_Id",
		Instance:   &redispb.Instance{Tier: redispb.Instance_BASIC, MemorySizeGb: 1},
	})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
