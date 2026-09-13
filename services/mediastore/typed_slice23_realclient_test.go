package mediastore_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	mediastoresdk "github.com/aws/aws-sdk-go-v2/service/mediastore"
	"github.com/aws/aws-sdk-go-v2/service/mediastore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// slice23ContainerPolicyDoc builds a minimal valid container access policy
// document granting public GetObject on containerName.
func slice23ContainerPolicyDoc(containerName string) string {
	const tmpl = `{"Version":"2012-10-17","Statement":[{"Sid":"s1","Effect":"Allow",` +
		`"Principal":"*","Action":"mediastore:GetObject",` +
		`"Resource":"arn:aws:mediastore:us-east-1:000000000000:container/%s/*"}]}`

	return fmt.Sprintf(tmpl, containerName)
}

// TestTypedSlice23RealClient drives mediastore's remaining typed-coverage-
// blind ops (gopherstack-n3zi slice 23) through the real aws-sdk-go-v2
// client: PutContainerPolicy, GetContainerPolicy, DeleteContainerPolicy,
// PutCorsPolicy, GetCorsPolicy, DeleteCorsPolicy, PutLifecyclePolicy,
// GetLifecyclePolicy, DeleteLifecyclePolicy, PutMetricPolicy,
// GetMetricPolicy, DeleteMetricPolicy, StartAccessLogging,
// StopAccessLogging.
func TestTypedSlice23RealClient(t *testing.T) {
	t.Parallel()

	t.Run("container policy lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestMediaStoreClient(t, newTestHandler(t))
		ctx := t.Context()
		name := "s23-container-policy"

		_, err := client.CreateContainer(ctx, &mediastoresdk.CreateContainerInput{ContainerName: aws.String(name)})
		require.NoError(t, err)

		policyDoc := slice23ContainerPolicyDoc(name)

		_, err = client.PutContainerPolicy(ctx, &mediastoresdk.PutContainerPolicyInput{
			ContainerName: aws.String(name), Policy: aws.String(policyDoc),
		})
		require.NoError(t, err)

		getOut, err := client.GetContainerPolicy(
			ctx,
			&mediastoresdk.GetContainerPolicyInput{ContainerName: aws.String(name)},
		)
		require.NoError(t, err)
		assert.Equal(t, policyDoc, aws.ToString(getOut.Policy))

		_, err = client.DeleteContainerPolicy(
			ctx,
			&mediastoresdk.DeleteContainerPolicyInput{ContainerName: aws.String(name)},
		)
		require.NoError(t, err)

		_, err = client.GetContainerPolicy(ctx, &mediastoresdk.GetContainerPolicyInput{ContainerName: aws.String(name)})
		require.Error(t, err, "policy no longer exists after delete")
	})

	t.Run("cors policy lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestMediaStoreClient(t, newTestHandler(t))
		ctx := t.Context()
		name := "s23-cors-policy"

		_, err := client.CreateContainer(ctx, &mediastoresdk.CreateContainerInput{ContainerName: aws.String(name)})
		require.NoError(t, err)

		rules := []types.CorsRule{{
			AllowedOrigins: []string{"https://example.com"},
			AllowedHeaders: []string{"*"},
			AllowedMethods: []types.MethodName{types.MethodNameGet},
			MaxAgeSeconds:  3000,
		}}

		_, err = client.PutCorsPolicy(ctx, &mediastoresdk.PutCorsPolicyInput{
			ContainerName: aws.String(name), CorsPolicy: rules,
		})
		require.NoError(t, err)

		getOut, err := client.GetCorsPolicy(ctx, &mediastoresdk.GetCorsPolicyInput{ContainerName: aws.String(name)})
		require.NoError(t, err)
		require.Len(t, getOut.CorsPolicy, 1)
		assert.Equal(t, []string{"https://example.com"}, getOut.CorsPolicy[0].AllowedOrigins)
		assert.Equal(t, []types.MethodName{types.MethodNameGet}, getOut.CorsPolicy[0].AllowedMethods)
		assert.Equal(t, int32(3000), getOut.CorsPolicy[0].MaxAgeSeconds)

		_, err = client.DeleteCorsPolicy(ctx, &mediastoresdk.DeleteCorsPolicyInput{ContainerName: aws.String(name)})
		require.NoError(t, err)

		_, err = client.GetCorsPolicy(ctx, &mediastoresdk.GetCorsPolicyInput{ContainerName: aws.String(name)})
		require.Error(t, err, "policy no longer exists after delete")
	})

	t.Run("lifecycle policy lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestMediaStoreClient(t, newTestHandler(t))
		ctx := t.Context()
		name := "s23-lifecycle-policy"

		_, err := client.CreateContainer(ctx, &mediastoresdk.CreateContainerInput{ContainerName: aws.String(name)})
		require.NoError(t, err)

		policyDoc := `{"rules":[{"definition":{"path":[{"prefix":"logs/"}]},"days":1,"action":"EXPIRE"}]}`

		_, err = client.PutLifecyclePolicy(ctx, &mediastoresdk.PutLifecyclePolicyInput{
			ContainerName: aws.String(name), LifecyclePolicy: aws.String(policyDoc),
		})
		require.NoError(t, err)

		getOut, err := client.GetLifecyclePolicy(
			ctx,
			&mediastoresdk.GetLifecyclePolicyInput{ContainerName: aws.String(name)},
		)
		require.NoError(t, err)
		assert.Equal(t, policyDoc, aws.ToString(getOut.LifecyclePolicy))

		_, err = client.DeleteLifecyclePolicy(
			ctx,
			&mediastoresdk.DeleteLifecyclePolicyInput{ContainerName: aws.String(name)},
		)
		require.NoError(t, err)

		_, err = client.GetLifecyclePolicy(ctx, &mediastoresdk.GetLifecyclePolicyInput{ContainerName: aws.String(name)})
		require.Error(t, err, "policy no longer exists after delete")
	})

	t.Run("metric policy lifecycle", func(t *testing.T) {
		t.Parallel()

		client := newTestMediaStoreClient(t, newTestHandler(t))
		ctx := t.Context()
		name := "s23-metric-policy"

		_, err := client.CreateContainer(ctx, &mediastoresdk.CreateContainerInput{ContainerName: aws.String(name)})
		require.NoError(t, err)

		policy := &types.MetricPolicy{
			ContainerLevelMetrics: types.ContainerLevelMetricsEnabled,
			MetricPolicyRules: []types.MetricPolicyRule{{
				ObjectGroup:     aws.String("/logs/*"),
				ObjectGroupName: aws.String("LogsGroup"),
			}},
		}

		_, err = client.PutMetricPolicy(ctx, &mediastoresdk.PutMetricPolicyInput{
			ContainerName: aws.String(name), MetricPolicy: policy,
		})
		require.NoError(t, err)

		getOut, err := client.GetMetricPolicy(ctx, &mediastoresdk.GetMetricPolicyInput{ContainerName: aws.String(name)})
		require.NoError(t, err)
		require.NotNil(t, getOut.MetricPolicy)
		assert.Equal(t, types.ContainerLevelMetricsEnabled, getOut.MetricPolicy.ContainerLevelMetrics)
		require.Len(t, getOut.MetricPolicy.MetricPolicyRules, 1)
		assert.Equal(t, "/logs/*", aws.ToString(getOut.MetricPolicy.MetricPolicyRules[0].ObjectGroup))
		assert.Equal(t, "LogsGroup", aws.ToString(getOut.MetricPolicy.MetricPolicyRules[0].ObjectGroupName))

		_, err = client.DeleteMetricPolicy(ctx, &mediastoresdk.DeleteMetricPolicyInput{ContainerName: aws.String(name)})
		require.NoError(t, err)

		_, err = client.GetMetricPolicy(ctx, &mediastoresdk.GetMetricPolicyInput{ContainerName: aws.String(name)})
		require.Error(t, err, "policy no longer exists after delete")
	})

	t.Run("access logging toggle", func(t *testing.T) {
		t.Parallel()

		client := newTestMediaStoreClient(t, newTestHandler(t))
		ctx := t.Context()
		name := "s23-access-logging"

		_, err := client.CreateContainer(ctx, &mediastoresdk.CreateContainerInput{ContainerName: aws.String(name)})
		require.NoError(t, err)

		descBefore, err := client.DescribeContainer(
			ctx,
			&mediastoresdk.DescribeContainerInput{ContainerName: aws.String(name)},
		)
		require.NoError(t, err)
		assert.False(t, aws.ToBool(descBefore.Container.AccessLoggingEnabled))

		_, err = client.StartAccessLogging(ctx, &mediastoresdk.StartAccessLoggingInput{ContainerName: aws.String(name)})
		require.NoError(t, err)

		descAfterStart, err := client.DescribeContainer(
			ctx,
			&mediastoresdk.DescribeContainerInput{ContainerName: aws.String(name)},
		)
		require.NoError(t, err)
		assert.True(t, aws.ToBool(descAfterStart.Container.AccessLoggingEnabled))

		_, err = client.StopAccessLogging(ctx, &mediastoresdk.StopAccessLoggingInput{ContainerName: aws.String(name)})
		require.NoError(t, err)

		descAfterStop, err := client.DescribeContainer(
			ctx,
			&mediastoresdk.DescribeContainerInput{ContainerName: aws.String(name)},
		)
		require.NoError(t, err)
		assert.False(t, aws.ToBool(descAfterStop.Container.AccessLoggingEnabled))
	})
}
