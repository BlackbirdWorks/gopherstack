package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_UndeclaredMembersAbsent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		op     string
		body   map[string]any
		path   func(m map[string]any) map[string]any
		absent []string
	}{
		{
			name:   "cluster",
			op:     "CreateCluster",
			body:   map[string]any{"clusterName": "c1"},
			path:   func(m map[string]any) map[string]any { return m["cluster"].(map[string]any) },
			absent: []string{"createdAt"},
		},
		{
			name: "capacity_provider",
			op:   "CreateCapacityProvider",
			body: map[string]any{
				"name": "cp1",
				"autoScalingGroupProvider": map[string]any{
					"autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:x:autoScalingGroupName/g",
					"managedScaling":      map[string]any{"status": "ENABLED", "targetCapacityUtilization": 80},
				},
			},
			path:   func(m map[string]any) map[string]any { return m["capacityProvider"].(map[string]any) },
			absent: []string{"createdAt"},
		},
		{
			name: "task_definition",
			op:   "RegisterTaskDefinition",
			body: map[string]any{
				"family":               "td1",
				"platformFamily":       "Linux",
				"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
			},
			path:   func(m map[string]any) map[string]any { return m["taskDefinition"].(map[string]any) },
			absent: []string{"platformFamily"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := doECSRequest(t, newTestHandler(t), tc.op, tc.body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			obj := tc.path(out)
			for _, a := range tc.absent {
				assert.NotContains(t, obj, a)
			}
		})
	}
}

func TestSDK_ServiceConnectOnPrimaryDeployment(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		namespace string
	}{
		{name: "frontend", namespace: "frontend"},
		{name: "backend", namespace: "backend"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestECSClient(t, newTestHandler(t))
			ctx := t.Context()

			_, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{ClusterName: aws.String("sc")})
			require.NoError(t, err)

			_, err = client.RegisterTaskDefinition(ctx, &ecssdk.RegisterTaskDefinitionInput{
				Family: aws.String("sc-td"),
				ContainerDefinitions: []types.ContainerDefinition{
					{Name: aws.String("app"), Image: aws.String("nginx")},
				},
			})
			require.NoError(t, err)

			out, err := client.CreateService(ctx, &ecssdk.CreateServiceInput{
				Cluster:        aws.String("sc"),
				ServiceName:    aws.String("svc"),
				TaskDefinition: aws.String("sc-td"),
				DesiredCount:   aws.Int32(1),
				ServiceConnectConfiguration: &types.ServiceConnectConfiguration{
					Enabled:   true,
					Namespace: aws.String(tc.namespace),
				},
			})
			require.NoError(t, err)

			var primary *types.Deployment

			for i := range out.Service.Deployments {
				if aws.ToString(out.Service.Deployments[i].Status) == "PRIMARY" {
					primary = &out.Service.Deployments[i]
				}
			}

			require.NotNil(t, primary)
			require.NotNil(t, primary.ServiceConnectConfiguration)
			assert.True(t, primary.ServiceConnectConfiguration.Enabled)
			assert.Equal(t, tc.namespace, aws.ToString(primary.ServiceConnectConfiguration.Namespace))
		})
	}
}
