package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	"github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// TestListServiceDeployments_StatusAndCreatedAtFilters is a regression test
// for gopherstack-uox6: ListServiceDeploymentsInput.CreatedAt and .Status
// (ecs@v1.96.0 api_op_ListServiceDeployments.go) were never declared on the
// backend's wire input struct at all, so both filters were silently ignored
// -- every deployment was returned regardless of what the caller asked for.
func TestListServiceDeployments_StatusAndCreatedAtFilters(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
	h := ecs.NewHandler(backend)
	client := newTestECSClient(t, h)

	const cluster = "sdfilter-cluster"
	const service = "sdfilter-svc"

	old := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	recent := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	depArnPrefix := "arn:aws:ecs:us-east-1:000000000000:service-deployment/" + cluster + "/" + service + "/"
	clusterArn := "arn:aws:ecs:us-east-1:000000000000:cluster/" + cluster
	serviceArn := "arn:aws:ecs:us-east-1:000000000000:service/" + cluster + "/" + service

	backend.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
		ServiceDeploymentArn: depArnPrefix + "dep-old",
		ClusterArn:           clusterArn,
		ServiceArn:           serviceArn,
		Status:               "SUCCESSFUL",
		CreatedAt:            &old,
	})
	backend.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
		ServiceDeploymentArn: depArnPrefix + "dep-recent",
		ClusterArn:           clusterArn,
		ServiceArn:           serviceArn,
		Status:               "IN_PROGRESS",
		CreatedAt:            &recent,
	})

	t.Run("no_filter_returns_both", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListServiceDeployments(t.Context(), &ecssdk.ListServiceDeploymentsInput{
			Cluster: aws.String(cluster),
			Service: aws.String(service),
		})
		require.NoError(t, err)
		assert.Len(t, out.ServiceDeployments, 2)
	})

	t.Run("status_filter_narrows_to_matching", func(t *testing.T) {
		t.Parallel()

		out, err := client.ListServiceDeployments(t.Context(), &ecssdk.ListServiceDeploymentsInput{
			Cluster: aws.String(cluster),
			Service: aws.String(service),
			Status:  []types.ServiceDeploymentStatus{types.ServiceDeploymentStatusInProgress},
		})
		require.NoError(t, err)
		require.Len(t, out.ServiceDeployments, 1)
		assert.Contains(t, aws.ToString(out.ServiceDeployments[0].ServiceDeploymentArn), "dep-recent")
	})

	t.Run("created_at_after_excludes_older", func(t *testing.T) {
		t.Parallel()

		cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		out, err := client.ListServiceDeployments(t.Context(), &ecssdk.ListServiceDeploymentsInput{
			Cluster:   aws.String(cluster),
			Service:   aws.String(service),
			CreatedAt: &types.CreatedAt{After: aws.Time(cutoff)},
		})
		require.NoError(t, err)
		require.Len(t, out.ServiceDeployments, 1)
		assert.Contains(t, aws.ToString(out.ServiceDeployments[0].ServiceDeploymentArn), "dep-recent")
	})

	t.Run("created_at_before_excludes_newer", func(t *testing.T) {
		t.Parallel()

		cutoff := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
		out, err := client.ListServiceDeployments(t.Context(), &ecssdk.ListServiceDeploymentsInput{
			Cluster:   aws.String(cluster),
			Service:   aws.String(service),
			CreatedAt: &types.CreatedAt{Before: aws.Time(cutoff)},
		})
		require.NoError(t, err)
		require.Len(t, out.ServiceDeployments, 1)
		assert.Contains(t, aws.ToString(out.ServiceDeployments[0].ServiceDeploymentArn), "dep-old")
	})
}

// TestListDaemonDeployments_CreatedAtFilter is a regression test for
// gopherstack-uox6: ListDaemonDeploymentsInput.CreatedAt (ecs@v1.96.0
// api_op_ListDaemonDeployments.go) was never declared on the backend's wire
// input struct (Status alone was already read), so it was silently ignored.
// CreateDaemon's own deployment is stamped with real time.Now(), so the
// filter is exercised with cutoffs relative to now rather than a fixed date.
func TestListDaemonDeployments_CreatedAtFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)

	tdArn := registerDaemonTaskDef(t, h, "filter-family")
	cpArn := createCapacityProviderForDaemon(t, h, "filter-cp")

	rec := doECSRequest(t, h, "CreateDaemon", map[string]any{
		"daemonName":              "filter-target",
		"daemonTaskDefinitionArn": tdArn,
		"capacityProviderArns":    []string{cpArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	daemonArn, _ := created["daemonArn"].(string)
	require.NotEmpty(t, daemonArn)

	future := time.Now().Add(time.Hour)
	out, err := client.ListDaemonDeployments(t.Context(), &ecssdk.ListDaemonDeploymentsInput{
		DaemonArn: aws.String(daemonArn),
		CreatedAt: &types.CreatedAt{After: aws.Time(future)},
	})
	require.NoError(t, err)
	assert.Empty(t, out.DaemonDeployments, "After in the future must exclude the just-created deployment")

	past := time.Now().Add(-time.Hour)
	out, err = client.ListDaemonDeployments(t.Context(), &ecssdk.ListDaemonDeploymentsInput{
		DaemonArn: aws.String(daemonArn),
		CreatedAt: &types.CreatedAt{After: aws.Time(past)},
	})
	require.NoError(t, err)
	assert.Len(t, out.DaemonDeployments, 1, "After in the past must include the just-created deployment")
}
