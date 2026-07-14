package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestPaginationCoverage(t *testing.T) {
	t.Parallel()

	t.Run("describe capacity providers page one has next token", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"cp-a", "cp-b", "cp-c"} {
			doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": name})
		}

		rec := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{"maxResults": 2})
		require.Equal(t, http.StatusOK, rec.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		cps := body["capacityProviders"].([]any)
		assert.Len(t, cps, 2)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("describe capacity providers page two consumes token", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"cp-a", "cp-b", "cp-c"} {
			doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": name})
		}
		first := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{"maxResults": 2})
		var b1 map[string]any
		require.NoError(t, json.Unmarshal(first.Body.Bytes(), &b1))
		next := b1["nextToken"].(string)

		second := doECSRequest(
			t,
			h,
			"DescribeCapacityProviders",
			map[string]any{"maxResults": 2, "nextToken": next},
		)
		require.Equal(t, http.StatusOK, second.Code)
		var b2 map[string]any
		require.NoError(t, json.Unmarshal(second.Body.Bytes(), &b2))
		cps := b2["capacityProviders"].([]any)
		assert.Len(t, cps, 1)
		assert.Empty(t, b2["nextToken"])
	})

	t.Run("describe capacity providers defaults max results", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for i := range 105 {
			doECSRequest(
				t,
				h,
				"CreateCapacityProvider",
				map[string]any{
					"name": "cp-" + time.Now().
						Add(time.Duration(i)*time.Nanosecond).
						Format("150405.000000000"),
				},
			)
		}
		rec := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["capacityProviders"].([]any), 100)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list task definition families page one has next token", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, family := range []string{"fam-a", "fam-b", "fam-c"} {
			doECSRequest(
				t,
				h,
				"RegisterTaskDefinition",
				map[string]any{
					"family":               family,
					"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
				},
			)
		}
		rec := doECSRequest(t, h, "ListTaskDefinitionFamilies", map[string]any{"maxResults": 2})
		require.Equal(t, http.StatusOK, rec.Code)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["families"].([]any), 2)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list task definition families page two", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, family := range []string{"fam-a", "fam-b", "fam-c"} {
			doECSRequest(
				t,
				h,
				"RegisterTaskDefinition",
				map[string]any{
					"family":               family,
					"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
				},
			)
		}
		first := doECSRequest(t, h, "ListTaskDefinitionFamilies", map[string]any{"maxResults": 2})
		var b1 map[string]any
		require.NoError(t, json.Unmarshal(first.Body.Bytes(), &b1))
		next := b1["nextToken"].(string)
		second := doECSRequest(
			t,
			h,
			"ListTaskDefinitionFamilies",
			map[string]any{"maxResults": 2, "nextToken": next},
		)
		var b2 map[string]any
		require.NoError(t, json.Unmarshal(second.Body.Bytes(), &b2))
		assert.Len(t, b2["families"].([]any), 1)
		assert.Empty(t, b2["nextToken"])
	})

	t.Run(
		"list task definition families honors family filter before pagination",
		func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			for _, family := range []string{"web-a", "web-b", "jobs-a"} {
				doECSRequest(
					t,
					h,
					"RegisterTaskDefinition",
					map[string]any{
						"family":               family,
						"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
					},
				)
			}
			rec := doECSRequest(
				t,
				h,
				"ListTaskDefinitionFamilies",
				map[string]any{"familyPrefix": "web", "maxResults": 1},
			)
			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Len(t, body["families"].([]any), 1)
			assert.NotEmpty(t, body["nextToken"])
		},
	)

	t.Run("list services by namespace page one", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ns-cluster"})
		doECSRequest(
			t,
			h,
			"RegisterTaskDefinition",
			map[string]any{
				"family":               "nsfam",
				"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
			},
		)
		for _, service := range []string{"frontend-api", "frontend-web", "backend-jobs"} {
			doECSRequest(
				t,
				h,
				"CreateService",
				map[string]any{
					"cluster":        "ns-cluster",
					"serviceName":    service,
					"taskDefinition": "nsfam",
				},
			)
		}
		rec := doECSRequest(
			t,
			h,
			"ListServicesByNamespace",
			map[string]any{"cluster": "ns-cluster", "namespace": "frontend", "maxResults": 1},
		)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["serviceArns"].([]any), 1)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list services by namespace page two", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ns-cluster"})
		doECSRequest(
			t,
			h,
			"RegisterTaskDefinition",
			map[string]any{
				"family":               "nsfam",
				"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
			},
		)
		for _, service := range []string{"frontend-a", "frontend-b", "frontend-c"} {
			doECSRequest(
				t,
				h,
				"CreateService",
				map[string]any{
					"cluster":        "ns-cluster",
					"serviceName":    service,
					"taskDefinition": "nsfam",
				},
			)
		}
		first := doECSRequest(
			t,
			h,
			"ListServicesByNamespace",
			map[string]any{"cluster": "ns-cluster", "namespace": "frontend", "maxResults": 2},
		)
		var b1 map[string]any
		require.NoError(t, json.Unmarshal(first.Body.Bytes(), &b1))
		second := doECSRequest(
			t,
			h,
			"ListServicesByNamespace",
			map[string]any{
				"cluster":    "ns-cluster",
				"namespace":  "frontend",
				"maxResults": 2,
				"nextToken":  b1["nextToken"].(string),
			},
		)
		var b2 map[string]any
		require.NoError(t, json.Unmarshal(second.Body.Bytes(), &b2))
		assert.Len(t, b2["serviceArns"].([]any), 1)
		assert.Empty(t, b2["nextToken"])
	})

	t.Run("list services by namespace default page size", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ns-cluster"})
		doECSRequest(
			t,
			h,
			"RegisterTaskDefinition",
			map[string]any{
				"family":               "nsfam",
				"containerDefinitions": []map[string]any{{"name": "c", "image": "nginx"}},
			},
		)
		for i := range 101 {
			doECSRequest(
				t,
				h,
				"CreateService",
				map[string]any{
					"cluster": "ns-cluster",
					"serviceName": "frontend-svc-" + time.Now().
						Add(time.Duration(i)*time.Nanosecond).
						Format("150405.000000000"),
					"taskDefinition": "nsfam",
				},
			)
		}
		rec := doECSRequest(
			t,
			h,
			"ListServicesByNamespace",
			map[string]any{"cluster": "ns-cluster", "namespace": "frontend"},
		)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["serviceArns"].([]any), 100)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list service deployments page one", func(t *testing.T) {
		t.Parallel()
		b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
		now := time.Now()
		for i := range 3 {
			arn := "arn:aws:ecs:us-east-1:000000000000:service-deployment/d-" + time.Now().
				Add(time.Duration(i)*time.Nanosecond).
				Format("150405.000000000")
			b.AddServiceDeploymentInternal(
				&ecs.ServiceDeployment{
					ServiceDeploymentArn: arn,
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/default",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/default/svc",
					Status:               "IN_PROGRESS",
					CreatedAt:            &now,
					UpdatedAt:            &now,
				},
			)
		}
		h := ecs.NewHandler(b)
		rec := doECSRequest(
			t,
			h,
			"ListServiceDeployments",
			map[string]any{"cluster": "default", "service": "svc", "maxResults": 2},
		)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["serviceDeploymentArns"].([]any), 2)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list service deployments page two", func(t *testing.T) {
		t.Parallel()
		b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
		now := time.Now()
		for i := range 3 {
			arn := "arn:aws:ecs:us-east-1:000000000000:service-deployment/dp-" + time.Now().
				Add(time.Duration(i)*time.Nanosecond).
				Format("150405.000000000")
			b.AddServiceDeploymentInternal(
				&ecs.ServiceDeployment{
					ServiceDeploymentArn: arn,
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/default",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/default/svc",
					Status:               "IN_PROGRESS",
					CreatedAt:            &now,
					UpdatedAt:            &now,
				},
			)
		}
		h := ecs.NewHandler(b)
		first := doECSRequest(
			t,
			h,
			"ListServiceDeployments",
			map[string]any{"cluster": "default", "service": "svc", "maxResults": 2},
		)
		var b1 map[string]any
		require.NoError(t, json.Unmarshal(first.Body.Bytes(), &b1))
		second := doECSRequest(
			t,
			h,
			"ListServiceDeployments",
			map[string]any{
				"cluster":    "default",
				"service":    "svc",
				"maxResults": 2,
				"nextToken":  b1["nextToken"].(string),
			},
		)
		var b2 map[string]any
		require.NoError(t, json.Unmarshal(second.Body.Bytes(), &b2))
		assert.Len(t, b2["serviceDeploymentArns"].([]any), 1)
		assert.Empty(t, b2["nextToken"])
	})

	t.Run("list service deployments defaults max results", func(t *testing.T) {
		t.Parallel()
		b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
		now := time.Now()
		for i := range 101 {
			arn := "arn:aws:ecs:us-east-1:000000000000:service-deployment/default-" + time.Now().
				Add(time.Duration(i)*time.Nanosecond).
				Format("150405.000000000")
			b.AddServiceDeploymentInternal(
				&ecs.ServiceDeployment{
					ServiceDeploymentArn: arn,
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/default",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/default/svc",
					Status:               "IN_PROGRESS",
					CreatedAt:            &now,
					UpdatedAt:            &now,
				},
			)
		}
		h := ecs.NewHandler(b)
		rec := doECSRequest(
			t,
			h,
			"ListServiceDeployments",
			map[string]any{"cluster": "default", "service": "svc"},
		)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["serviceDeploymentArns"].([]any), 100)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list account settings page one", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"a", "b", "c"} {
			doECSRequest(
				t,
				h,
				"PutAccountSetting",
				map[string]any{"name": name, "value": "enabled"},
			)
		}
		rec := doECSRequest(t, h, "ListAccountSettings", map[string]any{"maxResults": 2})
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["settings"].([]any), 2)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list account settings page two", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"a", "b", "c"} {
			doECSRequest(
				t,
				h,
				"PutAccountSetting",
				map[string]any{"name": name, "value": "enabled"},
			)
		}
		first := doECSRequest(t, h, "ListAccountSettings", map[string]any{"maxResults": 2})
		var b1 map[string]any
		require.NoError(t, json.Unmarshal(first.Body.Bytes(), &b1))
		second := doECSRequest(
			t,
			h,
			"ListAccountSettings",
			map[string]any{"maxResults": 2, "nextToken": b1["nextToken"].(string)},
		)
		var b2 map[string]any
		require.NoError(t, json.Unmarshal(second.Body.Bytes(), &b2))
		assert.Len(t, b2["settings"].([]any), 1)
		assert.Empty(t, b2["nextToken"])
	})

	t.Run("list account settings filtered then paginated", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, principal := range []string{"p1", "p2", "p3"} {
			doECSRequest(
				t,
				h,
				"PutAccountSetting",
				map[string]any{
					"name":         "containerInsights",
					"value":        "enabled",
					"principalArn": principal,
				},
			)
		}
		rec := doECSRequest(
			t,
			h,
			"ListAccountSettings",
			map[string]any{"name": "containerInsights", "maxResults": 2},
		)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["settings"].([]any), 2)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list account settings default max results", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for i := range 102 {
			doECSRequest(
				t,
				h,
				"PutAccountSetting",
				map[string]any{
					"name": "setting-" + time.Now().
						Add(time.Duration(i)*time.Nanosecond).
						Format("150405.000000000"),
					"value": "enabled",
				},
			)
		}
		rec := doECSRequest(t, h, "ListAccountSettings", map[string]any{})
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["settings"].([]any), 100)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list attributes page one", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"a", "b", "c"} {
			doECSRequest(
				t,
				h,
				"PutAttributes",
				map[string]any{
					"attributes": []map[string]any{
						{
							"name":       name,
							"targetId":   "i-1",
							"targetType": "container-instance",
							"value":      "v",
						},
					},
				},
			)
		}
		rec := doECSRequest(t, h, "ListAttributes", map[string]any{"maxResults": 2})
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["attributes"].([]any), 2)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list attributes page two", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, name := range []string{"a", "b", "c"} {
			doECSRequest(
				t,
				h,
				"PutAttributes",
				map[string]any{
					"attributes": []map[string]any{
						{
							"name":       name,
							"targetId":   "i-1",
							"targetType": "container-instance",
							"value":      "v",
						},
					},
				},
			)
		}
		first := doECSRequest(t, h, "ListAttributes", map[string]any{"maxResults": 2})
		var b1 map[string]any
		require.NoError(t, json.Unmarshal(first.Body.Bytes(), &b1))
		second := doECSRequest(
			t,
			h,
			"ListAttributes",
			map[string]any{"maxResults": 2, "nextToken": b1["nextToken"].(string)},
		)
		var b2 map[string]any
		require.NoError(t, json.Unmarshal(second.Body.Bytes(), &b2))
		assert.Len(t, b2["attributes"].([]any), 1)
		assert.Empty(t, b2["nextToken"])
	})

	t.Run("list attributes filtered then paginated", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for _, target := range []string{"container-instance", "container-instance", "task"} {
			doECSRequest(
				t,
				h,
				"PutAttributes",
				map[string]any{
					"attributes": []map[string]any{
						{
							"name":       "shared",
							"targetId":   time.Now().Format("150405.000000000"),
							"targetType": target,
							"value":      "v",
						},
					},
				},
			)
		}
		rec := doECSRequest(
			t,
			h,
			"ListAttributes",
			map[string]any{"targetType": "container-instance", "maxResults": 1},
		)
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["attributes"].([]any), 1)
		assert.NotEmpty(t, body["nextToken"])
	})

	t.Run("list attributes default max results", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		for i := range 102 {
			doECSRequest(
				t,
				h,
				"PutAttributes",
				map[string]any{
					"attributes": []map[string]any{
						{
							"name": time.Now().
								Add(time.Duration(i) * time.Nanosecond).
								Format("150405.000000000"),
							"targetId":   "i-1",
							"targetType": "container-instance",
							"value":      "v",
						},
					},
				},
			)
		}
		rec := doECSRequest(t, h, "ListAttributes", map[string]any{})
		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Len(t, body["attributes"].([]any), 100)
		assert.NotEmpty(t, body["nextToken"])
	})
}
