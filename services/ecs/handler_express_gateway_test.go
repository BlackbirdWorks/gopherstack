package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestECS_CreateExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantName string
		wantCode int
	}{
		{
			name: "creates express gateway service",
			input: map[string]any{
				"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
				"serviceName":           "my-express-svc",
			},
			wantCode: http.StatusOK,
			wantName: "my-express-svc",
		},
		{
			name: "missing executionRoleArn returns 400",
			input: map[string]any{
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing infrastructureRoleArn returns 400",
			input: map[string]any{
				"executionRoleArn": "arn:aws:iam::000000000000:role/exec-role",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "with tags",
			input: map[string]any{
				"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
				"serviceName":           "tagged-svc",
				"tags":                  []map[string]any{{"key": "env", "value": "prod"}},
			},
			wantCode: http.StatusOK,
			wantName: "tagged-svc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "CreateExpressGatewayService", tt.input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantName, svc["serviceName"])
			assert.Equal(t, "ACTIVE", svc["status"].(map[string]any)["statusCode"])
			assert.NotEmpty(t, svc["serviceArn"])
		})
	}
}

func TestECS_CreateExpressGatewayService_DuplicateARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	input := map[string]any{
		"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
		"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
		"serviceName":           "my-svc",
	}

	doECSRequest(t, h, "CreateExpressGatewayService", input)
	rec := doECSRequest(t, h, "CreateExpressGatewayService", input)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "AlreadyExists")
}

func TestECS_DeleteExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createFn func(h *ecs.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "delete existing service",
			createFn: func(h *ecs.Handler) string {
				rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
					"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
					"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
					"serviceName":           "svc-to-delete",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))

				return r["service"].(map[string]any)["serviceArn"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "delete non-existent returns 400",
			createFn: func(_ *ecs.Handler) string {
				return "arn:nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty serviceArn returns 400",
			createFn: func(_ *ecs.Handler) string {
				return ""
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serviceArn := tt.createFn(h)

			rec := doECSRequest(
				t,
				h,
				"DeleteExpressGatewayService",
				map[string]any{"serviceArn": serviceArn},
			)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, serviceArn, svc["serviceArn"])
		})
	}
}

func TestECS_DescribeExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createFn func(h *ecs.Handler) string
		name     string
		wantCode int
	}{
		{
			name: "describe existing service",
			createFn: func(h *ecs.Handler) string {
				rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
					"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
					"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
					"serviceName":           "my-express",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var r map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &r))

				return r["service"].(map[string]any)["serviceArn"].(string)
			},
			wantCode: http.StatusOK,
		},
		{
			name: "describe non-existent returns 400",
			createFn: func(_ *ecs.Handler) string {
				return "arn:nonexistent"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			serviceArn := tt.createFn(h)

			rec := doECSRequest(
				t,
				h,
				"DescribeExpressGatewayService",
				map[string]any{"serviceArn": serviceArn},
			)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, serviceArn, svc["serviceArn"])
			assert.Equal(t, "ACTIVE", svc["status"].(map[string]any)["statusCode"])
		})
	}
}

func TestECS_ExpressGatewayService_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
		"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
		"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
		"serviceName":           "rt-svc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	arn := createResp["service"].(map[string]any)["serviceArn"].(string)

	// Describe
	rec = doECSRequest(t, h, "DescribeExpressGatewayService", map[string]any{"serviceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doECSRequest(t, h, "DeleteExpressGatewayService", map[string]any{"serviceArn": arn})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe after delete returns 400
	rec = doECSRequest(t, h, "DescribeExpressGatewayService", map[string]any{"serviceArn": arn})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_UpdateExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "updates service",
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var serviceArn string

			if tt.wantCode == http.StatusOK {
				rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
					"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
					"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
					"serviceName":           "my-gw-service",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				svc := createResp["service"].(map[string]any)
				serviceArn = svc["serviceArn"].(string)
			} else {
				serviceArn = "arn:aws:ecs:us-east-1:000000000000:service/nonexistent"
			}

			rec := doECSRequest(t, h, "UpdateExpressGatewayService", map[string]any{
				"serviceArn":            serviceArn,
				"executionRoleArn":      "arn:aws:iam::000000000000:role/new-exec",
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/new-infra",
			})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			// UpdateExpressGatewayServiceOutput.Service is the narrower
			// UpdatedExpressGatewayService shape (types.UpdatedExpressGatewayService):
			// no top-level executionRoleArn/infrastructureRoleArn -- the new
			// execution role lands on targetConfiguration (the new service
			// revision), so verify it there and confirm the infrastructure role
			// change via a follow-up Describe.
			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, serviceArn, svc["serviceArn"])
			targetCfg, ok := svc["targetConfiguration"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "arn:aws:iam::000000000000:role/new-exec", targetCfg["executionRoleArn"])

			descRec := doECSRequest(
				t, h, "DescribeExpressGatewayService", map[string]any{"serviceArn": serviceArn},
			)
			require.Equal(t, http.StatusOK, descRec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
			descSvc := descResp["service"].(map[string]any)
			assert.Equal(t, "arn:aws:iam::000000000000:role/new-infra", descSvc["infrastructureRoleArn"])
		})
	}
}

func TestExpressGatewayService_DeepCopy_Tags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		tagKey  string
		tagVal  string
		wantTag string
		mutate  bool
	}{
		{
			name:    "tags are independent after create",
			tagKey:  "team",
			tagVal:  "platform",
			mutate:  true,
			wantTag: "platform",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

			tags := []ecs.Tag{{Key: tt.tagKey, Value: tt.tagVal}}
			svc, err := b.CreateExpressGatewayService(ecs.CreateExpressGatewayServiceInput{
				ExecutionRoleArn:      "arn:aws:iam::000000000000:role/exec",
				InfrastructureRoleArn: "arn:aws:iam::000000000000:role/infra",
				ServiceName:           "gw-test",
				Tags:                  tags,
			})
			require.NoError(t, err)

			if tt.mutate && len(svc.Tags) > 0 {
				svc.Tags[0].Value = "mutated"
			}

			got, err := b.DescribeExpressGatewayService(svc.ServiceArn)
			require.NoError(t, err)
			require.Len(t, got.Tags, 1)
			assert.Equal(t, tt.wantTag, got.Tags[0].Value)
		})
	}
}

// TestExpressGatewayService_TagResource_VisibleOnDescribe proves that a
// TagResource call on an Express service ARN is reflected on a subsequent
// DescribeExpressGatewayService(include=[TAGS]) call. Previously
// svc.Tags (echoed on Create/Describe/Update) and the resourceTags side map
// (updated by TagResource/UntagResource, read by ListTagsForResource) were
// two independent, never-synchronized copies: TagResource "succeeded" but
// was invisible on Describe, and creation-time tags were invisible to
// ListTagsForResource.
func TestExpressGatewayService_TagResource_VisibleOnDescribe(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createResp := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
		"executionRoleArn":      "arn:aws:iam::000000000000:role/exec-role",
		"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra-role",
		"serviceName":           "tag-sync-svc",
	})
	require.Equal(t, http.StatusOK, createResp.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createOut))
	serviceArn := createOut["service"].(map[string]any)["serviceArn"].(string)

	tagResp := doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": serviceArn,
		"tags":        []map[string]any{{"key": "team", "value": "platform"}},
	})
	require.Equal(t, http.StatusOK, tagResp.Code)

	// ListTagsForResource sees it immediately.
	listResp := doECSRequest(t, h, "ListTagsForResource", map[string]any{
		"resourceArn": serviceArn,
	})
	require.Equal(t, http.StatusOK, listResp.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	listTags := listOut["tags"].([]any)
	require.Len(t, listTags, 1)
	assert.Equal(t, "team", listTags[0].(map[string]any)["key"])

	// DescribeExpressGatewayService with Include=[TAGS] also sees it.
	descResp := doECSRequest(t, h, "DescribeExpressGatewayService", map[string]any{
		"serviceArn": serviceArn,
		"include":    []string{"TAGS"},
	})
	require.Equal(t, http.StatusOK, descResp.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	descTags := descOut["service"].(map[string]any)["tags"].([]any)
	require.Len(t, descTags, 1)
	assert.Equal(t, "team", descTags[0].(map[string]any)["key"])

	// Without Include=[TAGS], tags are omitted.
	noTagsResp := doECSRequest(t, h, "DescribeExpressGatewayService", map[string]any{
		"serviceArn": serviceArn,
	})
	require.Equal(t, http.StatusOK, noTagsResp.Code)

	var noTagsOut map[string]any
	require.NoError(t, json.Unmarshal(noTagsResp.Body.Bytes(), &noTagsOut))
	assert.Nil(t, noTagsOut["service"].(map[string]any)["tags"])
}

// TestExpressGatewayService_RevisionConfiguration_SDKRoundTrip proves, through
// the real aws-sdk-go-v2 ECS client (not ad-hoc map[string]any assertions),
// that CreateExpressGatewayService/UpdateExpressGatewayService now carry the
// Cpu/Memory/HealthCheckPath/NetworkConfiguration/PrimaryContainer/
// ScalingTarget/TaskRoleArn fields into a real ActiveConfigurations service
// revision, and that CurrentDeployment/UpdatedAt/Status are populated.
// Previously ExpressGatewayService had none of these: Create/Update/Describe
// only round-tripped ServiceArn/ServiceName/Cluster/Status/ExecutionRoleArn/
// InfrastructureRoleArn/Tags, so a real client reading
// service.ActiveConfigurations[0].Cpu (or any other revision field) got a
// zero value no matter what the caller submitted.
func TestExpressGatewayService_RevisionConfiguration_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "create_and_describe_round_trip_revision_fields",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestECSClient(t, h)

				createOut, err := client.CreateExpressGatewayService(
					t.Context(), &ecssdk.CreateExpressGatewayServiceInput{
						InfrastructureRoleArn: aws.String("arn:aws:iam::000000000000:role/infra"),
						ExecutionRoleArn:      aws.String("arn:aws:iam::000000000000:role/exec"),
						TaskRoleArn:           aws.String("arn:aws:iam::000000000000:role/task"),
						ServiceName:           aws.String("rt-revision-svc"),
						Cpu:                   aws.String("512"),
						Memory:                aws.String("1024"),
						HealthCheckPath:       aws.String("/health"),
						NetworkConfiguration: &ecstypes.ExpressGatewayServiceNetworkConfiguration{
							SecurityGroups: []string{"sg-1"},
							Subnets:        []string{"subnet-1"},
						},
						PrimaryContainer: &ecstypes.ExpressGatewayContainer{
							Image:         aws.String("nginx:latest"),
							ContainerPort: aws.Int32(8080),
							Command:       []string{"start"},
							Environment: []ecstypes.KeyValuePair{
								{Name: aws.String("ENV"), Value: aws.String("prod")},
							},
						},
						ScalingTarget: &ecstypes.ExpressGatewayScalingTarget{
							AutoScalingMetric:      ecstypes.ExpressGatewayServiceScalingMetricAverageCPUUtilization,
							AutoScalingTargetValue: aws.Int32(70),
							MinTaskCount:           aws.Int32(1),
							MaxTaskCount:           aws.Int32(5),
						},
					},
				)
				require.NoError(t, err)
				require.NotNil(t, createOut.Service)
				assertExpressGatewayRevision(t, createOut.Service)

				descOut, err := client.DescribeExpressGatewayService(
					t.Context(), &ecssdk.DescribeExpressGatewayServiceInput{
						ServiceArn: createOut.Service.ServiceArn,
					},
				)
				require.NoError(t, err)
				assertExpressGatewayRevision(t, descOut.Service)
			},
		},
		{
			name: "update_replaces_active_configuration_with_new_revision",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestECSClient(t, h)

				createOut, err := client.CreateExpressGatewayService(
					t.Context(), &ecssdk.CreateExpressGatewayServiceInput{
						InfrastructureRoleArn: aws.String("arn:aws:iam::000000000000:role/infra"),
						ExecutionRoleArn:      aws.String("arn:aws:iam::000000000000:role/exec"),
						ServiceName:           aws.String("rt-update-svc"),
						Cpu:                   aws.String("256"),
					},
				)
				require.NoError(t, err)
				firstRevisionArn := createOut.Service.ActiveConfigurations[0].ServiceRevisionArn

				updateOut, err := client.UpdateExpressGatewayService(
					t.Context(), &ecssdk.UpdateExpressGatewayServiceInput{
						ServiceArn:       createOut.Service.ServiceArn,
						ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/new-exec"),
						TaskRoleArn:      aws.String("arn:aws:iam::000000000000:role/new-task"),
						Cpu:              aws.String("1024"),
						Memory:           aws.String("2048"),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, updateOut.Service)
				require.NotNil(t, updateOut.Service.TargetConfiguration)
				assert.Equal(t, "1024", aws.ToString(updateOut.Service.TargetConfiguration.Cpu))
				assert.Equal(t, "2048", aws.ToString(updateOut.Service.TargetConfiguration.Memory))
				assert.Equal(
					t,
					"arn:aws:iam::000000000000:role/new-exec",
					aws.ToString(updateOut.Service.TargetConfiguration.ExecutionRoleArn),
				)
				assert.NotEqual(
					t, firstRevisionArn,
					aws.ToString(updateOut.Service.TargetConfiguration.ServiceRevisionArn),
					"update must roll out a new service revision, not mutate the old one",
				)

				descOut, err := client.DescribeExpressGatewayService(
					t.Context(), &ecssdk.DescribeExpressGatewayServiceInput{
						ServiceArn: createOut.Service.ServiceArn,
					},
				)
				require.NoError(t, err)
				require.Len(t, descOut.Service.ActiveConfigurations, 1,
					"the new revision replaces the old one as the sole active configuration")
				assert.Equal(t, "1024", aws.ToString(descOut.Service.ActiveConfigurations[0].Cpu))
				assert.Equal(
					t, aws.ToString(updateOut.Service.TargetConfiguration.ServiceRevisionArn),
					aws.ToString(descOut.Service.CurrentDeployment),
				)
			},
		},
		{
			name: "task_definition_arn_rejects_primary_container",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestECSClient(t, h)

				_, err := client.CreateExpressGatewayService(
					t.Context(), &ecssdk.CreateExpressGatewayServiceInput{
						InfrastructureRoleArn: aws.String("arn:aws:iam::000000000000:role/infra"),
						ServiceName:           aws.String("rt-mutex-svc"),
						TaskDefinitionArn: aws.String(
							"arn:aws:ecs:us-east-1:000000000000:task-definition/mine:1",
						),
						PrimaryContainer: &ecstypes.ExpressGatewayContainer{
							Image: aws.String("nginx:latest"),
						},
					},
				)
				require.Error(
					t, err,
					"taskDefinitionArn and primaryContainer are mutually exclusive per the real API",
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// assertExpressGatewayRevision asserts the full set of revision fields on an
// ECSExpressGatewayService decoded through the real SDK client, shared by the
// Create and Describe legs of TestExpressGatewayService_RevisionConfiguration_SDKRoundTrip.
func assertExpressGatewayRevision(t *testing.T, svc *ecstypes.ECSExpressGatewayService) {
	t.Helper()

	require.NotNil(t, svc.Status)
	assert.Equal(t, ecstypes.ExpressGatewayServiceStatusCodeActive, svc.Status.StatusCode)
	require.NotNil(t, svc.UpdatedAt)
	require.NotNil(t, svc.CreatedAt)
	require.NotEmpty(t, aws.ToString(svc.CurrentDeployment))

	require.Len(t, svc.ActiveConfigurations, 1)
	cfg := svc.ActiveConfigurations[0]
	assert.Equal(t, "512", aws.ToString(cfg.Cpu))
	assert.Equal(t, "1024", aws.ToString(cfg.Memory))
	assert.Equal(t, "/health", aws.ToString(cfg.HealthCheckPath))
	assert.Equal(t, "arn:aws:iam::000000000000:role/exec", aws.ToString(cfg.ExecutionRoleArn))
	assert.Equal(t, "arn:aws:iam::000000000000:role/task", aws.ToString(cfg.TaskRoleArn))
	assert.Equal(t, aws.ToString(svc.CurrentDeployment), aws.ToString(cfg.ServiceRevisionArn))

	require.NotNil(t, cfg.NetworkConfiguration)
	assert.Equal(t, []string{"sg-1"}, cfg.NetworkConfiguration.SecurityGroups)
	assert.Equal(t, []string{"subnet-1"}, cfg.NetworkConfiguration.Subnets)

	require.NotNil(t, cfg.PrimaryContainer)
	assert.Equal(t, "nginx:latest", aws.ToString(cfg.PrimaryContainer.Image))
	assert.Equal(t, int32(8080), aws.ToInt32(cfg.PrimaryContainer.ContainerPort))
	assert.Equal(t, []string{"start"}, cfg.PrimaryContainer.Command)
	require.Len(t, cfg.PrimaryContainer.Environment, 1)
	assert.Equal(t, "ENV", aws.ToString(cfg.PrimaryContainer.Environment[0].Name))

	require.NotNil(t, cfg.ScalingTarget)
	assert.Equal(
		t, ecstypes.ExpressGatewayServiceScalingMetricAverageCPUUtilization,
		cfg.ScalingTarget.AutoScalingMetric,
	)
	assert.Equal(t, int32(70), aws.ToInt32(cfg.ScalingTarget.AutoScalingTargetValue))
	assert.Equal(t, int32(1), aws.ToInt32(cfg.ScalingTarget.MinTaskCount))
	assert.Equal(t, int32(5), aws.ToInt32(cfg.ScalingTarget.MaxTaskCount))
}
