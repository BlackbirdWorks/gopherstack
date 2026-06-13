package pipes_test

// Audit batch-2: AWS-accuracy coverage for EventBridge Pipes (go-zh21).
//
// Covers:
//   - ECS RunTaskParameters: NetworkConfiguration, CapacityProviderStrategy,
//     PlacementConstraints, PlacementStrategy, Overrides, Group, PlatformVersion,
//     EnableECSManagedTags, EnableExecuteCommand
//   - Batch: DependsOn, ContainerOverrides, ArrayProperties, RetryStrategy
//   - StepFunctions: FIRE_AND_FORGET vs REQUEST_RESPONSE invocation types
//   - EventBridge EventBus: DetailType, EndpointId, Source, Resources
//   - RedshiftData: Database, DbUser, SecretManagerArn, Sqls, StatementName, WithEvent
//   - SageMaker: PipelineParameterList multi-param
//   - Self-managed Kafka: Credentials (all 4 types), Vpc, StartingPosition, ConsumerGroupID
//   - MSK: Credentials (both types), StartingPosition, ConsumerGroupID
//   - ActiveMQ: Credentials (BasicAuth)
//   - RabbitMQ: Credentials (BasicAuth), VirtualHost
//   - RuntimeMetricsStreaming: CloudWatch namespace + level
//   - FilterCriteria: multiple patterns, empty pattern passthrough, no match
//   - Immutability: clone isolation for all new param types
//   - HTTP roundtrip for all new param types via Handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// --- helpers ---

func b2Backend() *pipes.InMemoryBackend {
	return pipes.NewInMemoryBackend("123456789012", "us-east-1")
}

func b2Handler(t *testing.T) *pipes.Handler {
	t.Helper()

	return pipes.NewHandler(b2Backend())
}

func b2Do(t *testing.T, h *pipes.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(t, err)
	}
	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/pipes/aws4_request")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))

	return rec
}

func b2Create(t *testing.T, h *pipes.Handler, name string, body map[string]any) map[string]any {
	t.Helper()
	rec := b2Do(t, h, http.MethodPost, "/v1/pipes/"+name, body)
	require.Equal(t, http.StatusOK, rec.Code, "create pipe %q: %s", name, rec.Body.String())
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func b2Describe(t *testing.T, h *pipes.Handler, name string) map[string]any {
	t.Helper()
	rec := b2Do(t, h, http.MethodGet, "/v1/pipes/"+name, nil)
	require.Equal(t, http.StatusOK, rec.Code, "describe pipe %q: %s", name, rec.Body.String())
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

func b2Update(t *testing.T, h *pipes.Handler, name string, body map[string]any) map[string]any {
	t.Helper()
	rec := b2Do(t, h, http.MethodPut, "/v1/pipes/"+name, body)
	require.Equal(t, http.StatusOK, rec.Code, "update pipe %q: %s", name, rec.Body.String())
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp
}

const (
	b2SQSSource    = "arn:aws:sqs:us-east-1:123456789012:queue"
	b2LambdaTarget = "arn:aws:lambda:us-east-1:123456789012:function:fn"
	b2SFNTarget    = "arn:aws:states:us-east-1:123456789012:stateMachine:sm"
	b2ECSTarget    = "arn:aws:ecs:us-east-1:123456789012:cluster/cluster"
)

// nestedString extracts a string from nested map[string]any.
func nestedString(t *testing.T, m map[string]any, keys ...string) string {
	t.Helper()
	cur := m
	for i, k := range keys {
		if i == len(keys)-1 {
			v, ok := cur[k]
			require.True(t, ok, "key %q missing in %v", k, cur)
			s, ok := v.(string)
			require.True(t, ok, "key %q is not string: %T", k, v)

			return s
		}
		sub, ok := cur[k]
		require.True(t, ok, "intermediate key %q missing", k)
		cur, ok = sub.(map[string]any)
		require.True(t, ok, "intermediate key %q is not object: %T", k, sub)
	}

	return ""
}

// nestedFloat extracts a float64 from nested map[string]any.
func nestedFloat(t *testing.T, m map[string]any, keys ...string) float64 {
	t.Helper()
	cur := m
	for i, k := range keys {
		if i == len(keys)-1 {
			v, ok := cur[k]
			require.True(t, ok, "key %q missing", k)
			f, ok := v.(float64)
			require.True(t, ok, "key %q is not float64: %T", k, v)

			return f
		}
		sub, ok := cur[k]
		require.True(t, ok, "intermediate key %q missing", k)
		cur, ok = sub.(map[string]any)
		require.True(t, ok, "intermediate key %q not object: %T", k, sub)
	}

	return 0
}

// --- ECS RunTaskParameters tests ---

// TestAudit2_ECS_NetworkConfiguration verifies ECS network config round-trips through HTTP.
func TestAudit2_ECS_NetworkConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		assignPublicIP string
		subnets        []any
		securityGroups []any
	}{
		{
			name:           "fargate_with_public_ip",
			subnets:        []any{"subnet-aaa", "subnet-bbb"},
			securityGroups: []any{"sg-111", "sg-222"},
			assignPublicIP: "ENABLED",
		},
		{
			name:           "ec2_no_public_ip",
			subnets:        []any{"subnet-ccc"},
			securityGroups: []any{"sg-333"},
			assignPublicIP: "DISABLED",
		},
		{
			name:           "single_subnet_no_sg",
			subnets:        []any{"subnet-ddd"},
			securityGroups: nil,
			assignPublicIP: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": b2SQSSource,
				"Target": b2ECSTarget,
				"TargetParameters": map[string]any{
					"EcsTaskParameters": map[string]any{
						"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
						"LaunchType":        "FARGATE",
						"NetworkConfiguration": map[string]any{
							"AwsvpcConfiguration": map[string]any{
								"Subnets":        tt.subnets,
								"SecurityGroups": tt.securityGroups,
								"AssignPublicIp": tt.assignPublicIP,
							},
						},
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp, ok := resp["TargetParameters"].(map[string]any)
			require.True(t, ok, "TargetParameters missing")
			ecs, ok := tp["EcsTaskParameters"].(map[string]any)
			require.True(t, ok, "EcsTaskParameters missing")
			nc, ok := ecs["NetworkConfiguration"].(map[string]any)
			require.True(t, ok, "NetworkConfiguration missing")
			vpc, ok := nc["AwsvpcConfiguration"].(map[string]any)
			require.True(t, ok, "AwsvpcConfiguration missing")

			if len(tt.subnets) > 0 {
				got, subnetOK := vpc["Subnets"].([]any)
				require.True(t, subnetOK, "Subnets should be array")
				assert.Len(t, got, len(tt.subnets))
				assert.Equal(t, tt.subnets[0], got[0])
			}
			if tt.assignPublicIP != "" {
				assert.Equal(t, tt.assignPublicIP, vpc["AssignPublicIp"])
			}
		})
	}
}

// TestAudit2_ECS_CapacityProviderStrategy verifies capacity provider strategy round-trips.
func TestAudit2_ECS_CapacityProviderStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		providers []map[string]any
		wantLen   int
	}{
		{
			name: "single_fargate",
			providers: []map[string]any{
				{"CapacityProvider": "FARGATE", "Weight": 1, "Base": 0},
			},
			wantLen: 1,
		},
		{
			name: "fargate_spot_fallback",
			providers: []map[string]any{
				{"CapacityProvider": "FARGATE", "Weight": 1, "Base": 1},
				{"CapacityProvider": "FARGATE_SPOT", "Weight": 3, "Base": 0},
			},
			wantLen: 2,
		},
		{
			name: "custom_capacity_provider",
			providers: []map[string]any{
				{"CapacityProvider": "my-cp", "Weight": 2, "Base": 0},
			},
			wantLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": b2SQSSource,
				"Target": b2ECSTarget,
				"TargetParameters": map[string]any{
					"EcsTaskParameters": map[string]any{
						"TaskDefinitionArn":        "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
						"CapacityProviderStrategy": tt.providers,
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			ecs := tp["EcsTaskParameters"].(map[string]any)
			cps, ok := ecs["CapacityProviderStrategy"].([]any)
			require.True(t, ok, "CapacityProviderStrategy should be array")
			assert.Len(t, cps, tt.wantLen)

			first := cps[0].(map[string]any)
			assert.Equal(t, tt.providers[0]["CapacityProvider"], first["CapacityProvider"])
		})
	}
}

// TestAudit2_ECS_PlacementConstraints verifies ECS placement constraints persist.
func TestAudit2_ECS_PlacementConstraints(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		constraints []map[string]any
		wantLen     int
	}{
		{
			name: "distinct_instance",
			constraints: []map[string]any{
				{"Type": "distinctInstance"},
			},
			wantLen: 1,
		},
		{
			name: "member_of_with_expression",
			constraints: []map[string]any{
				{"Type": "memberOf", "Expression": "attribute:ecs.instance-type =~ t3.*"},
			},
			wantLen: 1,
		},
		{
			name: "multiple_constraints",
			constraints: []map[string]any{
				{"Type": "distinctInstance"},
				{"Type": "memberOf", "Expression": "attribute:ecs.az =~ us-east-1*"},
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": b2SQSSource,
				"Target": b2ECSTarget,
				"TargetParameters": map[string]any{
					"EcsTaskParameters": map[string]any{
						"TaskDefinitionArn":    "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
						"PlacementConstraints": tt.constraints,
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			ecs := tp["EcsTaskParameters"].(map[string]any)
			pc, ok := ecs["PlacementConstraints"].([]any)
			require.True(t, ok, "PlacementConstraints should be array")
			assert.Len(t, pc, tt.wantLen)

			first := pc[0].(map[string]any)
			assert.Equal(t, tt.constraints[0]["Type"], first["Type"])
		})
	}
}

// TestAudit2_ECS_PlacementStrategy verifies ECS placement strategy persists.
func TestAudit2_ECS_PlacementStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantType  string
		wantField string
		strategy  []map[string]any
		wantLen   int
	}{
		{
			name:      "spread_by_az",
			strategy:  []map[string]any{{"Type": "spread", "Field": "attribute:ecs.availability-zone"}},
			wantLen:   1,
			wantType:  "spread",
			wantField: "attribute:ecs.availability-zone",
		},
		{
			name:      "binpack_memory",
			strategy:  []map[string]any{{"Type": "binpack", "Field": "memory"}},
			wantLen:   1,
			wantType:  "binpack",
			wantField: "memory",
		},
		{
			name: "random_strategy",
			strategy: []map[string]any{
				{"Type": "random"},
			},
			wantLen:  1,
			wantType: "random",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": b2SQSSource,
				"Target": b2ECSTarget,
				"TargetParameters": map[string]any{
					"EcsTaskParameters": map[string]any{
						"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
						"PlacementStrategy": tt.strategy,
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			ecs := tp["EcsTaskParameters"].(map[string]any)
			ps, ok := ecs["PlacementStrategy"].([]any)
			require.True(t, ok, "PlacementStrategy should be array")
			assert.Len(t, ps, tt.wantLen)

			first := ps[0].(map[string]any)
			assert.Equal(t, tt.wantType, first["Type"])
			if tt.wantField != "" {
				assert.Equal(t, tt.wantField, first["Field"])
			}
		})
	}
}

// TestAudit2_ECS_Overrides verifies ECS task override fields persist.
func TestAudit2_ECS_Overrides(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		taskRoleArn      string
		executionRoleArn string
		cpu              string
		memory           string
	}{
		{
			name:        "role_override",
			taskRoleArn: "arn:aws:iam::123456789012:role/task-role",
		},
		{
			name:             "execution_role_override",
			executionRoleArn: "arn:aws:iam::123456789012:role/exec-role",
		},
		{
			name:   "cpu_memory_override",
			cpu:    "512",
			memory: "1024",
		},
		{
			name:             "full_override",
			taskRoleArn:      "arn:aws:iam::123456789012:role/task-role",
			executionRoleArn: "arn:aws:iam::123456789012:role/exec-role",
			cpu:              "1024",
			memory:           "2048",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			overrides := map[string]any{}
			if tt.taskRoleArn != "" {
				overrides["TaskRoleArn"] = tt.taskRoleArn
			}
			if tt.executionRoleArn != "" {
				overrides["ExecutionRoleArn"] = tt.executionRoleArn
			}
			if tt.cpu != "" {
				overrides["Cpu"] = tt.cpu
			}
			if tt.memory != "" {
				overrides["Memory"] = tt.memory
			}

			body := map[string]any{
				"Source": b2SQSSource,
				"Target": b2ECSTarget,
				"TargetParameters": map[string]any{
					"EcsTaskParameters": map[string]any{
						"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
						"Overrides":         overrides,
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			ecs := tp["EcsTaskParameters"].(map[string]any)
			ov, ok := ecs["Overrides"].(map[string]any)
			require.True(t, ok, "Overrides should be object")

			if tt.taskRoleArn != "" {
				assert.Equal(t, tt.taskRoleArn, ov["TaskRoleArn"])
			}
			if tt.executionRoleArn != "" {
				assert.Equal(t, tt.executionRoleArn, ov["ExecutionRoleArn"])
			}
			if tt.cpu != "" {
				assert.Equal(t, tt.cpu, ov["Cpu"])
			}
			if tt.memory != "" {
				assert.Equal(t, tt.memory, ov["Memory"])
			}
		})
	}
}

// TestAudit2_ECS_ExtraFields verifies Group, PlatformVersion, EnableECSManagedTags, EnableExecuteCommand.
func TestAudit2_ECS_ExtraFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                 string
		group                string
		platformVersion      string
		enableECSManagedTags bool
		enableExecuteCommand bool
		taskCount            float64
	}{
		{
			name:            "group_and_platform",
			group:           "service:my-svc",
			platformVersion: "1.4.0",
			taskCount:       2,
		},
		{
			name:                 "execute_command_enabled",
			enableExecuteCommand: true,
		},
		{
			name:                 "ecs_managed_tags",
			enableECSManagedTags: true,
		},
		{
			name:                 "all_flags",
			group:                "family:td",
			platformVersion:      "LATEST",
			enableECSManagedTags: true,
			enableExecuteCommand: true,
			taskCount:            3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			ecsParams := map[string]any{
				"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
			}
			if tt.group != "" {
				ecsParams["Group"] = tt.group
			}
			if tt.platformVersion != "" {
				ecsParams["PlatformVersion"] = tt.platformVersion
			}
			if tt.enableECSManagedTags {
				ecsParams["EnableECSManagedTags"] = true
			}
			if tt.enableExecuteCommand {
				ecsParams["EnableExecuteCommand"] = true
			}
			if tt.taskCount > 0 {
				ecsParams["TaskCount"] = tt.taskCount
			}

			body := map[string]any{
				"Source": b2SQSSource,
				"Target": b2ECSTarget,
				"TargetParameters": map[string]any{
					"EcsTaskParameters": ecsParams,
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			ecs := tp["EcsTaskParameters"].(map[string]any)

			if tt.group != "" {
				assert.Equal(t, tt.group, ecs["Group"])
			}
			if tt.platformVersion != "" {
				assert.Equal(t, tt.platformVersion, ecs["PlatformVersion"])
			}
			if tt.enableECSManagedTags {
				assert.Equal(t, true, ecs["EnableECSManagedTags"])
			}
			if tt.enableExecuteCommand {
				assert.Equal(t, true, ecs["EnableExecuteCommand"])
			}
			if tt.taskCount > 0 {
				assert.InEpsilon(t, tt.taskCount, ecs["TaskCount"], 0.01)
			}
		})
	}
}

// TestAudit2_ECS_FullParams verifies all ECS params combined.
func TestAudit2_ECS_FullParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantGroup string
	}{
		{name: "ecs_full_a", wantGroup: "service:alpha"},
		{name: "ecs_full_b", wantGroup: "service:beta"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: b2SQSSource,
				Target: b2ECSTarget,
				TargetParameters: &pipes.TargetParameters{
					EcsTaskParameters: &pipes.ECSTaskTargetParameters{
						TaskDefinitionArn:    "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
						LaunchType:           "FARGATE",
						Group:                tt.wantGroup,
						PlatformVersion:      "1.4.0",
						TaskCount:            2,
						EnableECSManagedTags: true,
						EnableExecuteCommand: true,
						NetworkConfiguration: &pipes.NetworkConfiguration{
							AwsvpcConfiguration: &pipes.AwsVpcConfiguration{
								Subnets:        []string{"subnet-aaa", "subnet-bbb"},
								SecurityGroups: []string{"sg-111"},
								AssignPublicIP: "ENABLED",
							},
						},
						CapacityProviderStrategy: []pipes.CapacityProviderStrategyItem{
							{CapacityProvider: "FARGATE", Weight: 1, Base: 1},
							{CapacityProvider: "FARGATE_SPOT", Weight: 3, Base: 0},
						},
						PlacementConstraints: []pipes.PlacementConstraint{
							{Type: "memberOf", Expression: "attribute:ecs.az =~ us-east-1*"},
						},
						PlacementStrategy: []pipes.PlacementStrategy{
							{Type: "spread", Field: "attribute:ecs.availability-zone"},
						},
						Overrides: &pipes.EcsTaskOverride{
							TaskRoleArn: "arn:aws:iam::123456789012:role/task-role",
							CPU:         "512",
							Memory:      "1024",
						},
					},
				},
			})
			require.NoError(t, err)

			p, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)

			ecs := p.TargetParameters.EcsTaskParameters
			assert.Equal(t, tt.wantGroup, ecs.Group)
			assert.Equal(t, "1.4.0", ecs.PlatformVersion)
			assert.Equal(t, 2, ecs.TaskCount)
			assert.True(t, ecs.EnableECSManagedTags)
			assert.True(t, ecs.EnableExecuteCommand)
			require.NotNil(t, ecs.NetworkConfiguration)
			assert.Equal(t, "ENABLED", ecs.NetworkConfiguration.AwsvpcConfiguration.AssignPublicIP)
			assert.Len(t, ecs.CapacityProviderStrategy, 2)
			assert.Equal(t, "FARGATE_SPOT", ecs.CapacityProviderStrategy[1].CapacityProvider)
			assert.Len(t, ecs.PlacementConstraints, 1)
			assert.Equal(t, "memberOf", ecs.PlacementConstraints[0].Type)
			assert.Len(t, ecs.PlacementStrategy, 1)
			assert.Equal(t, "spread", ecs.PlacementStrategy[0].Type)
			require.NotNil(t, ecs.Overrides)
			assert.Equal(t, "512", ecs.Overrides.CPU)
		})
	}
}

// --- Batch target tests ---

// TestAudit2_Batch_DependsOn verifies Batch DependsOn persists through HTTP.
func TestAudit2_Batch_DependsOn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantType  string
		dependsOn []map[string]any
		wantLen   int
	}{
		{
			name:      "sequential_dependency",
			dependsOn: []map[string]any{{"JobId": "job-aaa", "Type": "SEQUENTIAL"}},
			wantLen:   1,
			wantType:  "SEQUENTIAL",
		},
		{
			name:      "n_to_n_dependency",
			dependsOn: []map[string]any{{"JobId": "job-bbb", "Type": "N_TO_N"}},
			wantLen:   1,
			wantType:  "N_TO_N",
		},
		{
			name: "multiple_dependencies",
			dependsOn: []map[string]any{
				{"JobId": "job-aaa", "Type": "SEQUENTIAL"},
				{"JobId": "job-bbb", "Type": "N_TO_N"},
			},
			wantLen:  2,
			wantType: "SEQUENTIAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": b2SQSSource,
				"Target": "arn:aws:batch:us-east-1:123456789012:job-queue/q",
				"TargetParameters": map[string]any{
					"BatchJobParameters": map[string]any{
						"JobDefinition": "arn:aws:batch:us-east-1:123456789012:job-definition/jd:1",
						"JobName":       "my-job",
						"DependsOn":     tt.dependsOn,
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			batch := tp["BatchJobParameters"].(map[string]any)
			deps, ok := batch["DependsOn"].([]any)
			require.True(t, ok, "DependsOn should be array")
			assert.Len(t, deps, tt.wantLen)

			first := deps[0].(map[string]any)
			assert.Equal(t, tt.wantType, first["Type"])
		})
	}
}

// TestAudit2_Batch_ContainerOverrides verifies Batch container overrides.
func TestAudit2_Batch_ContainerOverrides(t *testing.T) {
	t.Parallel()

	tests := []struct {
		environment  map[string]any
		name         string
		instanceType string
		command      []any
	}{
		{
			name:    "command_override",
			command: []any{"echo", "hello"},
		},
		{
			name:         "instance_type_override",
			instanceType: "c5.xlarge",
		},
		{
			name:        "env_override",
			environment: map[string]any{"KEY": "VALUE", "ENV": "prod"},
		},
		{
			name:         "full_override",
			command:      []any{"bash", "-c", "echo hi"},
			instanceType: "m5.large",
			environment:  map[string]any{"LOG_LEVEL": "DEBUG"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			co := map[string]any{}
			if tt.command != nil {
				co["Command"] = tt.command
			}
			if tt.instanceType != "" {
				co["InstanceType"] = tt.instanceType
			}
			if tt.environment != nil {
				co["Environment"] = tt.environment
			}

			body := map[string]any{
				"Source": b2SQSSource,
				"Target": "arn:aws:batch:us-east-1:123456789012:job-queue/q",
				"TargetParameters": map[string]any{
					"BatchJobParameters": map[string]any{
						"JobDefinition":      "arn:aws:batch:us-east-1:123456789012:job-definition/jd:1",
						"JobName":            "my-job",
						"ContainerOverrides": co,
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			batch := tp["BatchJobParameters"].(map[string]any)
			got, ok := batch["ContainerOverrides"].(map[string]any)
			require.True(t, ok, "ContainerOverrides should be object")

			if tt.instanceType != "" {
				assert.Equal(t, tt.instanceType, got["InstanceType"])
			}
			if tt.command != nil {
				cmd, cmdOK := got["Command"].([]any)
				require.True(t, cmdOK)
				assert.Len(t, cmd, len(tt.command))
			}
		})
	}
}

// TestAudit2_Batch_ArrayProperties verifies BatchArrayProperties persists.
func TestAudit2_Batch_ArrayProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		size   float64
		wantSz float64
	}{
		{name: "size_2", size: 2, wantSz: 2},
		{name: "size_100", size: 100, wantSz: 100},
		{name: "size_10000", size: 10000, wantSz: 10000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": b2SQSSource,
				"Target": "arn:aws:batch:us-east-1:123456789012:job-queue/q",
				"TargetParameters": map[string]any{
					"BatchJobParameters": map[string]any{
						"JobDefinition":   "arn:aws:batch:us-east-1:123456789012:job-definition/jd:1",
						"JobName":         "my-job",
						"ArrayProperties": map[string]any{"Size": tt.size},
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			sz := nestedFloat(t, resp,
				"TargetParameters", "BatchJobParameters", "ArrayProperties", "Size")
			assert.InEpsilon(t, tt.wantSz, sz, 0.01)
		})
	}
}

// TestAudit2_Batch_RetryStrategy verifies BatchRetryStrategy persists.
func TestAudit2_Batch_RetryStrategy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		attempts     float64
		wantAttempts float64
	}{
		{name: "retry_once", attempts: 1, wantAttempts: 1},
		{name: "retry_three", attempts: 3, wantAttempts: 3},
		{name: "retry_ten", attempts: 10, wantAttempts: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": b2SQSSource,
				"Target": "arn:aws:batch:us-east-1:123456789012:job-queue/q",
				"TargetParameters": map[string]any{
					"BatchJobParameters": map[string]any{
						"JobDefinition": "arn:aws:batch:us-east-1:123456789012:job-definition/jd:1",
						"JobName":       "my-job",
						"RetryStrategy": map[string]any{"Attempts": tt.attempts},
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			got := nestedFloat(t, resp,
				"TargetParameters", "BatchJobParameters", "RetryStrategy", "Attempts")
			assert.InEpsilon(t, tt.wantAttempts, got, 0.01)
		})
	}
}

// TestAudit2_Batch_FullParams verifies all Batch params combined via backend API.
func TestAudit2_Batch_FullParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		jobName string
	}{
		{name: "batch_full_a", jobName: "job-alpha"},
		{name: "batch_full_b", jobName: "job-beta"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: b2SQSSource,
				Target: "arn:aws:batch:us-east-1:123456789012:job-queue/q",
				TargetParameters: &pipes.TargetParameters{
					BatchJobParameters: &pipes.BatchJobTargetParameters{
						JobDefinition:   "arn:aws:batch:us-east-1:123456789012:job-definition/jd:1",
						JobName:         tt.jobName,
						ArrayProperties: &pipes.BatchArrayProperties{Size: 5},
						RetryStrategy:   &pipes.BatchRetryStrategy{Attempts: 3},
						Parameters:      map[string]string{"k1": "v1", "k2": "v2"},
						DependsOn: []pipes.BatchJobDependency{
							{JobID: "job-parent", Type: "SEQUENTIAL"},
						},
						ContainerOverrides: &pipes.BatchContainerOverrides{
							Command:      []string{"bash", "-c", "echo hi"},
							InstanceType: "m5.large",
							Environment:  map[string]string{"ENV": "test"},
						},
					},
				},
			})
			require.NoError(t, err)

			p, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)

			batch := p.TargetParameters.BatchJobParameters
			assert.Equal(t, tt.jobName, batch.JobName)
			assert.Equal(t, 5, batch.ArrayProperties.Size)
			assert.Equal(t, 3, batch.RetryStrategy.Attempts)
			assert.Equal(t, "v1", batch.Parameters["k1"])
			require.Len(t, batch.DependsOn, 1)
			assert.Equal(t, "SEQUENTIAL", batch.DependsOn[0].Type)
			assert.Equal(t, "job-parent", batch.DependsOn[0].JobID)
			require.NotNil(t, batch.ContainerOverrides)
			assert.Equal(t, "m5.large", batch.ContainerOverrides.InstanceType)
			assert.Equal(t, []string{"bash", "-c", "echo hi"}, batch.ContainerOverrides.Command)
		})
	}
}

// --- StepFunctions invocation type tests ---

// TestAudit2_StepFunctions_InvocationTypes verifies FIRE_AND_FORGET and REQUEST_RESPONSE.
func TestAudit2_StepFunctions_InvocationTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		invocationType string
	}{
		{name: "fire_and_forget", invocationType: "FIRE_AND_FORGET"},
		{name: "request_response", invocationType: "REQUEST_RESPONSE"},
		{name: "empty_defaults_preserved", invocationType: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			sfnParams := map[string]any{}
			if tt.invocationType != "" {
				sfnParams["InvocationType"] = tt.invocationType
			}
			body := map[string]any{
				"Source": b2SQSSource,
				"Target": b2SFNTarget,
				"TargetParameters": map[string]any{
					"StepFunctionStateMachineParameters": sfnParams,
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			sfn, ok := tp["StepFunctionStateMachineParameters"].(map[string]any)
			require.True(t, ok, "StepFunctionStateMachineParameters missing")

			if tt.invocationType != "" {
				assert.Equal(t, tt.invocationType, sfn["InvocationType"])
			}
		})
	}
}

// --- EventBridge EventBus target tests ---

// TestAudit2_EventBridge_Params verifies EventBridge event bus parameters.
func TestAudit2_EventBridge_Params(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		detailType string
		source     string
		endpointID string
		resources  []any
	}{
		{
			name:       "basic_event",
			detailType: "my.event.type",
			source:     "my.source",
		},
		{
			name:       "with_endpoint",
			detailType: "type",
			source:     "src",
			endpointID: "endpoint-id-abc",
		},
		{
			name:       "with_resources",
			detailType: "type",
			source:     "src",
			resources:  []any{"arn:aws:s3:::my-bucket", "arn:aws:s3:::my-bucket-2"},
		},
		{
			name:       "full_event_bus",
			detailType: "order.placed",
			source:     "com.myapp.orders",
			endpointID: "ep-xyz",
			resources:  []any{"arn:aws:s3:::bucket"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			ebParams := map[string]any{
				"DetailType": tt.detailType,
				"Source":     tt.source,
			}
			if tt.endpointID != "" {
				ebParams["EndpointId"] = tt.endpointID
			}
			if tt.resources != nil {
				ebParams["Resources"] = tt.resources
			}

			body := map[string]any{
				"Source": b2SQSSource,
				"Target": "arn:aws:events:us-east-1:123456789012:event-bus/mybus",
				"TargetParameters": map[string]any{
					"EventBridgeEventBusParameters": ebParams,
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			eb, ok := tp["EventBridgeEventBusParameters"].(map[string]any)
			require.True(t, ok, "EventBridgeEventBusParameters missing")

			assert.Equal(t, tt.detailType, eb["DetailType"])
			assert.Equal(t, tt.source, eb["Source"])
			if tt.endpointID != "" {
				assert.Equal(t, tt.endpointID, eb["EndpointId"])
			}
			if tt.resources != nil {
				res, resOK := eb["Resources"].([]any)
				require.True(t, resOK)
				assert.Len(t, res, len(tt.resources))
			}
		})
	}
}

// --- Redshift target tests ---

// TestAudit2_Redshift_Params verifies all Redshift Data API parameters.
func TestAudit2_Redshift_Params(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		database         string
		dbUser           string
		secretManagerArn string
		statementName    string
		sqls             []any
		withEvent        bool
	}{
		{
			name:     "basic_query",
			database: "mydb",
			dbUser:   "admin",
			sqls:     []any{"SELECT 1"},
		},
		{
			name:             "with_secret",
			database:         "mydb",
			secretManagerArn: "arn:aws:secretsmanager:us-east-1:123456789012:secret:mysecret",
			sqls:             []any{"INSERT INTO table1 VALUES (1)"},
		},
		{
			name:          "named_statement",
			database:      "mydb",
			dbUser:        "user1",
			statementName: "my-statement",
			sqls:          []any{"SELECT count(*) FROM orders"},
		},
		{
			name:      "with_event_multiple_sqls",
			database:  "prod",
			dbUser:    "etl",
			sqls:      []any{"TRUNCATE staging", "INSERT INTO prod SELECT * FROM staging"},
			withEvent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			rdParams := map[string]any{
				"Database": tt.database,
				"Sqls":     tt.sqls,
			}
			if tt.dbUser != "" {
				rdParams["DbUser"] = tt.dbUser
			}
			if tt.secretManagerArn != "" {
				rdParams["SecretManagerArn"] = tt.secretManagerArn
			}
			if tt.statementName != "" {
				rdParams["StatementName"] = tt.statementName
			}
			if tt.withEvent {
				rdParams["WithEvent"] = true
			}

			body := map[string]any{
				"Source": b2SQSSource,
				"Target": "arn:aws:redshift:us-east-1:123456789012:cluster:mycluster",
				"TargetParameters": map[string]any{
					"RedshiftDataParameters": rdParams,
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			rd, ok := tp["RedshiftDataParameters"].(map[string]any)
			require.True(t, ok, "RedshiftDataParameters missing")

			assert.Equal(t, tt.database, rd["Database"])
			sqls, ok := rd["Sqls"].([]any)
			require.True(t, ok)
			assert.Len(t, sqls, len(tt.sqls))

			if tt.dbUser != "" {
				assert.Equal(t, tt.dbUser, rd["DbUser"])
			}
			if tt.secretManagerArn != "" {
				assert.Equal(t, tt.secretManagerArn, rd["SecretManagerArn"])
			}
			if tt.statementName != "" {
				assert.Equal(t, tt.statementName, rd["StatementName"])
			}
			if tt.withEvent {
				assert.Equal(t, true, rd["WithEvent"])
			}
		})
	}
}

// --- SageMaker target tests ---

// TestAudit2_SageMaker_Params verifies SageMaker PipelineParameterList depth.
func TestAudit2_SageMaker_Params(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantFirst string
		paramList []map[string]any
		wantLen   int
	}{
		{
			name:      "single_param",
			paramList: []map[string]any{{"Name": "LearningRate", "Value": "0.01"}},
			wantLen:   1,
			wantFirst: "LearningRate",
		},
		{
			name: "multi_params",
			paramList: []map[string]any{
				{"Name": "Epochs", "Value": "100"},
				{"Name": "BatchSize", "Value": "32"},
				{"Name": "LearningRate", "Value": "0.001"},
			},
			wantLen:   3,
			wantFirst: "Epochs",
		},
		{
			name: "special_value_types",
			paramList: []map[string]any{
				{"Name": "S3Uri", "Value": "s3://bucket/prefix/"},
				{"Name": "ModelArn", "Value": "arn:aws:sagemaker:us-east-1:123456789012:model/my-model"},
			},
			wantLen:   2,
			wantFirst: "S3Uri",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": b2SQSSource,
				"Target": "arn:aws:sagemaker:us-east-1:123456789012:pipeline/my-pipeline",
				"TargetParameters": map[string]any{
					"SageMakerPipelineParameters": map[string]any{
						"PipelineParameterList": tt.paramList,
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			tp := resp["TargetParameters"].(map[string]any)
			sm, ok := tp["SageMakerPipelineParameters"].(map[string]any)
			require.True(t, ok, "SageMakerPipelineParameters missing")
			pl, ok := sm["PipelineParameterList"].([]any)
			require.True(t, ok)
			assert.Len(t, pl, tt.wantLen)

			first := pl[0].(map[string]any)
			assert.Equal(t, tt.wantFirst, first["Name"])
		})
	}
}

// --- Self-managed Kafka source tests ---

// TestAudit2_SelfManagedKafka_Credentials verifies all 4 credential types.
func TestAudit2_SelfManagedKafka_Credentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		credKey   string
		credValue string
	}{
		{
			name:      "basic_auth",
			credKey:   "BasicAuth",
			credValue: "arn:aws:secretsmanager:us-east-1:123456789012:secret:basic",
		},
		{
			name:      "client_cert_tls",
			credKey:   "ClientCertificateTlsAuth",
			credValue: "arn:aws:secretsmanager:us-east-1:123456789012:secret:tls",
		},
		{
			name:      "sasl_scram_256",
			credKey:   "SaslScram256Auth",
			credValue: "arn:aws:secretsmanager:us-east-1:123456789012:secret:scram256",
		},
		{
			name:      "sasl_scram_512",
			credKey:   "SaslScram512Auth",
			credValue: "arn:aws:secretsmanager:us-east-1:123456789012:secret:scram512",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": "arn:aws:kafka:us-east-1:123456789012:cluster/mycluster/topic",
				"Target": b2LambdaTarget,
				"SourceParameters": map[string]any{
					"SelfManagedKafkaParameters": map[string]any{
						"TopicName": "my-topic",
						"Credentials": map[string]any{
							tt.credKey: tt.credValue,
						},
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			sp := resp["SourceParameters"].(map[string]any)
			smk := sp["SelfManagedKafkaParameters"].(map[string]any)
			creds, ok := smk["Credentials"].(map[string]any)
			require.True(t, ok, "Credentials should be object")
			assert.Equal(t, tt.credValue, creds[tt.credKey])
		})
	}
}

// TestAudit2_SelfManagedKafka_Vpc verifies VPC config for self-managed Kafka.
func TestAudit2_SelfManagedKafka_Vpc(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		subnets       []any
		securityGroup []any
	}{
		{
			name:          "single_subnet",
			subnets:       []any{"subnet-aaa"},
			securityGroup: []any{"sg-111"},
		},
		{
			name:          "multi_subnet",
			subnets:       []any{"subnet-aaa", "subnet-bbb", "subnet-ccc"},
			securityGroup: []any{"sg-111", "sg-222"},
		},
		{
			name:          "no_security_group",
			subnets:       []any{"subnet-ddd"},
			securityGroup: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			vpcConf := map[string]any{"Subnets": tt.subnets}
			if tt.securityGroup != nil {
				vpcConf["SecurityGroup"] = tt.securityGroup
			}
			body := map[string]any{
				"Source": "arn:aws:kafka:us-east-1:123456789012:cluster/mycluster/topic",
				"Target": b2LambdaTarget,
				"SourceParameters": map[string]any{
					"SelfManagedKafkaParameters": map[string]any{
						"TopicName": "my-topic",
						"Vpc":       vpcConf,
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			sp := resp["SourceParameters"].(map[string]any)
			smk := sp["SelfManagedKafkaParameters"].(map[string]any)
			vpc, ok := smk["Vpc"].(map[string]any)
			require.True(t, ok, "Vpc should be object")
			subnets, ok := vpc["Subnets"].([]any)
			require.True(t, ok)
			assert.Len(t, subnets, len(tt.subnets))
		})
	}
}

// TestAudit2_SelfManagedKafka_StartingPosition verifies StartingPosition variants.
func TestAudit2_SelfManagedKafka_StartingPosition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		startingPosition string
	}{
		{name: "trim_horizon", startingPosition: "TRIM_HORIZON"},
		{name: "latest", startingPosition: "LATEST"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: "arn:aws:kafka:us-east-1:123456789012:cluster/c/t",
				Target: b2LambdaTarget,
				SourceParameters: &pipes.SourceParameters{
					SelfManagedKafkaParameters: &pipes.SelfManagedKafkaSourceParameters{
						TopicName:        "my-topic",
						StartingPosition: tt.startingPosition,
						ConsumerGroupID:  "my-group",
					},
				},
			})
			require.NoError(t, err)

			p, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t, tt.startingPosition,
				p.SourceParameters.SelfManagedKafkaParameters.StartingPosition)
			assert.Equal(t, "my-group",
				p.SourceParameters.SelfManagedKafkaParameters.ConsumerGroupID)
		})
	}
}

// TestAudit2_SelfManagedKafka_BootstrapServers verifies AdditionalBootstrapServers.
func TestAudit2_SelfManagedKafka_BootstrapServers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		servers []any
		wantLen int
	}{
		{
			name:    "single_server",
			servers: []any{"kafka.example.com:9092"},
			wantLen: 1,
		},
		{
			name:    "multiple_servers",
			servers: []any{"kafka1.example.com:9092", "kafka2.example.com:9092", "kafka3.example.com:9092"},
			wantLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": "arn:aws:kafka:us-east-1:123456789012:cluster/c/t",
				"Target": b2LambdaTarget,
				"SourceParameters": map[string]any{
					"SelfManagedKafkaParameters": map[string]any{
						"TopicName":                  "my-topic",
						"AdditionalBootstrapServers": tt.servers,
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			sp := resp["SourceParameters"].(map[string]any)
			smk := sp["SelfManagedKafkaParameters"].(map[string]any)
			servers, ok := smk["AdditionalBootstrapServers"].([]any)
			require.True(t, ok)
			assert.Len(t, servers, tt.wantLen)
			assert.Equal(t, tt.servers[0], servers[0])
		})
	}
}

// --- MSK source tests ---

// TestAudit2_MSK_Credentials verifies both MSK credential types.
func TestAudit2_MSK_Credentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		credKey   string
		credValue string
	}{
		{
			name:      "client_cert_tls",
			credKey:   "ClientCertificateTlsAuth",
			credValue: "arn:aws:secretsmanager:us-east-1:123456789012:secret:tls",
		},
		{
			name:      "sasl_scram_512",
			credKey:   "SaslScram512Auth",
			credValue: "arn:aws:secretsmanager:us-east-1:123456789012:secret:scram",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": "arn:aws:kafka:us-east-1:123456789012:cluster/msk",
				"Target": b2LambdaTarget,
				"SourceParameters": map[string]any{
					"ManagedStreamingKafkaParameters": map[string]any{
						"TopicName": "my-topic",
						"Credentials": map[string]any{
							tt.credKey: tt.credValue,
						},
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			sp := resp["SourceParameters"].(map[string]any)
			msk := sp["ManagedStreamingKafkaParameters"].(map[string]any)
			creds, ok := msk["Credentials"].(map[string]any)
			require.True(t, ok, "Credentials should be object")
			assert.Equal(t, tt.credValue, creds[tt.credKey])
		})
	}
}

// TestAudit2_MSK_StartingPositionAndConsumerGroup verifies MSK source fields.
func TestAudit2_MSK_StartingPositionAndConsumerGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		startingPosition string
		consumerGroupID  string
	}{
		{
			name:             "trim_horizon_with_group",
			startingPosition: "TRIM_HORIZON",
			consumerGroupID:  "my-consumer-group",
		},
		{
			name:             "latest_with_group",
			startingPosition: "LATEST",
			consumerGroupID:  "another-group",
		},
		{
			name:            "no_position_with_group",
			consumerGroupID: "group-only",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: "arn:aws:kafka:us-east-1:123456789012:cluster/msk",
				Target: b2LambdaTarget,
				SourceParameters: &pipes.SourceParameters{
					ManagedStreamingKafkaParameters: &pipes.MSKSourceParameters{
						TopicName:        "my-topic",
						StartingPosition: tt.startingPosition,
						ConsumerGroupID:  tt.consumerGroupID,
					},
				},
			})
			require.NoError(t, err)

			p, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)

			msk := p.SourceParameters.ManagedStreamingKafkaParameters
			assert.Equal(t, tt.startingPosition, msk.StartingPosition)
			assert.Equal(t, tt.consumerGroupID, msk.ConsumerGroupID)
		})
	}
}

// --- ActiveMQ source tests ---

// TestAudit2_ActiveMQ_Credentials verifies ActiveMQ BasicAuth credential.
func TestAudit2_ActiveMQ_Credentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		basicAuth string
		queueName string
	}{
		{
			name:      "basic_auth_cred",
			basicAuth: "arn:aws:secretsmanager:us-east-1:123456789012:secret:amq-creds",
			queueName: "my.queue",
		},
		{
			name:      "different_queue_and_cred",
			basicAuth: "arn:aws:secretsmanager:us-east-1:123456789012:secret:other-creds",
			queueName: "other.queue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": "arn:aws:mq:us-east-1:123456789012:broker:mybroker:b-abc",
				"Target": b2LambdaTarget,
				"SourceParameters": map[string]any{
					"ActiveMQBrokerParameters": map[string]any{
						"QueueName": tt.queueName,
						"Credentials": map[string]any{
							"BasicAuth": tt.basicAuth,
						},
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			sp := resp["SourceParameters"].(map[string]any)
			amq := sp["ActiveMQBrokerParameters"].(map[string]any)
			creds, ok := amq["Credentials"].(map[string]any)
			require.True(t, ok, "Credentials should be object")
			assert.Equal(t, tt.basicAuth, creds["BasicAuth"])
			assert.Equal(t, tt.queueName, amq["QueueName"])
		})
	}
}

// TestAudit2_ActiveMQ_BatchingParams verifies ActiveMQ BatchSize and batching window.
func TestAudit2_ActiveMQ_BatchingParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		batchSize       float64
		batchWindowSecs float64
	}{
		{name: "batch_10", batchSize: 10, batchWindowSecs: 0},
		{name: "batch_100_window_5", batchSize: 100, batchWindowSecs: 5},
		{name: "batch_1_window_30", batchSize: 1, batchWindowSecs: 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			params := map[string]any{
				"QueueName": "test.queue",
				"Credentials": map[string]any{
					"BasicAuth": "arn:aws:secretsmanager:us-east-1:123456789012:secret:cred",
				},
				"BatchSize": tt.batchSize,
			}
			if tt.batchWindowSecs > 0 {
				params["MaximumBatchingWindowInSeconds"] = tt.batchWindowSecs
			}
			body := map[string]any{
				"Source": "arn:aws:mq:us-east-1:123456789012:broker:mybroker:b-abc",
				"Target": b2LambdaTarget,
				"SourceParameters": map[string]any{
					"ActiveMQBrokerParameters": params,
				},
			}
			resp := b2Create(t, h, tt.name, body)

			sp := resp["SourceParameters"].(map[string]any)
			amq := sp["ActiveMQBrokerParameters"].(map[string]any)
			assert.InEpsilon(t, tt.batchSize, amq["BatchSize"], 0.01)
		})
	}
}

// --- RabbitMQ source tests ---

// TestAudit2_RabbitMQ_Credentials verifies RabbitMQ BasicAuth credential.
func TestAudit2_RabbitMQ_Credentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		basicAuth   string
		queueName   string
		virtualHost string
	}{
		{
			name:        "default_vhost",
			basicAuth:   "arn:aws:secretsmanager:us-east-1:123456789012:secret:rmq-cred",
			queueName:   "my.queue",
			virtualHost: "/",
		},
		{
			name:        "custom_vhost",
			basicAuth:   "arn:aws:secretsmanager:us-east-1:123456789012:secret:rmq-cred-2",
			queueName:   "other.queue",
			virtualHost: "my-vhost",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": "arn:aws:mq:us-east-1:123456789012:broker:rmqbroker:b-xyz",
				"Target": b2LambdaTarget,
				"SourceParameters": map[string]any{
					"RabbitMQBrokerParameters": map[string]any{
						"QueueName":   tt.queueName,
						"VirtualHost": tt.virtualHost,
						"Credentials": map[string]any{
							"BasicAuth": tt.basicAuth,
						},
					},
				},
			}
			resp := b2Create(t, h, tt.name, body)

			sp := resp["SourceParameters"].(map[string]any)
			rmq := sp["RabbitMQBrokerParameters"].(map[string]any)
			creds, ok := rmq["Credentials"].(map[string]any)
			require.True(t, ok, "Credentials should be object")
			assert.Equal(t, tt.basicAuth, creds["BasicAuth"])
			assert.Equal(t, tt.virtualHost, rmq["VirtualHost"])
		})
	}
}

// TestAudit2_RabbitMQ_VirtualHost verifies VirtualHost variants.
func TestAudit2_RabbitMQ_VirtualHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		virtualHost string
	}{
		{name: "root_vhost", virtualHost: "/"},
		{name: "named_vhost", virtualHost: "production"},
		{name: "nested_vhost", virtualHost: "app/production"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: "arn:aws:mq:us-east-1:123456789012:broker:rmq:b-abc",
				Target: b2LambdaTarget,
				SourceParameters: &pipes.SourceParameters{
					RabbitMQBrokerParameters: &pipes.RabbitMQBrokerSourceParameters{
						QueueName:   "my.queue",
						VirtualHost: tt.virtualHost,
						Credentials: &pipes.MQBrokerCredentials{
							BasicAuth: "arn:aws:secretsmanager:us-east-1:123456789012:secret:cred",
						},
					},
				},
			})
			require.NoError(t, err)

			p, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t, tt.virtualHost, p.SourceParameters.RabbitMQBrokerParameters.VirtualHost)
		})
	}
}

// --- FilterCriteria tests ---

// TestAudit2_FilterCriteria_MultiplePatterns verifies multiple filter patterns behavior.
func TestAudit2_FilterCriteria_MultiplePatterns(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		msgBody   string
		patterns  []string
		wantMatch bool
	}{
		{
			name:      "first_pattern_matches",
			patterns:  []string{"ORDER_PLACED", "ORDER_CANCELLED"},
			msgBody:   `{"eventType": "ORDER_PLACED", "orderId": "123"}`,
			wantMatch: true,
		},
		{
			name:      "second_pattern_matches",
			patterns:  []string{"ORDER_PLACED", "ORDER_CANCELLED"},
			msgBody:   `{"eventType": "ORDER_CANCELLED", "orderId": "456"}`,
			wantMatch: true,
		},
		{
			name:      "no_pattern_matches",
			patterns:  []string{"ORDER_PLACED", "ORDER_CANCELLED"},
			msgBody:   `{"eventType": "ORDER_SHIPPED", "orderId": "789"}`,
			wantMatch: false,
		},
		{
			name:      "empty_pattern_matches_all",
			patterns:  []string{""},
			msgBody:   `{"anything": true}`,
			wantMatch: true,
		},
		{
			name:      "three_patterns_last_matches",
			patterns:  []string{"alpha", "beta", "gamma"},
			msgBody:   `{"type": "gamma"}`,
			wantMatch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			filters := make([]pipes.Filter, len(tt.patterns))
			for i, p := range tt.patterns {
				filters[i] = pipes.Filter{Pattern: p}
			}

			b := b2Backend()
			pipeName := tt.name + "-pipe"
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   pipeName,
				Source: b2SQSSource,
				Target: b2LambdaTarget,
				SourceParameters: &pipes.SourceParameters{
					FilterCriteria:     &pipes.FilterCriteria{Filters: filters},
					SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 10},
				},
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, pipeName)

			msgs := []*pipes.SQSMessage{{
				MessageID:     "msg-1",
				ReceiptHandle: "rh-1",
				Body:          tt.msgBody,
			}}

			var mockSQS mockSQSFilter
			mockSQS.msgs = msgs
			r := pipes.NewRunner(b)
			r.SetSQSReader(&mockSQS)

			invoker := &mockLambdaFilter{}
			r.SetLambdaInvoker(invoker)

			pipes.PollAllPipesOnce(context.Background(), r)

			if tt.wantMatch {
				assert.True(t, invoker.called, "expected Lambda to be invoked for matching msg")
			} else {
				assert.False(t, invoker.called, "expected Lambda NOT invoked for non-matching msg")
			}
		})
	}
}

type mockSQSFilter struct {
	msgs []*pipes.SQSMessage
}

func (m *mockSQSFilter) ReceivePipeMessages(_ string, _ int) ([]*pipes.SQSMessage, error) {
	return m.msgs, nil
}

func (m *mockSQSFilter) DeletePipeMessages(_ string, _ []string) error {
	return nil
}

type mockLambdaFilter struct {
	payload []byte
	called  bool
}

func (m *mockLambdaFilter) InvokeFunction(
	_ context.Context,
	_ string,
	_ string,
	payload []byte,
) ([]byte, int, error) {
	m.called = true
	m.payload = payload

	return nil, 200, nil
}

// --- RuntimeMetricsStreaming tests ---

// TestAudit2_RuntimeMetricsStreaming_Create verifies metrics streaming config persists.
func TestAudit2_RuntimeMetricsStreaming_Create(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		level     string
		namespace string
	}{
		{
			name:      "all_level_with_ns",
			level:     "ALL",
			namespace: "MyApp/Pipes",
		},
		{
			name:      "errors_level",
			level:     "ERRORS",
			namespace: "MyApp/PipeErrors",
		},
		{
			name:  "no_namespace",
			level: "ALL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			rmsBody := map[string]any{"Level": tt.level}
			if tt.namespace != "" {
				rmsBody["MetricsDestination"] = map[string]any{
					"CloudwatchMetrics": map[string]any{
						"Namespace": tt.namespace,
					},
				}
			}

			body := map[string]any{
				"Source":                  b2SQSSource,
				"Target":                  b2LambdaTarget,
				"RuntimeMetricsStreaming": rmsBody,
			}
			resp := b2Create(t, h, tt.name, body)

			rms, ok := resp["RuntimeMetricsStreaming"].(map[string]any)
			require.True(t, ok, "RuntimeMetricsStreaming missing")
			assert.Equal(t, tt.level, rms["Level"])

			if tt.namespace != "" {
				ns := nestedString(t, rms, "MetricsDestination", "CloudwatchMetrics", "Namespace")
				assert.Equal(t, tt.namespace, ns)
			}
		})
	}
}

// TestAudit2_RuntimeMetricsStreaming_Update verifies metrics streaming updates.
func TestAudit2_RuntimeMetricsStreaming_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		initialLevel string
		updatedLevel string
	}{
		{
			name:         "upgrade_to_all",
			initialLevel: "ERRORS",
			updatedLevel: "ALL",
		},
		{
			name:         "downgrade_to_errors",
			initialLevel: "ALL",
			updatedLevel: "ERRORS",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			b2Create(t, h, tt.name, map[string]any{
				"Source": b2SQSSource,
				"Target": b2LambdaTarget,
				"RuntimeMetricsStreaming": map[string]any{
					"Level": tt.initialLevel,
				},
			})

			resp := b2Update(t, h, tt.name, map[string]any{
				"RuntimeMetricsStreaming": map[string]any{
					"Level": tt.updatedLevel,
				},
			})

			rms, ok := resp["RuntimeMetricsStreaming"].(map[string]any)
			require.True(t, ok, "RuntimeMetricsStreaming missing after update")
			assert.Equal(t, tt.updatedLevel, rms["Level"])
		})
	}
}

// --- Immutability / clone isolation tests ---

// TestAudit2_Clone_ECSNetworkIsolation verifies ECS network config clone isolates slices.
func TestAudit2_Clone_ECSNetworkIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "ecs_clone_subnets"},
		{name: "ecs_clone_secgroups"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: b2SQSSource,
				Target: b2ECSTarget,
				TargetParameters: &pipes.TargetParameters{
					EcsTaskParameters: &pipes.ECSTaskTargetParameters{
						TaskDefinitionArn: "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
						NetworkConfiguration: &pipes.NetworkConfiguration{
							AwsvpcConfiguration: &pipes.AwsVpcConfiguration{
								Subnets:        []string{"subnet-aaa"},
								SecurityGroups: []string{"sg-111"},
							},
						},
						CapacityProviderStrategy: []pipes.CapacityProviderStrategyItem{
							{CapacityProvider: "FARGATE", Weight: 1},
						},
						PlacementConstraints: []pipes.PlacementConstraint{
							{Type: "distinctInstance"},
						},
						PlacementStrategy: []pipes.PlacementStrategy{
							{Type: "spread", Field: "az"},
						},
					},
				},
			})
			require.NoError(t, err)

			p1, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)

			p1.TargetParameters.EcsTaskParameters.NetworkConfiguration.
				AwsvpcConfiguration.Subnets[0] = "mutated"
			p1.TargetParameters.EcsTaskParameters.CapacityProviderStrategy[0].CapacityProvider = "mutated"
			p1.TargetParameters.EcsTaskParameters.PlacementConstraints[0].Type = "mutated"
			p1.TargetParameters.EcsTaskParameters.PlacementStrategy[0].Type = "mutated"

			p2, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t, "subnet-aaa",
				p2.TargetParameters.EcsTaskParameters.NetworkConfiguration.AwsvpcConfiguration.Subnets[0])
			assert.Equal(t, "FARGATE",
				p2.TargetParameters.EcsTaskParameters.CapacityProviderStrategy[0].CapacityProvider)
			assert.Equal(t, "distinctInstance",
				p2.TargetParameters.EcsTaskParameters.PlacementConstraints[0].Type)
			assert.Equal(t, "spread",
				p2.TargetParameters.EcsTaskParameters.PlacementStrategy[0].Type)
		})
	}
}

// TestAudit2_Clone_BatchDependsOnIsolation verifies Batch DependsOn clone isolates slices.
func TestAudit2_Clone_BatchDependsOnIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "batch_depends_clone_a"},
		{name: "batch_depends_clone_b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: b2SQSSource,
				Target: "arn:aws:batch:us-east-1:123456789012:job-queue/q",
				TargetParameters: &pipes.TargetParameters{
					BatchJobParameters: &pipes.BatchJobTargetParameters{
						JobDefinition: "jd",
						JobName:       "job",
						DependsOn: []pipes.BatchJobDependency{
							{JobID: "original-job", Type: "SEQUENTIAL"},
						},
						ContainerOverrides: &pipes.BatchContainerOverrides{
							Command:     []string{"echo", "hi"},
							Environment: map[string]string{"K": "V"},
						},
					},
				},
			})
			require.NoError(t, err)

			p1, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)

			p1.TargetParameters.BatchJobParameters.DependsOn[0].JobID = "mutated"
			p1.TargetParameters.BatchJobParameters.ContainerOverrides.Command[0] = "mutated"
			p1.TargetParameters.BatchJobParameters.ContainerOverrides.Environment["K"] = "mutated"

			p2, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t, "original-job",
				p2.TargetParameters.BatchJobParameters.DependsOn[0].JobID)
			assert.Equal(t, "echo",
				p2.TargetParameters.BatchJobParameters.ContainerOverrides.Command[0])
			assert.Equal(t, "V",
				p2.TargetParameters.BatchJobParameters.ContainerOverrides.Environment["K"])
		})
	}
}

// TestAudit2_Clone_SelfManagedKafkaVpcIsolation verifies Kafka VPC clone isolates slices.
func TestAudit2_Clone_SelfManagedKafkaVpcIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "kafka_vpc_clone_a"},
		{name: "kafka_vpc_clone_b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: "arn:aws:kafka:us-east-1:123456789012:cluster/c/t",
				Target: b2LambdaTarget,
				SourceParameters: &pipes.SourceParameters{
					SelfManagedKafkaParameters: &pipes.SelfManagedKafkaSourceParameters{
						TopicName: "topic",
						Vpc: &pipes.SelfManagedKafkaVpc{
							SecurityGroup: []string{"sg-original"},
							Subnets:       []string{"subnet-original"},
						},
						Credentials: &pipes.SelfManagedKafkaAccessCredentials{
							BasicAuth: "arn:aws:secretsmanager:us-east-1:123456789012:secret:cred",
						},
					},
				},
			})
			require.NoError(t, err)

			p1, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)

			p1.SourceParameters.SelfManagedKafkaParameters.Vpc.SecurityGroup[0] = "mutated"
			p1.SourceParameters.SelfManagedKafkaParameters.Vpc.Subnets[0] = "mutated"
			p1.SourceParameters.SelfManagedKafkaParameters.Credentials.BasicAuth = "mutated"

			p2, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t, "sg-original",
				p2.SourceParameters.SelfManagedKafkaParameters.Vpc.SecurityGroup[0])
			assert.Equal(t, "subnet-original",
				p2.SourceParameters.SelfManagedKafkaParameters.Vpc.Subnets[0])
			assert.Equal(t, "arn:aws:secretsmanager:us-east-1:123456789012:secret:cred",
				p2.SourceParameters.SelfManagedKafkaParameters.Credentials.BasicAuth)
		})
	}
}

// TestAudit2_Clone_MSKCredentialsIsolation verifies MSK credentials clone isolates.
func TestAudit2_Clone_MSKCredentialsIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "msk_cred_clone_a"},
		{name: "msk_cred_clone_b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: "arn:aws:kafka:us-east-1:123456789012:cluster/msk",
				Target: b2LambdaTarget,
				SourceParameters: &pipes.SourceParameters{
					ManagedStreamingKafkaParameters: &pipes.MSKSourceParameters{
						TopicName: "topic",
						Credentials: &pipes.MSKAccessCredentials{
							SaslScram512Auth: "arn:aws:secretsmanager:us-east-1:123456789012:secret:orig",
						},
					},
				},
			})
			require.NoError(t, err)

			p1, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			p1.SourceParameters.ManagedStreamingKafkaParameters.Credentials.SaslScram512Auth = "mutated"

			p2, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t,
				"arn:aws:secretsmanager:us-east-1:123456789012:secret:orig",
				p2.SourceParameters.ManagedStreamingKafkaParameters.Credentials.SaslScram512Auth)
		})
	}
}

// TestAudit2_Clone_ActiveMQCredentialsIsolation verifies ActiveMQ clone isolates credentials.
func TestAudit2_Clone_ActiveMQCredentialsIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "amq_clone_a"},
		{name: "amq_clone_b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: "arn:aws:mq:us-east-1:123456789012:broker:b:b-abc",
				Target: b2LambdaTarget,
				SourceParameters: &pipes.SourceParameters{
					ActiveMQBrokerParameters: &pipes.ActiveMQBrokerSourceParameters{
						QueueName: "q",
						Credentials: &pipes.MQBrokerCredentials{
							BasicAuth: "arn:aws:secretsmanager:us-east-1:123456789012:secret:amq-orig",
						},
					},
				},
			})
			require.NoError(t, err)

			p1, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			p1.SourceParameters.ActiveMQBrokerParameters.Credentials.BasicAuth = "mutated"

			p2, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t,
				"arn:aws:secretsmanager:us-east-1:123456789012:secret:amq-orig",
				p2.SourceParameters.ActiveMQBrokerParameters.Credentials.BasicAuth)
		})
	}
}

// TestAudit2_Clone_RabbitMQCredentialsIsolation verifies RabbitMQ clone isolates credentials.
func TestAudit2_Clone_RabbitMQCredentialsIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "rmq_clone_a"},
		{name: "rmq_clone_b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: "arn:aws:mq:us-east-1:123456789012:broker:rmq:b-xyz",
				Target: b2LambdaTarget,
				SourceParameters: &pipes.SourceParameters{
					RabbitMQBrokerParameters: &pipes.RabbitMQBrokerSourceParameters{
						QueueName: "q",
						Credentials: &pipes.MQBrokerCredentials{
							BasicAuth: "arn:aws:secretsmanager:us-east-1:123456789012:secret:rmq-orig",
						},
					},
				},
			})
			require.NoError(t, err)

			p1, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			p1.SourceParameters.RabbitMQBrokerParameters.Credentials.BasicAuth = "mutated"

			p2, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t,
				"arn:aws:secretsmanager:us-east-1:123456789012:secret:rmq-orig",
				p2.SourceParameters.RabbitMQBrokerParameters.Credentials.BasicAuth)
		})
	}
}

// --- Update path tests for new fields ---

// TestAudit2_Update_ECSParams verifies UpdatePipe handles new ECS fields.
func TestAudit2_Update_ECSParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		initialGroup  string
		updatedGroup  string
		updatedLaunch string
	}{
		{
			name:          "update_ecs_group",
			initialGroup:  "service:initial",
			updatedGroup:  "service:updated",
			updatedLaunch: "FARGATE",
		},
		{
			name:          "update_ecs_launch_type",
			initialGroup:  "service:svc",
			updatedGroup:  "service:svc",
			updatedLaunch: "EC2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			b2Create(t, h, tt.name, map[string]any{
				"Source": b2SQSSource,
				"Target": b2ECSTarget,
				"TargetParameters": map[string]any{
					"EcsTaskParameters": map[string]any{
						"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
						"Group":             tt.initialGroup,
					},
				},
			})

			updated := b2Update(t, h, tt.name, map[string]any{
				"TargetParameters": map[string]any{
					"EcsTaskParameters": map[string]any{
						"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/td:2",
						"Group":             tt.updatedGroup,
						"LaunchType":        tt.updatedLaunch,
					},
				},
			})

			tp := updated["TargetParameters"].(map[string]any)
			ecs := tp["EcsTaskParameters"].(map[string]any)
			assert.Equal(t, tt.updatedGroup, ecs["Group"])
			assert.Equal(t, tt.updatedLaunch, ecs["LaunchType"])
		})
	}
}

// TestAudit2_Update_BatchDependsOn verifies UpdatePipe propagates Batch DependsOn.
func TestAudit2_Update_BatchDependsOn(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		initialDeps []map[string]any
		updatedDeps []map[string]any
		wantLen     int
	}{
		{
			name:        "add_dependency",
			initialDeps: nil,
			updatedDeps: []map[string]any{{"JobId": "job-new", "Type": "SEQUENTIAL"}},
			wantLen:     1,
		},
		{
			name:        "change_dependency",
			initialDeps: []map[string]any{{"JobId": "job-old", "Type": "SEQUENTIAL"}},
			updatedDeps: []map[string]any{
				{"JobId": "job-a", "Type": "SEQUENTIAL"},
				{"JobId": "job-b", "Type": "N_TO_N"},
			},
			wantLen: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			initialBatch := map[string]any{
				"JobDefinition": "jd",
				"JobName":       "job",
			}
			if tt.initialDeps != nil {
				initialBatch["DependsOn"] = tt.initialDeps
			}
			b2Create(t, h, tt.name, map[string]any{
				"Source": b2SQSSource,
				"Target": "arn:aws:batch:us-east-1:123456789012:job-queue/q",
				"TargetParameters": map[string]any{
					"BatchJobParameters": initialBatch,
				},
			})

			updated := b2Update(t, h, tt.name, map[string]any{
				"TargetParameters": map[string]any{
					"BatchJobParameters": map[string]any{
						"JobDefinition": "jd",
						"JobName":       "job",
						"DependsOn":     tt.updatedDeps,
					},
				},
			})

			tp := updated["TargetParameters"].(map[string]any)
			batch := tp["BatchJobParameters"].(map[string]any)
			deps, ok := batch["DependsOn"].([]any)
			require.True(t, ok)
			assert.Len(t, deps, tt.wantLen)
		})
	}
}

// TestAudit2_Update_SelfManagedKafkaCredentials verifies credential updates.
func TestAudit2_Update_SelfManagedKafkaCredentials(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialCredKey string
		initialCredVal string
		updatedCredKey string
		updatedCredVal string
	}{
		{
			name:           "basic_to_scram512",
			initialCredKey: "BasicAuth",
			initialCredVal: "arn:aws:secretsmanager:us-east-1:123456789012:secret:basic",
			updatedCredKey: "SaslScram512Auth",
			updatedCredVal: "arn:aws:secretsmanager:us-east-1:123456789012:secret:scram",
		},
		{
			name:           "tls_to_scram256",
			initialCredKey: "ClientCertificateTlsAuth",
			initialCredVal: "arn:aws:secretsmanager:us-east-1:123456789012:secret:tls",
			updatedCredKey: "SaslScram256Auth",
			updatedCredVal: "arn:aws:secretsmanager:us-east-1:123456789012:secret:scram256",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			b2Create(t, h, tt.name, map[string]any{
				"Source": "arn:aws:kafka:us-east-1:123456789012:cluster/c/t",
				"Target": b2LambdaTarget,
				"SourceParameters": map[string]any{
					"SelfManagedKafkaParameters": map[string]any{
						"TopicName": "topic",
						"Credentials": map[string]any{
							tt.initialCredKey: tt.initialCredVal,
						},
					},
				},
			})

			updated := b2Update(t, h, tt.name, map[string]any{
				"SourceParameters": map[string]any{
					"SelfManagedKafkaParameters": map[string]any{
						"TopicName": "topic",
						"Credentials": map[string]any{
							tt.updatedCredKey: tt.updatedCredVal,
						},
					},
				},
			})

			sp := updated["SourceParameters"].(map[string]any)
			smk := sp["SelfManagedKafkaParameters"].(map[string]any)
			creds := smk["Credentials"].(map[string]any)
			assert.Equal(t, tt.updatedCredVal, creds[tt.updatedCredKey])
		})
	}
}

// --- Combined source + target tests ---

// TestAudit2_KafkaSourceLambdaTargetRoundtrip verifies full source+target persist via HTTP.
func TestAudit2_KafkaSourceLambdaTargetRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		startingPos     string
		consumerGroupID string
	}{
		{
			name:            "trim_horizon_group_a",
			startingPos:     "TRIM_HORIZON",
			consumerGroupID: "group-a",
		},
		{
			name:            "latest_group_b",
			startingPos:     "LATEST",
			consumerGroupID: "group-b",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": "arn:aws:kafka:us-east-1:123456789012:cluster/c/t",
				"Target": b2LambdaTarget,
				"SourceParameters": map[string]any{
					"SelfManagedKafkaParameters": map[string]any{
						"TopicName":        "my-topic",
						"StartingPosition": tt.startingPos,
						"ConsumerGroupId":  tt.consumerGroupID,
						"Credentials": map[string]any{
							"SaslScram512Auth": "arn:aws:secretsmanager:us-east-1:123456789012:secret:s",
						},
						"Vpc": map[string]any{
							"Subnets":       []any{"subnet-aaa"},
							"SecurityGroup": []any{"sg-111"},
						},
					},
				},
				"TargetParameters": map[string]any{
					"LambdaFunctionParameters": map[string]any{
						"InvocationType": "REQUEST_RESPONSE",
					},
				},
			}

			resp := b2Create(t, h, tt.name, body)
			got := b2Describe(t, h, tt.name)

			assert.Equal(t, resp["Name"], got["Name"])
			assert.Equal(t, "CREATING", resp["CurrentState"])

			sp := got["SourceParameters"].(map[string]any)
			smk := sp["SelfManagedKafkaParameters"].(map[string]any)
			assert.Equal(t, tt.startingPos, smk["StartingPosition"])
			assert.Equal(t, tt.consumerGroupID, smk["ConsumerGroupId"])
		})
	}
}

// TestAudit2_MSKSourceECSTargetRoundtrip verifies MSK source + ECS target roundtrip.
func TestAudit2_MSKSourceECSTargetRoundtrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topicName string
		credType  string
		credVal   string
	}{
		{
			name:      "msk_scram512_ecs",
			topicName: "orders",
			credType:  "SaslScram512Auth",
			credVal:   "arn:aws:secretsmanager:us-east-1:123456789012:secret:scram",
		},
		{
			name:      "msk_tls_ecs",
			topicName: "events",
			credType:  "ClientCertificateTlsAuth",
			credVal:   "arn:aws:secretsmanager:us-east-1:123456789012:secret:tls",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": "arn:aws:kafka:us-east-1:123456789012:cluster/msk",
				"Target": b2ECSTarget,
				"SourceParameters": map[string]any{
					"ManagedStreamingKafkaParameters": map[string]any{
						"TopicName": tt.topicName,
						"Credentials": map[string]any{
							tt.credType: tt.credVal,
						},
					},
				},
				"TargetParameters": map[string]any{
					"EcsTaskParameters": map[string]any{
						"TaskDefinitionArn": "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
						"LaunchType":        "FARGATE",
						"NetworkConfiguration": map[string]any{
							"AwsvpcConfiguration": map[string]any{
								"Subnets":        []any{"subnet-aaa"},
								"SecurityGroups": []any{"sg-111"},
								"AssignPublicIp": "ENABLED",
							},
						},
					},
				},
			}

			resp := b2Create(t, h, tt.name, body)

			sp := resp["SourceParameters"].(map[string]any)
			msk := sp["ManagedStreamingKafkaParameters"].(map[string]any)
			assert.Equal(t, tt.topicName, msk["TopicName"])
			creds := msk["Credentials"].(map[string]any)
			assert.Equal(t, tt.credVal, creds[tt.credType])

			tp := resp["TargetParameters"].(map[string]any)
			ecs := tp["EcsTaskParameters"].(map[string]any)
			assert.Equal(t, "FARGATE", ecs["LaunchType"])
		})
	}
}

// --- Error path tests ---

// TestAudit2_Error_DuplicatePipe verifies duplicate create returns conflict.
func TestAudit2_Error_DuplicatePipe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "dup-pipe-a"},
		{name: "dup-pipe-b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source": b2SQSSource,
				"Target": b2LambdaTarget,
			}
			b2Create(t, h, tt.name, body)
			rec := b2Do(t, h, http.MethodPost, "/v1/pipes/"+tt.name, body)
			assert.Equal(t, http.StatusConflict, rec.Code)
		})
	}
}

// TestAudit2_Error_NotFound verifies describe returns 404 for unknown pipe.
func TestAudit2_Error_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "does-not-exist-a"},
		{name: "does-not-exist-b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			rec := b2Do(t, h, http.MethodGet, "/v1/pipes/"+tt.name, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

// TestAudit2_Error_InvalidDesiredState verifies validation error for invalid state.
func TestAudit2_Error_InvalidDesiredState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		desiredState string
	}{
		{name: "invalid_state_a", desiredState: "INVALID"},
		{name: "invalid_state_b", desiredState: "STARTING"},
		{name: "invalid_state_c", desiredState: "CREATING"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			body := map[string]any{
				"Source":       b2SQSSource,
				"Target":       b2LambdaTarget,
				"DesiredState": tt.desiredState,
			}
			rec := b2Do(t, h, http.MethodPost, "/v1/pipes/"+tt.name, body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// --- Lifecycle tests with new params ---

// TestAudit2_Lifecycle_StartStop verifies start/stop transitions with ECS params.
func TestAudit2_Lifecycle_StartStop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "start-stop-ecs-a"},
		{name: "start-stop-ecs-b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:         tt.name,
				Source:       b2SQSSource,
				Target:       b2ECSTarget,
				DesiredState: "RUNNING",
				TargetParameters: &pipes.TargetParameters{
					EcsTaskParameters: &pipes.ECSTaskTargetParameters{
						TaskDefinitionArn: "arn:aws:ecs:us-east-1:123456789012:task-definition/td:1",
						LaunchType:        "FARGATE",
						NetworkConfiguration: &pipes.NetworkConfiguration{
							AwsvpcConfiguration: &pipes.AwsVpcConfiguration{
								Subnets: []string{"subnet-aaa"},
							},
						},
					},
				},
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, tt.name)

			stopped, err := b.StopPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t, "STOPPING", stopped.CurrentState)

			require.Eventually(t, func() bool {
				p, e := b.GetPipe(context.Background(), tt.name)

				return e == nil && p.CurrentState == "STOPPED"
			}, 500e6, 5e6)

			started, err := b.StartPipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t, "STARTING", started.CurrentState)

			require.Eventually(t, func() bool {
				p, e := b.GetPipe(context.Background(), tt.name)

				return e == nil && p.CurrentState == "RUNNING"
			}, 500e6, 5e6)

			p, err := b.GetPipe(context.Background(), tt.name)
			require.NoError(t, err)
			ecs := p.TargetParameters.EcsTaskParameters
			require.NotNil(t, ecs.NetworkConfiguration)
			assert.Equal(t, "subnet-aaa",
				ecs.NetworkConfiguration.AwsvpcConfiguration.Subnets[0])
		})
	}
}

// TestAudit2_Lifecycle_Delete verifies new params survive until deletion.
func TestAudit2_Lifecycle_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "delete-with-batch-params-a"},
		{name: "delete-with-batch-params-b"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := b2Backend()
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				Name:   tt.name,
				Source: b2SQSSource,
				Target: "arn:aws:batch:us-east-1:123456789012:job-queue/q",
				TargetParameters: &pipes.TargetParameters{
					BatchJobParameters: &pipes.BatchJobTargetParameters{
						JobDefinition: "jd",
						JobName:       "job",
						DependsOn: []pipes.BatchJobDependency{
							{JobID: "parent-job", Type: "SEQUENTIAL"},
						},
					},
				},
			})
			require.NoError(t, err)

			deleted, err := b.DeletePipe(context.Background(), tt.name)
			require.NoError(t, err)
			assert.Equal(t, "DELETING", deleted.CurrentState)
			assert.Equal(t, "parent-job",
				deleted.TargetParameters.BatchJobParameters.DependsOn[0].JobID)

			require.Eventually(t, func() bool {
				_, e := b.GetPipe(context.Background(), tt.name)

				return e != nil
			}, 500e6, 5e6)
		})
	}
}
