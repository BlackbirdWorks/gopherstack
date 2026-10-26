package cloudformation_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	"github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const samSMDefinition = `      Definition:
        StartAt: Hello
        States:
          Hello:
            Type: Pass
            End: true
`

func samSM(events string) string {
	return samHeader + "Resources:\n  SM:\n    Type: AWS::Serverless::StateMachine\n    Properties:\n" +
		samSMDefinition + "      Events:\n" + events
}

// samEvent renders one event; props are "Key: value" lines at property depth.
func samEvent(name, typ string, props ...string) string {
	var sb strings.Builder

	sb.WriteString("        " + name + ":\n          Type: " + typ + "\n          Properties:\n")

	for _, p := range props {
		sb.WriteString("            " + p + "\n")
	}

	return sb.String()
}

func processedResources(t *testing.T, env *samEventEnv) map[string]map[string]any {
	t.Helper()

	var doc struct {
		Resources map[string]map[string]any `json:"Resources"`
	}

	require.NoError(
		t,
		json.Unmarshal([]byte(getTemplate(t, env.client, "sam-stack", types.TemplateStageProcessed)), &doc),
	)

	return doc.Resources
}

func dig(t *testing.T, v any, path ...string) any {
	t.Helper()

	for _, k := range path {
		m, ok := v.(map[string]any)
		require.True(t, ok, "path %v at %q", path, k)
		v = m[k]
	}

	return v
}

func policyNames(t *testing.T, role map[string]any) []string {
	t.Helper()

	pols := dig(t, role, "Properties", "Policies").([]any)
	out := make([]string, 0, len(pols))

	for _, p := range pols {
		out = append(out, dig(t, p, "PolicyName").(string))
	}

	return out
}

func TestSAMTransform_StateMachineEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(t *testing.T, res map[string]map[string]any)
		wantType map[string]string
		name     string
		events   string
	}{
		{
			name: "schedule",
			events: samEvent("Tick", "Schedule", "Schedule: rate(1 minute)", "Name: tick-rule", "Enabled: false",
				`Input: '{"a":1}'`),
			wantType: map[string]string{"SMTick": "AWS::Events::Rule", "SMTickRole": "AWS::IAM::Role"},
			check: func(t *testing.T, res map[string]map[string]any) {
				t.Helper()
				p := dig(t, res["SMTick"], "Properties").(map[string]any)
				assert.Equal(t, "DISABLED", p["State"])
				assert.Equal(t, "tick-rule", p["Name"])
				target := p["Targets"].([]any)[0].(map[string]any)
				assert.Equal(t, "SMTickStepFunctionsTarget", target["Id"])
				assert.Equal(t, map[string]any{"Ref": "SM"}, target["Arn"])
				assert.Equal(t, `{"a":1}`, target["Input"])
				assert.Equal(t, []string{"SMTickRoleStartExecutionPolicy"}, policyNames(t, res["SMTickRole"]))
			},
		},
		{
			name: "schedule_role_arn_and_target_id",
			events: samEvent(
				"Tick",
				"Schedule",
				"Schedule: rate(1 minute)",
				"RoleArn: arn:aws:iam::000000000000:role/r",
				"Target:",
				"  Id: MyTarget",
			),
			wantType: map[string]string{"SMTick": "AWS::Events::Rule"},
			check: func(t *testing.T, res map[string]map[string]any) {
				t.Helper()
				assert.NotContains(t, res, "SMTickRole")
				target := dig(t, res["SMTick"], "Properties", "Targets").([]any)[0].(map[string]any)
				assert.Equal(t, "MyTarget", target["Id"])
				assert.Equal(t, "arn:aws:iam::000000000000:role/r", target["RoleArn"])
			},
		},
		{
			name: "cloudwatch_event",
			events: samEvent("CWEvent", "CloudWatchEvent", "RuleName: MyRule", "State: ENABLED", "Pattern:",
				"  detail:", "    state:", "      - terminated"),
			wantType: map[string]string{"SMCWEvent": "AWS::Events::Rule", "SMCWEventRole": "AWS::IAM::Role"},
			check: func(t *testing.T, res map[string]map[string]any) {
				t.Helper()
				p := dig(t, res["SMCWEvent"], "Properties").(map[string]any)
				assert.Equal(t, "MyRule", p["Name"])
				assert.Equal(t, "ENABLED", p["State"])
				target := p["Targets"].([]any)[0].(map[string]any)
				assert.Equal(t, "SMCWEventStepFunctionsTarget", target["Id"])
			},
		},
		{
			name:     "eventbridge_rule",
			events:   samEvent("Rule", "EventBridgeRule", "Pattern:", "  source:", "    - my.app"),
			wantType: map[string]string{"SMRule": "AWS::Events::Rule", "SMRuleRole": "AWS::IAM::Role"},
		},
		{
			name: "schedule_v2",
			events: samEvent("V2", "ScheduleV2", "ScheduleExpression: rate(1 minute)", "DeadLetterConfig:",
				"  Arn: arn:aws:sqs:us-east-1:000000000000:dlq"),
			wantType: map[string]string{"SMV2": "AWS::Scheduler::Schedule", "SMV2Role": "AWS::IAM::Role"},
			check: func(t *testing.T, res map[string]map[string]any) {
				t.Helper()
				assert.Equal(t, []string{"SMV2StartExecutionPolicy", "SMV2SQSPolicy"}, policyNames(t, res["SMV2Role"]))
				stmt := dig(t, res["SMV2Role"], "Properties", "AssumeRolePolicyDocument", "Statement").([]any)[0]
				assert.Equal(t, []any{"scheduler.amazonaws.com"}, dig(t, stmt, "Principal", "Service"))
			},
		},
		{
			name:   "api_implicit",
			events: samEvent("Start", "Api", "Path: /start", "Method: post"),
			wantType: map[string]string{
				"SMStartRole":       "AWS::IAM::Role",
				"ServerlessRestApi": "AWS::ApiGateway::RestApi",
			},
			check: func(t *testing.T, res map[string]map[string]any) {
				t.Helper()
				op := dig(t, res["ServerlessRestApi"], "Properties", "Body", "paths", "/start", "post").(map[string]any)
				integ := op["x-amazon-apigateway-integration"].(map[string]any)
				assert.Equal(t, "aws", integ["type"])
				assert.Equal(t, map[string]any{"Fn::GetAtt": []any{"SMStartRole", "Arn"}}, integ["credentials"])
				assert.Equal(
					t,
					map[string]any{
						"Fn::Sub": "arn:${AWS::Partition}:apigateway:${AWS::Region}:states:action/StartExecution",
					},
					integ["uri"],
				)
				assert.Equal(t, []string{"SMStartRoleStartExecutionPolicy"}, policyNames(t, res["SMStartRole"]))
				assert.Contains(t, dig(t, integ, "requestTemplates", "application/json", "Fn::Sub"),
					`"stateMachineArn": "${SM}"`)
			},
		},
		{
			name:     "api_unescape",
			events:   samEvent("Start", "Api", "Path: /start", "Method: post", "UnescapeMappingTemplate: true"),
			wantType: map[string]string{"SMStartRole": "AWS::IAM::Role"},
			check: func(t *testing.T, res map[string]map[string]any) {
				t.Helper()
				integ := dig(t, res["ServerlessRestApi"], "Properties", "Body", "paths", "/start", "post",
					"x-amazon-apigateway-integration")
				assert.Contains(t, dig(t, integ, "requestTemplates", "application/json", "Fn::Sub"),
					`.replaceAll("\\'","'")`)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			deploySAM(t, env.client, samSM(tc.events))

			ids := logicalIDs(t, env.client)
			assert.Equal(t, "AWS::StepFunctions::StateMachine", ids["SM"])

			for id, typ := range tc.wantType {
				assert.Equal(t, typ, ids[id], id)
			}

			if tc.check != nil {
				tc.check(t, processedResources(t, env))
			}

			assertDeleteClean(t, env.client)
		})
	}
}

func TestSAMTransform_StateMachineEventFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		events string
		reason string
	}{
		{"unknown_type", samEvent("E", "SQS", "Queue: x"), "not supported"},
		{"schedule_missing_expression", samEvent("E", "Schedule", "Name: x"), "Schedule"},
		{"unknown_property", samEvent("E", "Schedule", "Schedule: rate(1 minute)", "Bogus: 1"), "Bogus"},
		{
			"dlq_generated",
			samEvent("E", "Schedule", "Schedule: rate(1 minute)", "DeadLetterConfig:", "  Type: SQS"),
			"DeadLetterConfig",
		},
		{"api_missing_method", samEvent("E", "Api", "Path: /x"), "'Path' and 'Method'"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			_, err := env.client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
				StackName: aws.String("bad"), ChangeSetName: aws.String("cs"), ChangeSetType: types.ChangeSetTypeCreate,
				TemplateBody: aws.String(samSM(tc.events)),
				Capabilities: []types.Capability{types.CapabilityCapabilityIam, types.CapabilityCapabilityAutoExpand},
			})
			require.NoError(t, err)

			cs, err := env.client.DescribeChangeSet(t.Context(), &cfnsdk.DescribeChangeSetInput{
				StackName: aws.String("bad"), ChangeSetName: aws.String("cs"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.ChangeSetStatusFailed, cs.Status)
			assert.Contains(t, aws.ToString(cs.StatusReason), tc.reason)
		})
	}
}

func TestSAMTransform_ApiEventExtras(t *testing.T) {
	t.Parallel()

	fn := func(apiAuth, events string) string {
		return samHeader + "Resources:\n  Api:\n    Type: AWS::Serverless::Api\n    Properties:\n      StageName: Prod\n" +
			apiAuth + "  Fn:\n    Type: AWS::Serverless::Function\n    Properties:\n" + samFnHeader +
			"      Events:\n" + events
	}
	tests := []struct {
		check  func(t *testing.T, paths map[string]any, defs map[string]any)
		name   string
		apiKey string
		events string
	}{
		{
			name: "request_parameters",
			events: samEvent("E", "Api", "RestApiId: !Ref Api", "Path: /one", "Method: get", "RequestParameters:",
				"  - method.request.header.Authorization:", "      Required: true", "      Caching: true",
				"  - method.request.querystring.full.type", "  - method.request.path.id"),
			check: func(t *testing.T, paths, _ map[string]any) {
				t.Helper()
				op := dig(t, paths, "/one", "get").(map[string]any)
				assert.Equal(t, []any{
					map[string]any{"in": "header", "name": "Authorization", "required": true, "type": "string"},
					map[string]any{"in": "query", "name": "full.type", "required": false, "type": "string"},
					map[string]any{"in": "path", "name": "id", "required": false, "type": "string"},
				}, op["parameters"])
				assert.Equal(t, []any{"method.request.header.Authorization"},
					dig(t, op, "x-amazon-apigateway-integration", "cacheKeyParameters"))
			},
		},
		{
			name:   "timeout",
			events: samEvent("E", "Api", "RestApiId: !Ref Api", "Path: /one", "Method: get", "TimeoutInMillis: 5000"),
			check: func(t *testing.T, paths, _ map[string]any) {
				t.Helper()
				assert.InDelta(
					t,
					5000,
					dig(t, paths, "/one", "get", "x-amazon-apigateway-integration", "timeoutInMillis"),
					0,
				)
			},
		},
		{
			name: "api_key_required_event",
			events: samEvent("E", "Api", "RestApiId: !Ref Api", "Path: /one", "Method: get", "Auth:",
				"  ApiKeyRequired: true"),
			check: func(t *testing.T, paths, defs map[string]any) {
				t.Helper()
				assert.Equal(t, []any{map[string]any{"api_key": []any{}}}, dig(t, paths, "/one", "get", "security"))
				assert.Equal(t, map[string]any{"in": "header", "name": "x-api-key", "type": "apiKey"}, defs["api_key"])
			},
		},
		{
			name:   "api_key_default_override",
			apiKey: "      Auth:\n        ApiKeyRequired: true\n",
			events: samEvent("A", "Api", "RestApiId: !Ref Api", "Path: /a", "Method: get") +
				samEvent(
					"B",
					"Api",
					"RestApiId: !Ref Api",
					"Path: /b",
					"Method: get",
					"Auth:",
					"  ApiKeyRequired: false",
				),
			check: func(t *testing.T, paths, defs map[string]any) {
				t.Helper()
				assert.Equal(t, []any{map[string]any{"api_key": []any{}}}, dig(t, paths, "/a", "get", "security"))
				assert.NotContains(t, dig(t, paths, "/b", "get"), "security")
				assert.Contains(t, defs, "api_key")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			env := newSAMEventEnv(t)
			deploySAM(t, env.client, fn(tc.apiKey, tc.events))

			res := processedResources(t, env)
			body := dig(t, res["Api"], "Properties", "Body")
			defs, _ := dig(t, body, "securityDefinitions").(map[string]any)
			tc.check(t, dig(t, body, "paths").(map[string]any), defs)
			assertDeleteClean(t, env.client)
		})
	}
}

func TestSAMTransform_ApiEventExtraFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, props, reason string
	}{
		{
			"bad_request_parameter",
			"            RequestParameters:\n              - request.header.X\n",
			"RequestParameters",
		},
		{"timeout_out_of_range", "            TimeoutInMillis: 60000\n", "TimeoutInMillis"},
		{"api_key_not_bool", "            Auth:\n              ApiKeyRequired: maybe\n", "ApiKeyRequired"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tmpl := samHeader + "Resources:\n  Fn:\n    Type: AWS::Serverless::Function\n    Properties:\n" + samFnHeader +
				"      Events:\n        E:\n          Type: Api\n          Properties:\n            Path: /x\n" +
				"            Method: get\n" + tc.props
			env := newSAMEventEnv(t)
			_, err := env.client.CreateChangeSet(t.Context(), &cfnsdk.CreateChangeSetInput{
				StackName: aws.String("bad"), ChangeSetName: aws.String("cs"), ChangeSetType: types.ChangeSetTypeCreate,
				TemplateBody: aws.String(tmpl),
				Capabilities: []types.Capability{types.CapabilityCapabilityIam, types.CapabilityCapabilityAutoExpand},
			})
			require.NoError(t, err)

			cs, err := env.client.DescribeChangeSet(t.Context(), &cfnsdk.DescribeChangeSetInput{
				StackName: aws.String("bad"), ChangeSetName: aws.String("cs"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.ChangeSetStatusFailed, cs.Status)
			assert.Contains(t, aws.ToString(cs.StatusReason), tc.reason)
		})
	}
}
