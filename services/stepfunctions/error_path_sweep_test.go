package stepfunctions_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// Real AWS: DeleteActivity's own error switch (aws-sdk-go-v2/service/sfn
// deserializers.go, awsAwsjson10_deserializeOpErrorDeleteActivity) models
// only InvalidArn -- no ActivityDoesNotExist -- so this is documented as
// idempotent: deleting an activity that does not exist must succeed.
func Test_SDKRoundTrip_DeleteActivity_UnknownArn_Idempotent(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)

	_, err := client.DeleteActivity(t.Context(), &sfnsdk.DeleteActivityInput{
		ActivityArn: aws.String("arn:aws:states:us-east-1:123456789012:activity:nonexistent"),
	})
	require.NoError(t, err, "DeleteActivity must be idempotent on a missing activity")
}

// Real AWS: DeleteStateMachine's own error switch models only InvalidArn and
// ValidationException -- no StateMachineDoesNotExist -- so it is idempotent.
func Test_SDKRoundTrip_DeleteStateMachine_UnknownArn_Idempotent(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)

	_, err := client.DeleteStateMachine(t.Context(), &sfnsdk.DeleteStateMachineInput{
		StateMachineArn: aws.String(
			"arn:aws:states:us-east-1:123456789012:stateMachine:nonexistent",
		),
	})
	require.NoError(t, err, "DeleteStateMachine must be idempotent on a missing state machine")
}

// Real AWS: DeleteStateMachineVersion's own error switch models
// ConflictException, InvalidArn, and ValidationException -- no
// StateMachineVersionDoesNotExist type exists anywhere in this SDK.
func Test_SDKRoundTrip_DeleteStateMachineVersion_UnknownArn_Idempotent(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)

	_, err := client.DeleteStateMachineVersion(t.Context(), &sfnsdk.DeleteStateMachineVersionInput{
		StateMachineVersionArn: aws.String(
			"arn:aws:states:us-east-1:123456789012:stateMachine:nonexistent:1",
		),
	})
	require.NoError(t, err, "DeleteStateMachineVersion must be idempotent on a missing version")
}

// Real AWS: ListStateMachineVersions models InvalidArn, InvalidToken, and
// ValidationException -- unlike its ListExecutions/ListStateMachineAliases
// siblings, it does not model StateMachineDoesNotExist, so it must return an
// empty page rather than raise for an unknown state machine ARN.
func Test_SDKRoundTrip_ListStateMachineVersions_UnknownArn_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)

	out, err := client.ListStateMachineVersions(t.Context(), &sfnsdk.ListStateMachineVersionsInput{
		StateMachineArn: aws.String(
			"arn:aws:states:us-east-1:123456789012:stateMachine:nonexistent",
		),
	})
	require.NoError(t, err)
	assert.Empty(t, out.StateMachineVersions)
}

// Real AWS: DescribeStateMachineAlias, UpdateStateMachineAlias, and
// DeleteStateMachineAlias each model ResourceNotFound for a missing alias --
// "StateMachineAliasDoesNotExist" names no type anywhere in this SDK.
func Test_SDKRoundTrip_StateMachineAlias_UnknownArn_ResourceNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, client *sfnsdk.Client, aliasArn string) error
		name string
	}{
		{
			name: "describe",
			call: func(t *testing.T, client *sfnsdk.Client, aliasArn string) error {
				t.Helper()
				_, err := client.DescribeStateMachineAlias(
					t.Context(), &sfnsdk.DescribeStateMachineAliasInput{
						StateMachineAliasArn: aws.String(aliasArn),
					},
				)

				return err
			},
		},
		{
			name: "update",
			call: func(t *testing.T, client *sfnsdk.Client, aliasArn string) error {
				t.Helper()
				_, err := client.UpdateStateMachineAlias(
					t.Context(), &sfnsdk.UpdateStateMachineAliasInput{
						StateMachineAliasArn: aws.String(aliasArn),
						Description:          aws.String("x"),
					},
				)

				return err
			},
		},
		{
			name: "delete",
			call: func(t *testing.T, client *sfnsdk.Client, aliasArn string) error {
				t.Helper()
				_, err := client.DeleteStateMachineAlias(
					t.Context(), &sfnsdk.DeleteStateMachineAliasInput{
						StateMachineAliasArn: aws.String(aliasArn),
					},
				)

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := stepfunctions.NewInMemoryBackend()
			h := stepfunctions.NewHandler(backend)
			client := newSFNSDKClient(t, h)

			err := tc.call(t, client, "arn:aws:states:us-east-1:123456789012:stateMachine:sm:nonexistent")
			require.Error(t, err)

			var rnf *sfntypes.ResourceNotFound
			require.ErrorAs(t, err, &rnf, "expected a real ResourceNotFound from the SDK deserializer")
		})
	}
}

// Real AWS: ListExecutions models StateMachineDoesNotExist for an unknown
// stateMachineArn -- unlike ListStateMachineVersions, this sibling raises.
func Test_SDKRoundTrip_ListExecutions_UnknownArn_StateMachineDoesNotExist(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)

	_, err := client.ListExecutions(t.Context(), &sfnsdk.ListExecutionsInput{
		StateMachineArn: aws.String(
			"arn:aws:states:us-east-1:123456789012:stateMachine:nonexistent",
		),
	})
	require.Error(t, err)

	var smdne *sfntypes.StateMachineDoesNotExist
	require.ErrorAs(t, err, &smdne, "expected a real StateMachineDoesNotExist from the SDK deserializer")
}

// Real AWS: DescribeMapRun and UpdateMapRun each model InvalidArn and
// ResourceNotFound -- "MapRunDoesNotExist" names no type anywhere in this SDK.
func Test_SDKRoundTrip_MapRun_UnknownArn_ResourceNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call func(t *testing.T, client *sfnsdk.Client, mapRunArn string) error
		name string
	}{
		{
			name: "describe",
			call: func(t *testing.T, client *sfnsdk.Client, mapRunArn string) error {
				t.Helper()
				_, err := client.DescribeMapRun(t.Context(), &sfnsdk.DescribeMapRunInput{
					MapRunArn: aws.String(mapRunArn),
				})

				return err
			},
		},
		{
			name: "update",
			call: func(t *testing.T, client *sfnsdk.Client, mapRunArn string) error {
				t.Helper()
				_, err := client.UpdateMapRun(t.Context(), &sfnsdk.UpdateMapRunInput{
					MapRunArn: aws.String(mapRunArn),
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := stepfunctions.NewInMemoryBackend()
			h := stepfunctions.NewHandler(backend)
			client := newSFNSDKClient(t, h)

			err := tc.call(
				t, client, "arn:aws:states:us-east-1:123456789012:mapRun:sm/exec/nonexistent",
			)
			require.Error(t, err)

			var rnf *sfntypes.ResourceNotFound
			require.ErrorAs(t, err, &rnf, "expected a real ResourceNotFound from the SDK deserializer")
		})
	}
}

// Real AWS: ListMapRuns models ExecutionDoesNotExist for an unknown
// executionArn.
func Test_SDKRoundTrip_ListMapRuns_UnknownExecution_ExecutionDoesNotExist(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)

	_, err := client.ListMapRuns(t.Context(), &sfnsdk.ListMapRunsInput{
		ExecutionArn: aws.String(
			"arn:aws:states:us-east-1:123456789012:execution:sm:nonexistent",
		),
	})
	require.Error(t, err)

	var edne *sfntypes.ExecutionDoesNotExist
	require.ErrorAs(t, err, &edne, "expected a real ExecutionDoesNotExist from the SDK deserializer")
}

// TagResource models InvalidArn, ResourceNotFound, and TooManyTags -- the
// too-many-tags branch of validateTags must raise the modelled TooManyTags
// type, not the fabricated "TagPolicyViolation" shared today across all
// three of validateTags' branches.
func Test_TagResource_TooManyTags_WireType(t *testing.T) {
	t.Parallel()

	h, e := newSFNHandler(t)
	smARN := createSFNStateMachineCov(t.Context(), t, h, e, "tag-limit-sm")

	tagList := make([]map[string]string, 0, 51)
	for range 51 {
		tagList = append(tagList, map[string]string{"key": uuid.NewString(), "value": "v"})
	}

	body, err := json.Marshal(map[string]any{
		"resourceArn": smARN,
		"tags":        tagList,
	})
	require.NoError(t, err)

	rec := sfnPost(t.Context(), t, h, e, "TagResource", string(body))

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "TooManyTags", resp["__type"])
}

// CreateStateMachineAliasInput carries no stateMachineArn field on the real
// wire (AWS derives the target from routingConfiguration), so this backend's
// CreateStateMachineAlias -- which requires stateMachineArn explicitly -- is
// unreachable through the real typed client (see
// Test_SDKRoundTrip_StateMachineAlias_UpdateDate's comment for the same
// pre-existing gap). These cases exercise the wire error type directly over
// the JSON body instead of via errors.As, since driving them through
// client.CreateStateMachineAlias itself always resolves an empty
// stateMachineArn.
func Test_CreateStateMachineAlias_ErrorCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     func(h *stepfunctions.Handler, e *echo.Echo) string
		name     string
		wantType string
	}{
		{
			name:     "unknown state machine is ResourceNotFound",
			wantType: "ResourceNotFound",
			body: func(*stepfunctions.Handler, *echo.Echo) string {
				return `{
					"name": "a",
					"stateMachineArn": "arn:aws:states:us-east-1:123456789012:stateMachine:nonexistent",
					"routingConfiguration": [
						{"stateMachineVersionArn": "arn:aws:states:us-east-1:123456789012:stateMachine:x:1", "weight": 100}
					]
				}`
			},
		},
		{
			name:     "duplicate alias name is ConflictException",
			wantType: "ConflictException",
			body: func(h *stepfunctions.Handler, e *echo.Echo) string {
				smARN := createSFNStateMachineCov(t.Context(), t, h, e, "conflict-alias-sm")
				pubRec := sfnPost(
					t.Context(), t, h, e, "PublishStateMachineVersion",
					fmt.Sprintf(`{"stateMachineArn": %q}`, smARN),
				)

				var pubResp map[string]any
				require.NoError(t, json.Unmarshal(pubRec.Body.Bytes(), &pubResp))
				versionARN, _ := pubResp["stateMachineVersionArn"].(string)
				require.NotEmpty(t, versionARN)

				createBody := fmt.Sprintf(`{
					"name": "dup",
					"stateMachineArn": %q,
					"routingConfiguration": [{"stateMachineVersionArn": %q, "weight": 100}]
				}`, smARN, versionARN)

				createRec := sfnPost(t.Context(), t, h, e, "CreateStateMachineAlias", createBody)
				require.Equal(t, 200, createRec.Code)

				return createBody
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, e := newSFNHandler(t)
			rec := sfnPost(t.Context(), t, h, e, "CreateStateMachineAlias", tc.body(h, e))

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, tc.wantType, resp["__type"])
		})
	}
}
