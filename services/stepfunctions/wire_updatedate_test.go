package stepfunctions_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sfnsdk "github.com/aws/aws-sdk-go-v2/service/sfn"
	sfntypes "github.com/aws/aws-sdk-go-v2/service/sfn/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/stepfunctions"
)

// newSFNSDKClient stands up the real aws-sdk-go-v2 sfn client against an
// httptest server running this package's Handler through the same
// pkgs/service registry/router used in production, so responses are
// decoded by the genuine SDK deserializer rather than ad-hoc structs.
func newSFNSDKClient(t *testing.T, h *stepfunctions.Handler) *sfnsdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return sfnsdk.NewFromConfig(cfg, func(o *sfnsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// Test_SDKRoundTrip_StateMachineAlias_UpdateDate proves Create/Update/
// Describe/ListStateMachineAlias decode through the real SDK client.
// sfn's alias family names its second timestamp "updateDate" on the wire
// (confirmed against aws-sdk-go-v2/service/sfn@v1.45.4 deserializers.go,
// e.g. the DescribeStateMachineAlias case "updateDate": branch), but the
// domain StateMachineAlias struct tags its field "updatedDate" for stable
// snapshot persistence -- a raw-JSON assertion on that tag would pass while
// a real client silently got a nil UpdateDate back.
func Test_SDKRoundTrip_StateMachineAlias_UpdateDate(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	smName := "test-sm-" + uuid.NewString()[:8]
	createSM, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(validPassDef),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/sfn-role"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)
	smArn := *createSM.StateMachineArn

	pub, err := client.PublishStateMachineVersion(ctx, &sfnsdk.PublishStateMachineVersionInput{
		StateMachineArn: aws.String(smArn),
	})
	require.NoError(t, err)

	// CreateStateMachineAliasInput has no stateMachineArn field on the real
	// wire -- AWS derives the target state machine from the version ARN
	// inside routingConfiguration -- but this backend's CreateStateMachineAlias
	// requires it explicitly, so driving CreateStateMachineAlias itself
	// through the real SDK client 404s today. That gap is unrelated to
	// gopherstack-1ai8's timestamp scope and is called out separately in the
	// session report rather than fixed here; set the alias up directly
	// against the backend so this test can isolate the UpdateDate wire-tag
	// bug this issue is about.
	alias, err := backend.CreateStateMachineAlias(smArn, "live", "", []stepfunctions.AliasRoutingConfig{
		{StateMachineVersionArn: *pub.StateMachineVersionArn, Weight: 100},
	})
	require.NoError(t, err)
	aliasArn := alias.StateMachineAliasArn
	require.NotZero(t, alias.CreationDate)

	described, err := client.DescribeStateMachineAlias(ctx, &sfnsdk.DescribeStateMachineAliasInput{
		StateMachineAliasArn: aws.String(aliasArn),
	})
	require.NoError(t, err)
	require.NotNil(t, described.UpdateDate, "DescribeStateMachineAlias must decode a non-nil updateDate")
	assert.NotZero(t, *described.UpdateDate)

	updated, err := client.UpdateStateMachineAlias(ctx, &sfnsdk.UpdateStateMachineAliasInput{
		StateMachineAliasArn: aws.String(aliasArn),
		Description:          aws.String("updated"),
	})
	require.NoError(t, err)
	require.NotNil(t, updated.UpdateDate, "UpdateStateMachineAlias must decode a non-nil updateDate")
	assert.NotZero(t, *updated.UpdateDate)

	listed, err := client.ListStateMachineAliases(ctx, &sfnsdk.ListStateMachineAliasesInput{
		StateMachineArn: aws.String(smArn),
	})
	require.NoError(t, err)
	require.Len(t, listed.StateMachineAliases, 1)
	require.NotNil(t, listed.StateMachineAliases[0].CreationDate)
	assert.NotZero(t, *listed.StateMachineAliases[0].CreationDate)
}

// Test_SDKRoundTrip_DescribeStateMachineForExecution_UpdateDate proves
// DescribeStateMachineForExecution's sole required timestamp, wired as
// "updateDate" (aws-sdk-go-v2/service/sfn@v1.45.4 api_op_DescribeStateMachineForExecution.go),
// decodes non-nil through the real client.
func Test_SDKRoundTrip_DescribeStateMachineForExecution_UpdateDate(t *testing.T) {
	t.Parallel()

	backend := stepfunctions.NewInMemoryBackend()
	h := stepfunctions.NewHandler(backend)
	client := newSFNSDKClient(t, h)
	ctx := t.Context()

	smName := "test-sm-" + uuid.NewString()[:8]
	createSM, err := client.CreateStateMachine(ctx, &sfnsdk.CreateStateMachineInput{
		Name:       aws.String(smName),
		Definition: aws.String(validPassDef),
		RoleArn:    aws.String("arn:aws:iam::000000000000:role/sfn-role"),
		Type:       sfntypes.StateMachineTypeStandard,
	})
	require.NoError(t, err)

	execOut, err := client.StartExecution(ctx, &sfnsdk.StartExecutionInput{
		StateMachineArn: createSM.StateMachineArn,
	})
	require.NoError(t, err)

	described, err := client.DescribeStateMachineForExecution(ctx, &sfnsdk.DescribeStateMachineForExecutionInput{
		ExecutionArn: execOut.ExecutionArn,
	})
	require.NoError(t, err)
	require.NotNil(
		t, described.UpdateDate,
		"DescribeStateMachineForExecution must decode a non-nil updateDate",
	)
	assert.NotZero(t, *described.UpdateDate)
	assert.WithinDuration(t, time.Now(), *described.UpdateDate, time.Minute)
}
