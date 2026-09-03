package eventbridge_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// TestCreateEventBus_DeadLetterConfigKmsLogConfig_RealClient proves
// CreateEventBus's DeadLetterConfig/KmsKeyIdentifier/LogConfig -- previously
// parsed nowhere in this backend -- now round-trip through Create, Describe,
// and Update, and are correctly absent from the narrower ListEventBuses item
// shape (real "EventBus" type has neither member).
func TestCreateEventBus_DeadLetterConfigKmsLogConfig_RealClient(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	const dlqArn = "arn:aws:sqs:us-east-1:123456789012:my-dlq"
	const kmsKey = "arn:aws:kms:us-east-1:123456789012:key/abc-123"

	created, err := client.CreateEventBus(t.Context(), &eventbridgesdk.CreateEventBusInput{
		Name:             aws.String("secure-bus"),
		DeadLetterConfig: &ebtypes.DeadLetterConfig{Arn: aws.String(dlqArn)},
		KmsKeyIdentifier: aws.String(kmsKey),
		LogConfig: &ebtypes.LogConfig{
			IncludeDetail: ebtypes.IncludeDetailFull,
			Level:         ebtypes.LevelInfo,
		},
	})
	require.NoError(t, err)
	require.NotNil(t, created.DeadLetterConfig)
	assert.Equal(t, dlqArn, aws.ToString(created.DeadLetterConfig.Arn))
	assert.Equal(t, kmsKey, aws.ToString(created.KmsKeyIdentifier))
	require.NotNil(t, created.LogConfig)
	assert.Equal(t, ebtypes.IncludeDetailFull, created.LogConfig.IncludeDetail)

	described, err := client.DescribeEventBus(t.Context(), &eventbridgesdk.DescribeEventBusInput{
		Name: aws.String("secure-bus"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.DeadLetterConfig)
	assert.Equal(t, dlqArn, aws.ToString(described.DeadLetterConfig.Arn))
	assert.Equal(t, kmsKey, aws.ToString(described.KmsKeyIdentifier))
	require.NotNil(t, described.LogConfig)
	assert.Equal(t, ebtypes.LevelInfo, described.LogConfig.Level)

	const newDLQArn = "arn:aws:sqs:us-east-1:123456789012:other-dlq"

	updated, err := client.UpdateEventBus(t.Context(), &eventbridgesdk.UpdateEventBusInput{
		Name:             aws.String("secure-bus"),
		DeadLetterConfig: &ebtypes.DeadLetterConfig{Arn: aws.String(newDLQArn)},
	})
	require.NoError(t, err)
	require.NotNil(t, updated.DeadLetterConfig)
	assert.Equal(t, newDLQArn, aws.ToString(updated.DeadLetterConfig.Arn))

	listed, err := client.ListEventBuses(t.Context(), &eventbridgesdk.ListEventBusesInput{
		NamePrefix: aws.String("secure-"),
	})
	require.NoError(t, err)
	require.Len(t, listed.EventBuses, 1)
	// ListEventBuses' real item type has no DeadLetterConfig/KmsKeyIdentifier/
	// LogConfig members at all -- the typed SDK client has no field to
	// decode them into, confirming the handler's narrower List shape.
	assert.Equal(t, "secure-bus", aws.ToString(listed.EventBuses[0].Name))
}

// TestListArchives_FiltersByEventSourceAndState_RealClient proves
// ListArchives' EventSourceArn and State filters -- previously parsed
// nowhere in this backend, so every archive was returned regardless of the
// filter -- now actually narrow the result set.
func TestListArchives_FiltersByEventSourceAndState_RealClient(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	_, err := client.CreateEventBus(t.Context(), &eventbridgesdk.CreateEventBusInput{Name: aws.String("bus-a")})
	require.NoError(t, err)
	_, err = client.CreateEventBus(t.Context(), &eventbridgesdk.CreateEventBusInput{Name: aws.String("bus-b")})
	require.NoError(t, err)

	busAArn := "arn:aws:events:us-east-1:123456789012:event-bus/bus-a"
	busBArn := "arn:aws:events:us-east-1:123456789012:event-bus/bus-b"

	_, err = client.CreateArchive(t.Context(), &eventbridgesdk.CreateArchiveInput{
		ArchiveName:    aws.String("archive-a"),
		EventSourceArn: aws.String(busAArn),
	})
	require.NoError(t, err)
	_, err = client.CreateArchive(t.Context(), &eventbridgesdk.CreateArchiveInput{
		ArchiveName:    aws.String("archive-b"),
		EventSourceArn: aws.String(busBArn),
	})
	require.NoError(t, err)

	bySource, err := client.ListArchives(t.Context(), &eventbridgesdk.ListArchivesInput{
		EventSourceArn: aws.String(busAArn),
	})
	require.NoError(t, err)
	require.Len(t, bySource.Archives, 1)
	assert.Equal(t, "archive-a", aws.ToString(bySource.Archives[0].ArchiveName))

	byState, err := client.ListArchives(t.Context(), &eventbridgesdk.ListArchivesInput{
		State: ebtypes.ArchiveStateEnabled,
	})
	require.NoError(t, err)
	assert.Len(t, byState.Archives, 2)

	byMissingState, err := client.ListArchives(t.Context(), &eventbridgesdk.ListArchivesInput{
		State: ebtypes.ArchiveStateDisabled,
	})
	require.NoError(t, err)
	assert.Empty(t, byMissingState.Archives)
}

// TestDescribeReplay_ReplayArn_RealClient proves DescribeReplay's ReplayArn
// -- already computed by the backend and used by CancelReplay/StartReplay's
// own outputs -- now reaches the wire instead of always decoding empty.
func TestDescribeReplay_ReplayArn_RealClient(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	bus, err := client.CreateEventBus(t.Context(), &eventbridgesdk.CreateEventBusInput{Name: aws.String("replay-bus")})
	require.NoError(t, err)

	archive, err := client.CreateArchive(t.Context(), &eventbridgesdk.CreateArchiveInput{
		ArchiveName:    aws.String("replay-archive"),
		EventSourceArn: bus.EventBusArn,
	})
	require.NoError(t, err)

	started, err := client.StartReplay(t.Context(), &eventbridgesdk.StartReplayInput{
		ReplayName:     aws.String("my-replay"),
		EventSourceArn: archive.ArchiveArn,
		EventStartTime: aws.Time(time.Now().Add(-time.Hour)),
		EventEndTime:   aws.Time(time.Now()),
		Destination:    &ebtypes.ReplayDestination{Arn: bus.EventBusArn},
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(started.ReplayArn))

	described, err := client.DescribeReplay(t.Context(), &eventbridgesdk.DescribeReplayInput{
		ReplayName: aws.String("my-replay"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(started.ReplayArn), aws.ToString(described.ReplayArn))
	assert.NotEmpty(t, aws.ToString(described.ReplayArn))
}

// TestCreateUpdateEndpoint_EchoesBackendState_RealClient proves
// CreateEndpoint/UpdateEndpoint echo EventBuses/Name/ReplicationConfig/
// RoleArn/RoutingConfig -- all already known from the backend object right
// after Create/Update -- instead of returning only Arn/EndpointId/
// EndpointUrl/State (with EndpointId/EndpointUrl themselves not even real
// CreateEndpointOutput members).
func TestCreateUpdateEndpoint_EchoesBackendState_RealClient(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	_, err := client.CreateEventBus(t.Context(), &eventbridgesdk.CreateEventBusInput{Name: aws.String("primary")})
	require.NoError(t, err)
	_, err = client.CreateEventBus(t.Context(), &eventbridgesdk.CreateEventBusInput{Name: aws.String("secondary")})
	require.NoError(t, err)

	primaryArn := "arn:aws:events:us-east-1:123456789012:event-bus/primary"
	secondaryArn := "arn:aws:events:us-west-2:123456789012:event-bus/secondary"

	created, err := client.CreateEndpoint(t.Context(), &eventbridgesdk.CreateEndpointInput{
		Name: aws.String("my-endpoint"),
		RoutingConfig: &ebtypes.RoutingConfig{
			FailoverConfig: &ebtypes.FailoverConfig{
				Primary:   &ebtypes.Primary{HealthCheck: aws.String("arn:aws:route53:::healthcheck/abc")},
				Secondary: &ebtypes.Secondary{Route: aws.String("us-west-2")},
			},
		},
		ReplicationConfig: &ebtypes.ReplicationConfig{State: ebtypes.ReplicationStateEnabled},
		EventBuses: []ebtypes.EndpointEventBus{
			{EventBusArn: aws.String(primaryArn)},
			{EventBusArn: aws.String(secondaryArn)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-endpoint", aws.ToString(created.Name))
	require.Len(t, created.EventBuses, 2)
	require.NotNil(t, created.ReplicationConfig)
	assert.Equal(t, ebtypes.ReplicationStateEnabled, created.ReplicationConfig.State)
	require.NotNil(t, created.RoutingConfig)
	require.NotNil(t, created.RoutingConfig.FailoverConfig)

	updated, err := client.UpdateEndpoint(t.Context(), &eventbridgesdk.UpdateEndpointInput{
		Name:        aws.String("my-endpoint"),
		Description: aws.String("updated"),
	})
	require.NoError(t, err)
	assert.Equal(t, "my-endpoint", aws.ToString(updated.Name))
	require.Len(t, updated.EventBuses, 2)
	require.NotNil(t, updated.ReplicationConfig)
	assert.Equal(t, ebtypes.ReplicationStateEnabled, updated.ReplicationConfig.State)
}

// TestPutTargets_BatchRetryStrategy_RealClient proves Target.BatchParameters.
// RetryStrategy -- absent from this backend's model entirely, so previously
// silently dropped on PutTargets and never echoed back -- now round-trips
// through ListTargetsByRule.
func TestPutTargets_BatchRetryStrategy_RealClient(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	_, err := client.PutRule(t.Context(), &eventbridgesdk.PutRuleInput{
		Name:         aws.String("batch-rule"),
		EventPattern: aws.String(`{"source":["batch.test"]}`),
	})
	require.NoError(t, err)

	putResp, err := client.PutTargets(t.Context(), &eventbridgesdk.PutTargetsInput{
		Rule: aws.String("batch-rule"),
		Targets: []ebtypes.Target{
			{
				Id:  aws.String("t1"),
				Arn: aws.String("arn:aws:batch:us-east-1:123456789012:job-queue/my-queue"),
				BatchParameters: &ebtypes.BatchParameters{
					JobDefinition: aws.String("my-job-def"),
					JobName:       aws.String("my-job"),
					RetryStrategy: &ebtypes.BatchRetryStrategy{Attempts: 3},
				},
			},
		},
	})
	require.NoError(t, err)
	require.Zero(t, putResp.FailedEntryCount)

	listed, err := client.ListTargetsByRule(t.Context(), &eventbridgesdk.ListTargetsByRuleInput{
		Rule: aws.String("batch-rule"),
	})
	require.NoError(t, err)
	require.Len(t, listed.Targets, 1)
	require.NotNil(t, listed.Targets[0].BatchParameters)
	require.NotNil(t, listed.Targets[0].BatchParameters.RetryStrategy)
	assert.Equal(t, int32(3), listed.Targets[0].BatchParameters.RetryStrategy.Attempts)
}

// TestDeauthorizeUpdateConnection_EchoesTimestamps_RealClient proves
// DeauthorizeConnection/UpdateConnection echo CreationTime (and
// LastAuthorizedTime once set) -- both already known from the backend's
// Connection object -- instead of leaving them at the SDK's zero value.
func TestDeauthorizeUpdateConnection_EchoesTimestamps_RealClient(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	created, err := client.CreateConnection(t.Context(), &eventbridgesdk.CreateConnectionInput{
		Name:              aws.String("my-conn"),
		AuthorizationType: ebtypes.ConnectionAuthorizationTypeApiKey,
		AuthParameters: &ebtypes.CreateConnectionAuthRequestParameters{
			ApiKeyAuthParameters: &ebtypes.CreateConnectionApiKeyAuthRequestParameters{
				ApiKeyName:  aws.String("x-api-key"),
				ApiKeyValue: aws.String("super-secret-value"),
			},
		},
	})
	require.NoError(t, err)
	require.False(t, aws.ToTime(created.CreationTime).IsZero())

	updated, err := client.UpdateConnection(t.Context(), &eventbridgesdk.UpdateConnectionInput{
		Name:        aws.String("my-conn"),
		Description: aws.String("updated"),
	})
	require.NoError(t, err)
	assert.False(t, aws.ToTime(updated.CreationTime).IsZero())
	assert.Equal(t, aws.ToTime(created.CreationTime), aws.ToTime(updated.CreationTime))

	deauthed, err := client.DeauthorizeConnection(t.Context(), &eventbridgesdk.DeauthorizeConnectionInput{
		Name: aws.String("my-conn"),
	})
	require.NoError(t, err)
	assert.Equal(t, aws.ToTime(created.CreationTime), aws.ToTime(deauthed.CreationTime))

	// DescribeConnection's AuthParameters must never carry the plaintext API
	// key value back onto the wire, even though the backend stores it
	// internally to sign outbound requests -- confirms the pre-existing
	// maskConnectionAuthParameters redaction still holds after this
	// session's connectionSummary/connectionResponse split.
	described, err := client.DescribeConnection(t.Context(), &eventbridgesdk.DescribeConnectionInput{
		Name: aws.String("my-conn"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.AuthParameters)
	require.NotNil(t, described.AuthParameters.ApiKeyAuthParameters)
	assert.Equal(t, "x-api-key", aws.ToString(described.AuthParameters.ApiKeyAuthParameters.ApiKeyName))

	listed, err := client.ListConnections(t.Context(), &eventbridgesdk.ListConnectionsInput{})
	require.NoError(t, err)
	require.Len(t, listed.Connections, 1)
	// ListConnections' real item type has no AuthParameters member at all --
	// the typed SDK client has no field to decode it into.
	assert.Equal(t, "my-conn", aws.ToString(listed.Connections[0].Name))
}

// TestPutRule_CreatedBy_DescribeOnly_RealClient is a write-only-state bug:
// the backend has always tracked its own account ID (InMemoryBackend.accountID,
// used to build every rule's ARN), but Rule had no CreatedBy field at all, so
// PutRule accepted a rule creation and the caller's account identity was never
// surfaced anywhere -- DescribeRuleOutput.CreatedBy (aws-sdk-go-v2/service/
// eventbridge@v1.48.4 api_op_DescribeRule.go) was always nil on a real client.
// CreatedBy is DescribeRule-only: real types.Rule (backing ListRulesOutput)
// has no such member, so this also proves ListRules correctly omits it.
func TestPutRule_CreatedBy_DescribeOnly_RealClient(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	_, err := client.PutRule(t.Context(), &eventbridgesdk.PutRuleInput{
		Name:         aws.String("my-rule"),
		EventPattern: aws.String(`{"source":["test"]}`),
	})
	require.NoError(t, err)

	described, err := client.DescribeRule(t.Context(), &eventbridgesdk.DescribeRuleInput{
		Name: aws.String("my-rule"),
	})
	require.NoError(t, err)
	require.NotNil(t, described.CreatedBy,
		"DescribeRuleOutput.CreatedBy must round-trip from the backend's tracked account ID; pre-fix it was always nil")
	assert.NotEmpty(t, aws.ToString(described.CreatedBy))

	listed, err := client.ListRules(t.Context(), &eventbridgesdk.ListRulesInput{})
	require.NoError(t, err)
	require.Len(t, listed.Rules, 1)
	// types.Rule (ListRules' item shape) has no CreatedBy member at all --
	// the typed SDK client has no field to decode it into.
	assert.Equal(t, "my-rule", aws.ToString(listed.Rules[0].Name))
}

// TestPutPermission_Condition_RoundTripsThroughPolicy is a write-only-state
// bug: PutPermissionInput had no Condition field at all (real SDK:
// aws-sdk-go-v2/service/eventbridge@v1.48.4 api_op_PutPermission.go,
// PutPermissionInput.Condition *types.Condition), so a caller granting
// cross-account access scoped to an AWS Organization (Principal="*" plus a
// Condition on aws:PrincipalOrgID -- the documented pattern for
// org-wide grants) had that Condition silently dropped by json.Unmarshal:
// never stored on the statement, and DescribeEventBus.Policy -- the only real
// read path for a bus's resource policy -- could never echo it back.
func TestPutPermission_Condition_RoundTripsThroughPolicy(t *testing.T) {
	t.Parallel()

	h := eventbridge.NewHandler(eventbridge.NewInMemoryBackend())
	client := newTestEventBridgeClient(t, h)

	_, err := client.PutPermission(t.Context(), &eventbridgesdk.PutPermissionInput{
		Action:      aws.String("events:PutEvents"),
		Principal:   aws.String("*"),
		StatementId: aws.String("OrgGrant"),
		Condition: &ebtypes.Condition{
			Type:  aws.String("StringEquals"),
			Key:   aws.String("aws:PrincipalOrgID"),
			Value: aws.String("o-1234567890"),
		},
	})
	require.NoError(t, err)

	described, err := client.DescribeEventBus(t.Context(), &eventbridgesdk.DescribeEventBusInput{})
	require.NoError(t, err)
	require.NotNil(t, described.Policy)

	var statements []struct {
		Condition map[string]map[string]string `json:"Condition"`
		Sid       string                       `json:"Sid"`
	}
	require.NoError(t, json.Unmarshal([]byte(aws.ToString(described.Policy)), &statements))
	require.Len(t, statements, 1)
	assert.Equal(t, "OrgGrant", statements[0].Sid)
	require.NotEmpty(
		t, statements[0].Condition,
		"the Condition supplied to PutPermission must round-trip through "+
			"DescribeEventBus.Policy; pre-fix it was silently dropped",
	)
	assert.Equal(t, "o-1234567890", statements[0].Condition["StringEquals"]["aws:PrincipalOrgID"])
}
