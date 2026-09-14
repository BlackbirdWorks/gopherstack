package cloudformation

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	cfnsdk "github.com/aws/aws-sdk-go-v2/service/cloudformation"
	cfntypes "github.com/aws/aws-sdk-go-v2/service/cloudformation/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// TestStopStackSetOperation_RealClient covers StopStackSetOperation, whose real
// output shape has zero members but whose deserializer still calls
// decoder.GetElement("StopStackSetOperationResult") (cloudformation@v1.76.1
// deserializers.go). gopherstack omitted the element, so every real SDK client failed
// deserialization with "node not found" even though the backend mutation succeeded.
//
// This lives in the internal package (not cloudformation_test) because reaching a
// genuine success path requires a stack-set operation in RUNNING status, and no
// exported API can produce one: every op this backend records
// (recordStackSetOperation) is written as SUCCEEDED synchronously, never RUNNING, so
// StopStackSetOperation is unreachable on the happy path through any public call
// sequence -- a separate gap, out of scope for this fix, worth flagging on its own.
// The unexported stackSetOperations map is seeded directly here to exercise the
// success path this fix restores.
func TestStopStackSetOperation_RealClient(t *testing.T) {
	t.Parallel()

	backend := NewInMemoryBackend()
	h := NewHandler(backend)

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

	client := cfnsdk.NewFromConfig(cfg, func(o *cfnsdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})

	const stackSetName = "empty-result-stopop-stackset"

	_, err = backend.CreateStackSet(stackSetName, "", simpleTemplateInternal, StackSetOptions{})
	require.NoError(t, err)

	opID := uuid.New().String()
	backend.stackSetOperations[stackSetName] = map[string]*StackSetOperation{
		opID: {
			OperationID:  opID,
			StackSetName: stackSetName,
			Action:       "UPDATE",
			Status:       "RUNNING",
			CreatedAt:    time.Now(),
		},
	}

	_, err = client.StopStackSetOperation(t.Context(), &cfnsdk.StopStackSetOperationInput{
		StackSetName: aws.String(stackSetName),
		OperationId:  aws.String(opID),
	})
	require.NoError(t, err)
}

// TestStopStackSetOperation_AlreadySucceeded_RealClient covers the reachable
// counterpart to TestStopStackSetOperation_RealClient above: gopherstack-b3pm
// confirmed (see PARITY.md) that every stack-set operation this backend
// records completes synchronously as SUCCEEDED, so a real caller can never
// observe RUNNING -- the only outcome StopStackSetOperation can produce
// through any public call sequence is "stop a non-RUNNING operation". Real
// AWS's modeled error set for this op (cloudformation@v1.76.1
// deserializers.go, awsAwsquery_deserializeOpErrorStopStackSetOperation) is
// exactly {InvalidOperationException, OperationNotFoundException,
// StackSetNotFoundException}; stopping an already-completed operation is
// InvalidOperationException, which handleStopStackSetOperation already
// returns for this case (op.Status != "RUNNING" -> ErrOperationNotRunning).
func TestStopStackSetOperation_AlreadySucceeded_RealClient(t *testing.T) {
	t.Parallel()

	tests := []struct {
		triggerOp func(t *testing.T, client *cfnsdk.Client, stackSetName string) string
		name      string
	}{
		{
			name: "update_stack_set",
			triggerOp: func(t *testing.T, client *cfnsdk.Client, stackSetName string) string {
				t.Helper()
				out, err := client.UpdateStackSet(t.Context(), &cfnsdk.UpdateStackSetInput{
					StackSetName: aws.String(stackSetName),
					Description:  aws.String("bump"),
				})
				require.NoError(t, err)

				return aws.ToString(out.OperationId)
			},
		},
		{
			name: "detect_stack_set_drift",
			triggerOp: func(t *testing.T, client *cfnsdk.Client, stackSetName string) string {
				t.Helper()
				out, err := client.DetectStackSetDrift(t.Context(), &cfnsdk.DetectStackSetDriftInput{
					StackSetName: aws.String(stackSetName),
				})
				require.NoError(t, err)

				return aws.ToString(out.OperationId)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := NewInMemoryBackend()
			h := NewHandler(backend)

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

			client := cfnsdk.NewFromConfig(cfg, func(o *cfnsdk.Options) {
				o.BaseEndpoint = aws.String(srv.URL)
			})

			stackSetName := "already-succeeded-" + tt.name

			_, err = backend.CreateStackSet(stackSetName, "", simpleTemplateInternal, StackSetOptions{})
			require.NoError(t, err)

			opID := tt.triggerOp(t, client, stackSetName)
			require.NotEmpty(t, opID)

			_, err = client.StopStackSetOperation(t.Context(), &cfnsdk.StopStackSetOperationInput{
				StackSetName: aws.String(stackSetName),
				OperationId:  aws.String(opID),
			})
			require.Error(t, err)

			var invalidOp *cfntypes.InvalidOperationException
			require.ErrorAs(t, err, &invalidOp)
		})
	}
}

const simpleTemplateInternal = `{"AWSTemplateFormatVersion":"2010-09-09",` +
	`"Resources":{"MyBucket":{"Type":"AWS::S3::Bucket","Properties":{}}}}`
