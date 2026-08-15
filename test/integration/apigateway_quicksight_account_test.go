package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	apigwsdk "github.com/aws/aws-sdk-go-v2/service/apigateway"
	qssdk "github.com/aws/aws-sdk-go-v2/service/quicksight"
	qstypes "github.com/aws/aws-sdk-go-v2/service/quicksight/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestIntegration_APIGateway_QuickSight_AccountSubscriptionRouting is a
// permanent router-level regression guard for gopherstack-op3e: API
// Gateway's RouteMatcher unconditionally claimed any "/account/..." path
// (isAPIGWTopLevelRESTPath), and API Gateway is evaluated at the highest
// router priority tier (PriorityHeaderExact). Since API Gateway registers
// before QuickSight and always wins the priority sort regardless, every
// QuickSight account-subscription op -- which AWS mints under the singular
// "/account/{AwsAccountId}" -- was intercepted by API Gateway's own
// unmatched-operation 404 and never reached QuickSight. A Handler()-level
// unit test cannot see this: it calls the handler directly, bypassing
// RouteMatcher and the shared router entirely.
func TestIntegration_APIGateway_QuickSight_AccountSubscriptionRouting(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	qs := createQuickSightClient(t)
	apigw := createAPIGatewayAuditClient(t)
	ctx := t.Context()

	accountID := uuid.NewString()[:12]

	// Not parallel: describe depends on create having already run.
	t.Run("quicksight create account subscription", func(t *testing.T) { //nolint:paralleltest // sequential by design
		_, err := qs.CreateAccountSubscription(ctx, &qssdk.CreateAccountSubscriptionInput{
			AwsAccountId:         aws.String(accountID),
			AccountName:          aws.String("op3e-test-account-" + accountID),
			AuthenticationMethod: qstypes.AuthenticationMethodOptionIamAndQuicksight,
			Edition:              qstypes.EditionStandard,
			NotificationEmail:    aws.String("op3e@example.com"),
		})
		require.NoError(t, err)
	})

	t.Run("quicksight describe account subscription", func(t *testing.T) { //nolint:paralleltest // sequential by design
		out, err := qs.DescribeAccountSubscription(ctx, &qssdk.DescribeAccountSubscriptionInput{
			AwsAccountId: aws.String(accountID),
		})
		require.NoError(t, err)
		require.NotNil(t, out.AccountInfo)
	})

	t.Run("apigateway get account still reaches apigateway", func(t *testing.T) {
		t.Parallel()

		out, err := apigw.GetAccount(ctx, &apigwsdk.GetAccountInput{})
		require.NoError(t, err)
		require.NotNil(t, out)
	})

	t.Run("apigateway update account still reaches apigateway", func(t *testing.T) {
		t.Parallel()

		out, err := apigw.UpdateAccount(ctx, &apigwsdk.UpdateAccountInput{})
		require.NoError(t, err)
		require.NotNil(t, out)
	})
}
