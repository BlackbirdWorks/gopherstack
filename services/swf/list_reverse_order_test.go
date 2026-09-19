package swf_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	swfsdk "github.com/aws/aws-sdk-go-v2/service/swf"
	swftypes "github.com/aws/aws-sdk-go-v2/service/swf/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

// ListDomainsInput/ListActivityTypesInput/ListWorkflowTypesInput each declare a
// ReverseOrder member (swf@v1.37.4: "By default the results are returned in
// ascending alphabetical order") that was parsed nowhere -- every call returned
// ascending order regardless of the request.
func TestList_ReverseOrder_RealClient(t *testing.T) {
	t.Parallel()

	t.Run("domains", func(t *testing.T) {
		t.Parallel()

		backend := swf.NewInMemoryBackend()
		client := newTestSWFSDKClient(t, swf.NewHandler(backend))
		ctx := t.Context()

		for _, name := range []string{"a-domain", "z-domain"} {
			_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
				Name:                                   aws.String(name),
				WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
			})
			require.NoError(t, err)
		}

		out, err := client.ListDomains(ctx, &swfsdk.ListDomainsInput{
			RegistrationStatus: swftypes.RegistrationStatusRegistered,
			ReverseOrder:       true,
		})
		require.NoError(t, err)
		require.Len(t, out.DomainInfos, 2)
		require.Equal(t, "z-domain", aws.ToString(out.DomainInfos[0].Name), "ReverseOrder must sort descending")
		require.Equal(t, "a-domain", aws.ToString(out.DomainInfos[1].Name))
	})

	t.Run("activity types", func(t *testing.T) {
		t.Parallel()

		backend := swf.NewInMemoryBackend()
		client := newTestSWFSDKClient(t, swf.NewHandler(backend))
		ctx := t.Context()

		_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
			Name:                                   aws.String("dom"),
			WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
		})
		require.NoError(t, err)

		for _, name := range []string{"a-activity", "z-activity"} {
			_, registerErr := client.RegisterActivityType(ctx, &swfsdk.RegisterActivityTypeInput{
				Domain:  aws.String("dom"),
				Name:    aws.String(name),
				Version: aws.String("1.0"),
			})
			require.NoError(t, registerErr)
		}

		out, err := client.ListActivityTypes(ctx, &swfsdk.ListActivityTypesInput{
			Domain:             aws.String("dom"),
			RegistrationStatus: swftypes.RegistrationStatusRegistered,
			ReverseOrder:       true,
		})
		require.NoError(t, err)
		require.Len(t, out.TypeInfos, 2)
		require.Equal(t, "z-activity", aws.ToString(out.TypeInfos[0].ActivityType.Name),
			"ReverseOrder must sort descending")
		require.Equal(t, "a-activity", aws.ToString(out.TypeInfos[1].ActivityType.Name))
	})

	t.Run("workflow types", func(t *testing.T) {
		t.Parallel()

		backend := swf.NewInMemoryBackend()
		client := newTestSWFSDKClient(t, swf.NewHandler(backend))
		ctx := t.Context()

		_, err := client.RegisterDomain(ctx, &swfsdk.RegisterDomainInput{
			Name:                                   aws.String("dom"),
			WorkflowExecutionRetentionPeriodInDays: aws.String("1"),
		})
		require.NoError(t, err)

		for _, name := range []string{"a-workflow", "z-workflow"} {
			_, registerErr := client.RegisterWorkflowType(ctx, &swfsdk.RegisterWorkflowTypeInput{
				Domain:  aws.String("dom"),
				Name:    aws.String(name),
				Version: aws.String("1.0"),
			})
			require.NoError(t, registerErr)
		}

		out, err := client.ListWorkflowTypes(ctx, &swfsdk.ListWorkflowTypesInput{
			Domain:             aws.String("dom"),
			RegistrationStatus: swftypes.RegistrationStatusRegistered,
			ReverseOrder:       true,
		})
		require.NoError(t, err)
		require.Len(t, out.TypeInfos, 2)
		require.Equal(t, "z-workflow", aws.ToString(out.TypeInfos[0].WorkflowType.Name),
			"ReverseOrder must sort descending")
		require.Equal(t, "a-workflow", aws.ToString(out.TypeInfos[1].WorkflowType.Name))
	})
}
