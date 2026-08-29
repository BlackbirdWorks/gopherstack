package personalize_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	personalizesdk "github.com/aws/aws-sdk-go-v2/service/personalize"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// TestListCampaigns_SolutionArnFilter proves ListCampaigns' SolutionArn
// filter matches real client input. Campaign.SolutionVersionArn is stored
// as SolutionArn + "/" + versionID (campaigns.go, solutions.go:208), but the
// backend's filter compared it for exact equality against the bare
// SolutionArn a real client sends -- that condition is never true, so the
// filter silently excluded every campaign instead of narrowing to one
// solution's campaigns.
func TestListCampaigns_SolutionArnFilter(t *testing.T) {
	t.Parallel()

	h := personalize.NewHandler(personalize.NewInMemoryBackend("000000000000", "us-east-1"))
	client := newTestPersonalizeClient(t, h)
	ctx := t.Context()

	dg, err := client.CreateDatasetGroup(ctx, &personalizesdk.CreateDatasetGroupInput{
		Name: aws.String("dg-a"),
	})
	require.NoError(t, err)

	solA, err := client.CreateSolution(ctx, &personalizesdk.CreateSolutionInput{
		Name:            aws.String("sol-a"),
		DatasetGroupArn: dg.DatasetGroupArn,
		RecipeArn:       aws.String("arn:aws:personalize:::recipe/aws-user-personalization"),
	})
	require.NoError(t, err)

	solB, err := client.CreateSolution(ctx, &personalizesdk.CreateSolutionInput{
		Name:            aws.String("sol-b"),
		DatasetGroupArn: dg.DatasetGroupArn,
		RecipeArn:       aws.String("arn:aws:personalize:::recipe/aws-user-personalization"),
	})
	require.NoError(t, err)

	svA, err := client.CreateSolutionVersion(ctx, &personalizesdk.CreateSolutionVersionInput{
		SolutionArn: solA.SolutionArn,
	})
	require.NoError(t, err)

	svB, err := client.CreateSolutionVersion(ctx, &personalizesdk.CreateSolutionVersionInput{
		SolutionArn: solB.SolutionArn,
	})
	require.NoError(t, err)

	campA, err := client.CreateCampaign(ctx, &personalizesdk.CreateCampaignInput{
		Name:               aws.String("camp-a"),
		SolutionVersionArn: svA.SolutionVersionArn,
	})
	require.NoError(t, err)

	_, err = client.CreateCampaign(ctx, &personalizesdk.CreateCampaignInput{
		Name:               aws.String("camp-b"),
		SolutionVersionArn: svB.SolutionVersionArn,
	})
	require.NoError(t, err)

	out, err := client.ListCampaigns(ctx, &personalizesdk.ListCampaignsInput{
		SolutionArn: solA.SolutionArn,
	})
	require.NoError(t, err)
	require.Len(t, out.Campaigns, 1)
	require.Equal(t, aws.ToString(campA.CampaignArn), aws.ToString(out.Campaigns[0].CampaignArn))
}
