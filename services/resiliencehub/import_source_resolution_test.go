package resiliencehub_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	resiliencehubsdk "github.com/aws/aws-sdk-go-v2/service/resiliencehub"
	"github.com/aws/aws-sdk-go-v2/service/resiliencehub/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	dynamodbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/resiliencehub"
)

type ddbSiblings struct {
	ddb service.Registerable
}

func (f *ddbSiblings) GetCloudFormationHandler() service.Registerable { return nil }
func (f *ddbSiblings) GetResourceGroupsHandler() service.Registerable { return nil }
func (f *ddbSiblings) GetEKSHandler() service.Registerable            { return nil }
func (f *ddbSiblings) GetEC2Handler() service.Registerable            { return nil }
func (f *ddbSiblings) GetRDSHandler() service.Registerable            { return nil }
func (f *ddbSiblings) GetDynamoDBHandler() service.Registerable       { return f.ddb }

func TestImportResourcesToDraftAppVersion_SourceResolution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		table      string
		wantStatus types.ResourceImportStatusType
		wantCount  int
	}{
		{name: "existing table", table: "orders", wantStatus: types.ResourceImportStatusTypeSuccess, wantCount: 1},
		{name: "missing table", table: "ghost", wantStatus: types.ResourceImportStatusTypeFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodbbackend.NewInMemoryDB()
			_, err := db.CreateTable(t.Context(), &ddbsdk.CreateTableInput{
				TableName:   aws.String("orders"),
				BillingMode: ddbtypes.BillingModePayPerRequest,
				KeySchema: []ddbtypes.KeySchemaElement{
					{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
				},
				AttributeDefinitions: []ddbtypes.AttributeDefinition{
					{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
				},
			})
			require.NoError(t, err)

			backend := resiliencehub.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
			t.Cleanup(backend.Close)
			backend.SetAppConfig(&ddbSiblings{ddb: dynamodbbackend.NewHandler(db)})

			client := newRoundTripClient(t, resiliencehub.NewHandler(backend))
			ctx := t.Context()

			app, err := client.CreateApp(ctx, &resiliencehubsdk.CreateAppInput{Name: aws.String("import-app")})
			require.NoError(t, err)

			_, err = client.ImportResourcesToDraftAppVersion(
				ctx,
				&resiliencehubsdk.ImportResourcesToDraftAppVersionInput{
					AppArn:     app.App.AppArn,
					SourceArns: []string{"arn:aws:dynamodb:us-east-1:000000000000:table/" + tt.table},
				},
			)
			require.NoError(t, err)

			var status *resiliencehubsdk.DescribeDraftAppVersionResourcesImportStatusOutput

			require.Eventually(t, func() bool {
				status, err = client.DescribeDraftAppVersionResourcesImportStatus(
					ctx, &resiliencehubsdk.DescribeDraftAppVersionResourcesImportStatusInput{AppArn: app.App.AppArn},
				)

				return err == nil && status.Status != types.ResourceImportStatusTypePending &&
					status.Status != types.ResourceImportStatusTypeInProgress
			}, defaultAsyncWait, defaultAsyncPoll)

			assert.Equal(t, tt.wantStatus, status.Status)

			res, err := client.ListAppVersionResources(ctx, &resiliencehubsdk.ListAppVersionResourcesInput{
				AppArn: app.App.AppArn, AppVersion: aws.String("draft"),
			})
			require.NoError(t, err)
			assert.Len(t, res.PhysicalResources, tt.wantCount)
		})
	}
}
