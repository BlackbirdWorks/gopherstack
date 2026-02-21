//go:build e2e

package e2e_test

import (
	"log/slog"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	ssmsdk "github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/labstack/echo/v5"
	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	ddbbackend "github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	s3backend "github.com/blackbirdworks/gopherstack/s3"
	ssmbackend "github.com/blackbirdworks/gopherstack/ssm"
)

func TestE2E_DynamoDB_TTL(t *testing.T) {
	// Set up logger
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))

	s3Bk := s3backend.NewInMemoryBackend(nil)
	s3Hndlr := s3backend.NewHandler(s3Bk, logger)
	ddbBk := ddbbackend.NewInMemoryDB()
	ddbHndlr := ddbbackend.NewHandler(ddbBk, logger)
	ssmBk := ssmbackend.NewInMemoryBackend()
	ssmHndlr := ssmbackend.NewHandler(ssmBk, logger)

	e := echo.New()
	registry := service.NewRegistry(logger)
	_ = registry.Register(ddbHndlr)
	_ = registry.Register(s3Hndlr)
	_ = registry.Register(ssmHndlr)

	inMemClient := &dashboard.InMemClient{Handler: e}
	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithHTTPClient(inMemClient),
	)
	require.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = aws.String("http://local") })
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true; o.BaseEndpoint = aws.String("http://local") })
	ssmClient := ssmsdk.NewFromConfig(cfg, func(o *ssmsdk.Options) { o.BaseEndpoint = aws.String("http://local") })

	dashHndlr := dashboard.NewHandler(ddbClient, s3Client, ssmClient, ddbHndlr, s3Hndlr, ssmHndlr, logger)
	_ = registry.Register(dashHndlr)

	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	server := httptest.NewServer(e)
	defer server.Close()

	page, err := browser.NewPage()
	require.NoError(t, err)
	defer page.Close()

	tableName := "ttl-e2e-table"
	ctx := t.Context()

	// 1. Create table via SDK
	_, err = ddbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// 2. Navigate to table detail page
	_, err = page.Goto(server.URL + "/dashboard/dynamodb/table/" + tableName)
	require.NoError(t, err)

	// 3. Verify initial TTL status (DISABLED)
	require.NoError(t, page.Locator("text=TTL Status").WaitFor())
	require.NoError(t, page.Locator("span.badge", playwright.PageLocatorOptions{
		HasText: "DISABLED",
	}).WaitFor())

	// 4. Configure TTL via UI
	require.NoError(t, page.Fill("input[name='attributeName']", "ttl_attr"))
	require.NoError(t, page.Check("input[name='enabled']"))
	require.NoError(t, page.Click("button:has-text('Update TTL')"))

	// 5. Verify success toast and UI update
	require.NoError(t, page.Locator("text=TTL enabled successfully").WaitFor())
	require.NoError(t, page.Locator("span.badge-success", playwright.PageLocatorOptions{
		HasText: "ENABLED (ttl_attr)",
	}).WaitFor())

	// 6. Verify TTL status via SDK
	desc, err := ddbClient.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	require.Equal(t, types.TimeToLiveStatusEnabled, desc.TimeToLiveDescription.TimeToLiveStatus)
	require.Equal(t, "ttl_attr", *desc.TimeToLiveDescription.AttributeName)

	// 7. Disable TTL via UI
	require.NoError(t, page.Uncheck("input[name='enabled']"))
	require.NoError(t, page.Click("button:has-text('Update TTL')"))

	// 8. Verify success toast and UI update
	require.NoError(t, page.Locator("text=TTL disabled successfully").WaitFor())
	require.NoError(t, page.Locator("span.badge", playwright.PageLocatorOptions{
		HasText: "DISABLED",
	}).WaitFor())

	// 9. Verify TTL status via SDK again
	desc, err = ddbClient.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	require.Equal(t, types.TimeToLiveStatusDisabled, desc.TimeToLiveDescription.TimeToLiveStatus)
}
