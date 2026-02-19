//go:build e2e

package e2e_test

import (
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/labstack/echo/v5"
	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"Gopherstack/dashboard"
	ddbbackend "Gopherstack/dynamodb"
	"Gopherstack/pkgs/service"
	s3backend "Gopherstack/s3"
)

var pw *playwright.Playwright
var browser playwright.Browser

func TestMain(m *testing.M) {
	// Install Playwright if not already present
	_ = playwright.Install()

	var err error
	pw, err = playwright.Run()
	if err != nil {
		log.Fatalf("could not start playwright: %v", err)
	}

	browser, err = pw.Chromium.Launch(playwright.BrowserTypeLaunchOptions{
		Headless: playwright.Bool(true),
	})
	if err != nil {
		log.Fatalf("could not launch browser: %v", err)
	}

	code := m.Run()

	browser.Close()
	pw.Stop()

	os.Exit(code)
}

type integrationStack struct {
	handler    http.Handler
	s3Backend  *s3backend.InMemoryBackend
	ddbHandler *ddbbackend.DynamoDBHandler
	s3Client   *s3.Client
	dyClient   *dynamodb.Client
}

func newIntegrationStack(t *testing.T) *integrationStack {
	t.Helper()

	s3Bk := s3backend.NewInMemoryBackend(nil)
	s3Hndlr := s3backend.NewHandler(s3Bk, slog.Default())
	ddbHndlr := ddbbackend.NewHandler(slog.Default())

	// Create main Echo instance for full stack
	e := echo.New()

	// Setup Registry/Router like in main.go
	registry := service.NewRegistry(slog.Default())
	_ = registry.Register(ddbHndlr, ddbHndlr)

	// Dashboard needs clients that talk back to this same Echo instance
	inMemClient := &dashboard.InMemClient{Handler: e}

	cfg, err := config.LoadDefaultConfig(t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("dummy", "dummy", ""),
		),
		config.WithHTTPClient(inMemClient),
	)
	require.NoError(t, err)

	ddbClient := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String("http://local")
	})
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true
		o.BaseEndpoint = aws.String("http://local")
	})

	dashHndlr := dashboard.NewHandler(ddbClient, s3Client, ddbHndlr, s3Hndlr, slog.Default())
	_ = registry.Register(dashHndlr, dashHndlr)
	_ = registry.Register(s3Hndlr, s3Hndlr)

	router := service.NewServiceRouter(registry)
	e.Pre(func(_ echo.HandlerFunc) echo.HandlerFunc {
		return router.RouteHandler()
	})

	return &integrationStack{
		handler:    e,
		s3Backend:  s3Bk,
		ddbHandler: ddbHndlr,
		s3Client:   s3Client,
		dyClient:   ddbClient,
	}
}

func newDDBTable(t *testing.T, stack *integrationStack, tableName string) {
	t.Helper()

	_, err := stack.ddbHandler.DB.CreateTable(t.Context(), &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		ProvisionedThroughput: &ddbtypes.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)
}

func newS3Bucket(t *testing.T, stack *integrationStack, bucketName string) {
	t.Helper()

	_, err := stack.s3Backend.CreateBucket(
		t.Context(), &s3.CreateBucketInput{Bucket: aws.String(bucketName)},
	)
	require.NoError(t, err)
}

func TestE2E_CustomModal_ConfirmDelete(t *testing.T) {
	stack := newIntegrationStack(t)
	newDDBTable(t, stack, "Movies")

	server := httptest.NewServer(stack.handler)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	_, err = page.Goto(server.URL + "/dashboard/dynamodb/table/Movies")
	require.NoError(t, err)

	err = page.Click("button:has-text('Delete Table')")
	require.NoError(t, err)

	modal := page.Locator("#global_confirm_modal")
	err = modal.WaitFor(playwright.LocatorWaitForOptions{
		State: playwright.WaitForSelectorStateVisible,
	})
	require.NoError(t, err)

	message := page.Locator("#global_confirm_message")
	content, err := message.TextContent()
	require.NoError(t, err)
	assert.Contains(t, content, "Are you sure you want to delete this table?")

	err = page.Click("#global_confirm_cancel")
	require.NoError(t, err)

	_, err = page.WaitForFunction("() => !document.getElementById('global_confirm_modal').hasAttribute('open')", nil, playwright.PageWaitForFunctionOptions{
		Timeout: playwright.Float(5000),
	})
	require.NoError(t, err, "Modal should close after clicking cancel")

	err = page.Click("button:has-text('Delete Table')")
	require.NoError(t, err)
	err = page.Click("#global_confirm_proceed")
	require.NoError(t, err)

	err = page.WaitForURL(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)

	assert.Equal(t, server.URL+"/dashboard/dynamodb", page.URL())
}

func TestE2E_S3_ConfirmDeleteBucket(t *testing.T) {
	stack := newIntegrationStack(t)
	newS3Bucket(t, stack, "trash-bucket")

	server := httptest.NewServer(stack.handler)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	_, err = page.Goto(server.URL + "/dashboard/s3")
	require.NoError(t, err)

	bucketCard := page.Locator(".card:has-text('trash-bucket')")
	err = bucketCard.WaitFor(playwright.LocatorWaitForOptions{State: playwright.WaitForSelectorStateVisible})
	require.NoError(t, err)

	err = bucketCard.Locator("button.btn-error").Click()
	require.NoError(t, err)

	modal := page.Locator("#global_confirm_modal")
	err = modal.WaitFor(playwright.LocatorWaitForOptions{State: playwright.WaitForSelectorStateVisible})
	require.NoError(t, err)

	txt, err := page.Locator("#global_confirm_message").TextContent()
	require.NoError(t, err)
	assert.Contains(t, txt, "Are you sure you want to delete bucket 'trash-bucket'?")

	err = page.Click("#global_confirm_proceed")
	require.NoError(t, err)

	err = bucketCard.WaitFor(playwright.LocatorWaitForOptions{State: playwright.WaitForSelectorStateHidden})
	require.NoError(t, err)
}

func TestE2E_DynamoDB_CreateTableCompleteFlow(t *testing.T) {
	stack := newIntegrationStack(t)

	server := httptest.NewServer(stack.handler)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	// 1. Go to DynamoDB Index
	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)

	// 2. Click Create Table button
	err = page.Click("button:has-text('Create Table')")
	require.NoError(t, err)

	// 3. Fill and submit form
	require.NoError(t, page.Fill("input[name='tableName']", "TestTable"))
	require.NoError(t, page.Fill("input[name='partitionKey']", "pk"))
	_, err = page.SelectOption("select[name='partitionKeyType']", playwright.SelectOptionValues{Values: &[]string{"S"}})
	require.NoError(t, err)
	require.NoError(t, page.Click("button[type='submit']:has-text('Create')"))

	// 4. Verify table appears in list
	tableCard := page.Locator(".card:has-text('TestTable')")
	err = tableCard.WaitFor(playwright.LocatorWaitForOptions{State: playwright.WaitForSelectorStateVisible})
	require.NoError(t, err)

	// 5. Verify table is actually created in backend
	_, err = stack.ddbHandler.DB.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	assert.NoError(t, err)
}

func TestE2E_S3_UploadFileFlow(t *testing.T) {
	stack := newIntegrationStack(t)
	newS3Bucket(t, stack, "upload-bucket")

	server := httptest.NewServer(stack.handler)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	// 1. Go to bucket detail
	_, err = page.Goto(server.URL + "/dashboard/s3/bucket/upload-bucket")
	require.NoError(t, err)

	// 2. Prepare a dummy file to upload
	tmpFile := "test-upload.txt"
	require.NoError(t, os.WriteFile(tmpFile, []byte("hello gopherstack"), 0o644))
	defer os.Remove(tmpFile)

	// 3. Open upload modal
	err = page.Click("button:has-text('Upload File')")
	require.NoError(t, err)

	// 4. Set file input
	err = page.SetInputFiles("input[name='file']", []playwright.InputFile{
		{
			Name:     "test-upload.txt",
			MimeType: "text/plain",
			Buffer:   []byte("hello gopherstack"),
		},
	})
	require.NoError(t, err)

	// 5. Submit
	err = page.Click("button[type='submit']:has-text('Upload')")
	require.NoError(t, err)

	// 6. Verify file appears in tree
	fileRow := page.Locator("#file-tree:has-text('test-upload.txt')")
	err = fileRow.WaitFor(playwright.LocatorWaitForOptions{State: playwright.WaitForSelectorStateVisible})
	require.NoError(t, err)

	// 7. Verify object exists in backend
	_, err = stack.s3Backend.GetObject(t.Context(), &s3.GetObjectInput{
		Bucket: aws.String("upload-bucket"),
		Key:    aws.String("test-upload.txt"),
	})
	assert.NoError(t, err)
}
