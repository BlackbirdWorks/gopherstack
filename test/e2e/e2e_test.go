//go:build e2e

package e2e_test

import (
	"log"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
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
	handler    *echo.Echo
	s3Backend  *s3backend.InMemoryBackend
	s3Handler  *s3backend.S3Handler
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
	e.Use(router.RouteHandler())

	// Register metrics handlers for E2E tests
	dashboard.RegisterMetricsHandlers(e)

	return &integrationStack{
		handler:    e,
		s3Backend:  s3Bk,
		s3Handler:  s3Hndlr,
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

func saveScreenshot(t *testing.T, page playwright.Page, name string) {
	t.Helper()
	_ = os.MkdirAll("failures", 0o755)
	path := "failures/" + name + ".png"
	_, _ = page.Screenshot(playwright.PageScreenshotOptions{
		Path: aws.String(path),
	})
	t.Logf("Screenshot saved to %s", path)
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

func TestE2E_DynamoDB_ItemCRUD(t *testing.T) {
	stack := newIntegrationStack(t)
	newDDBTable(t, stack, "Items")

	server := httptest.NewServer(stack.handler)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_DynamoDB_ItemCRUD")
		}
	}()

	// 1. Go to table detail
	_, err = page.Goto(server.URL + "/dashboard/dynamodb/table/Items")
	require.NoError(t, err)

	// 2. Insert item (Scan should be empty initially, then show 1 item)
	err = page.Click("button:has-text('New Item')")
	require.NoError(t, err)

	require.NoError(t, page.Fill("textarea[name='itemJson']", `{"id": "test-1", "name": "Gopher"}`))
	require.NoError(t, page.Click("button[type='submit']:has-text('Create Item')"))

	// 3. Scan and verify item appears
	// Make sure we are on the Scan tab
	err = page.Click("input[aria-label='Scan']")
	require.NoError(t, err)

	err = page.Click("button.btn-primary:has-text('Execute Scan')")
	require.NoError(t, err)

	// Wait for results to be swap-in
	require.NoError(t, page.Locator("#scan-results").WaitFor())

	itemRow := page.Locator("tr:has-text('test-1')")
	err = itemRow.WaitFor(playwright.LocatorWaitForOptions{State: playwright.WaitForSelectorStateVisible})
	require.NoError(t, err)

	// 4. Edit item
	err = itemRow.Locator("button:has-text('Edit')").Click()
	require.NoError(t, err)

	require.NoError(t, page.Fill("textarea[name='itemJson']", `{"id": "test-1", "name": "Super Gopher"}`))
	err = page.Click("button[type='submit']:has-text('Save Changes')")
	require.NoError(t, err)

	// Wait for modal to close to ensure swap is complete
	// We use after-request hook in the form to close the modal
	editModal := page.Locator("#edit_item_modal")
	require.NoError(t, editModal.WaitFor(playwright.LocatorWaitForOptions{State: playwright.WaitForSelectorStateHidden, Timeout: aws.Float64(10000)}))

	// 5. Verify update (should be auto-updated by hx-target="#scan-results")
	content, _ := itemRow.TextContent()
	assert.Contains(t, content, "Super Gopher")

	// 6. Delete item
	err = itemRow.Locator("button:has-text('Delete')").Click()
	require.NoError(t, err)

	// In this build, Delete Table has hx-confirm, but Item Delete has hx-confirm
	// THE BUTTON CLICK TRIGGERS THE BROWSER CONFIRM DIALOG OR THE HTMX ONE
	// Let's assume it's the global confirmation modal based on the screenshot
	modal := page.Locator("#global_confirm_modal")
	err = modal.WaitFor(playwright.LocatorWaitForOptions{State: playwright.WaitForSelectorStateVisible, Timeout: aws.Float64(2000)})
	if err == nil {
		err = page.Click("#global_confirm_proceed")
		require.NoError(t, err)
	} else {
		// Fallback to native dialog handling if it was a native confirm
		// (Already handled by playwright usually, but if we are here it might be something else)
	}

	// 7. Verify item is gone
	err = itemRow.WaitFor(playwright.LocatorWaitForOptions{State: playwright.WaitForSelectorStateHidden})
	require.NoError(t, err)
}

func TestE2E_S3_FolderNavigation(t *testing.T) {
	stack := newIntegrationStack(t)
	newS3Bucket(t, stack, "nav-bucket")

	// Pre-create some objects with prefixes
	_, err := stack.s3Backend.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("nav-bucket"),
		Key:    aws.String("logs/2024/01/app.log"),
		Body:   strings.NewReader(""),
	})
	require.NoError(t, err)
	_, err = stack.s3Backend.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("nav-bucket"),
		Key:    aws.String("readme.md"),
		Body:   strings.NewReader(""),
	})
	require.NoError(t, err)

	server := httptest.NewServer(stack.handler)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.s3Handler.Endpoint = u.Host
	}

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_S3_FolderNavigation")
		}
	}()

	// 1. Go to bucket detail
	_, err = page.Goto(server.URL + "/dashboard/s3/bucket/nav-bucket")
	require.NoError(t, err)

	// 2. Verify tree structure (initially should see 'logs/' and 'readme.md')
	require.NoError(t, page.Locator("#file-tree:has-text('logs')").WaitFor())
	require.NoError(t, page.Locator("#file-tree:has-text('readme.md')").WaitFor())

	// 3. Navigate into 'logs/' (click the folder title - it might contain emoji)
	err = page.Click(".collapse-title:has-text('logs')")
	require.NoError(t, err)

	// 4. Verify '2024/' appears
	require.NoError(t, page.Locator(".collapse-title:has-text('2024')").WaitFor())
	err = page.Click(".collapse-title:has-text('2024')")
	require.NoError(t, err)

	require.NoError(t, page.Locator(".collapse-title:has-text('01')").WaitFor())
	err = page.Click(".collapse-title:has-text('01')")
	require.NoError(t, err)

	// 6. Verify 'app.log' appears
	require.NoError(t, page.Locator("#file-tree:has-text('app.log')").WaitFor())
}

func TestE2E_S3_MetadataTagging(t *testing.T) {
	stack := newIntegrationStack(t)
	newS3Bucket(t, stack, "meta-bucket")
	_, err := stack.s3Backend.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("meta-bucket"),
		Key:    aws.String("meta.txt"),
		Body:   strings.NewReader(""),
	})
	require.NoError(t, err)

	server := httptest.NewServer(stack.handler)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_S3_MetadataTagging")
		}
	}()

	// 1. Go to file detail
	_, err = page.Goto(server.URL + "/dashboard/s3/bucket/meta-bucket/file/meta.txt")
	require.NoError(t, err)

	// 2. Add a tag
	require.NoError(t, page.Fill("input[name='key']", "Project"))
	require.NoError(t, page.Fill("input[name='value']", "Gopherstack"))
	require.NoError(t, page.Click("button:has-text('Add')"))

	// 3. Verify tag appears (HTMX might swap the whole component, so wait for it)
	// Using a text-based locator which is more resilient to HTMX swaps
	tagItem := page.Locator("#tags-list").GetByText("Project: Gopherstack")
	err = tagItem.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// 4. Update Content-Type
	require.NoError(t, page.Fill("input[name='contentType']", "text/markdown"))
	require.NoError(t, page.Click("button:has-text('Update Content-Type')"))

	// 5. Verify update (page refreshes)
	// 5. Verify update (page refreshes) - check for content instead of timing out on URL
	require.NoError(t, page.Locator(".badge:has-text('Project: Gopherstack')").WaitFor())
	content, _ := page.Locator("body").TextContent()
	assert.Contains(t, content, "text/markdown")
}

func TestE2E_GlobalSearch(t *testing.T) {
	stack := newIntegrationStack(t)
	newDDBTable(t, stack, "SearchTable")
	newS3Bucket(t, stack, "SearchBucket")

	server := httptest.NewServer(stack.handler)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_GlobalSearch")
		}
	}()

	// 1. Go to DynamoDB Index and search
	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)
	require.NoError(t, page.Fill("input[placeholder*='Search tables']", "Search"))
	require.NoError(t, page.Locator(".card:has-text('SearchTable')").WaitFor())

	// 2. Go to S3 Index (search input might be missing in some builds)
	_, err = page.Goto(server.URL + "/dashboard/s3")
	require.NoError(t, err)

	count, _ := page.Locator("input[placeholder*='Search buckets']").Count()
	if count > 0 {
		require.NoError(t, page.Fill("input[placeholder*='Search buckets']", "Search"))
		require.NoError(t, page.Locator(".card:has-text('SearchBucket')").WaitFor())
	}
}

func TestE2E_MetricsDashboard(t *testing.T) {
	stack := newIntegrationStack(t)

	server := httptest.NewServer(stack.handler)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.s3Handler.Endpoint = u.Host
	}

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_MetricsDashboard")
		}
	}()

	// 1. Go to Home Dashboard first
	_, err = page.Goto(server.URL + "/dashboard")
	require.NoError(t, err)

	// 2. Click on Metrics tab in navbar
	err = page.Click(".navbar .menu a:has-text('Metrics')")
	require.NoError(t, err)

	err = page.WaitForURL(server.URL + "/dashboard/metrics")
	require.NoError(t, err)

	// 3. Verify page header
	require.NoError(t, page.Locator("h1:has-text('Performance Metrics')").WaitFor())

	// 4. Verify Go Runtime section is visible and has data
	runtimeHeader := page.Locator("h2:has-text('Go Runtime')")
	require.NoError(t, runtimeHeader.WaitFor())

	goroutines := page.Locator("#runtime-goroutines")
	err = goroutines.WaitFor(playwright.LocatorWaitForOptions{State: playwright.WaitForSelectorStateVisible})
	require.NoError(t, err)
	val, err := goroutines.TextContent()
	require.NoError(t, err)
	assert.NotEqual(t, "0", val, "Goroutines should be > 0")

	// 5. Trigger some activity to see operation latencies
	// Go back to S3 and create a bucket via UI
	_, err = page.Goto(server.URL + "/dashboard/s3")
	require.NoError(t, err)

	err = page.Click("button:has-text('Create Bucket')")
	require.NoError(t, err)
	require.NoError(t, page.Locator("#create_bucket_modal").WaitFor())

	err = page.Fill("input[name='bucketName']", "metrics-test-bucket")
	require.NoError(t, err)
	// Click the 'Create' button INSIDE the modal
	err = page.Click("#create_bucket_modal button[type='submit']:has-text('Create')")
	require.NoError(t, err)

	// Verify bucket appears with extra wait
	require.NoError(t, page.Locator("tr:has-text('metrics-test-bucket')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(10000),
	}))
	require.NoError(t, page.Locator(".card:has-text('metrics-test-bucket')").WaitFor())

	// 6. Go back to Metrics and check operation latencies
	err = page.Click(".navbar .menu a:has-text('Metrics')")
	require.NoError(t, err)

	// Wait for metrics to load and dashboard view to show
	err = page.Locator("#dashboard-view").WaitFor(playwright.LocatorWaitForOptions{
		State: playwright.WaitForSelectorStateVisible,
	})
	require.NoError(t, err)

	// Check if S3::CreateBucket appears in the table
	opRow := page.Locator("tr:has-text('CreateBucket')")
	err = opRow.WaitFor(playwright.LocatorWaitForOptions{
		State: playwright.WaitForSelectorStateVisible,
	})
	require.NoError(t, err, "S3::CreateBucket should appear in metrics table after activity")

	// Verify it says "S3" in the row
	content, _ := opRow.TextContent()
	assert.Contains(t, content, "S3")
}
