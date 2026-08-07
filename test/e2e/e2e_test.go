//go:build e2e

package e2e_test

import (
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/internal/teststack"
)

var (
	pw      *playwright.Playwright
	browser playwright.Browser
)

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

func newStack(t *testing.T) *teststack.Stack {
	t.Helper()

	return teststack.New(t)
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

// waitForSPA waits for the SvelteKit SPA to fully load after navigation.
func waitForSPA(t *testing.T, page playwright.Page) {
	t.Helper()
	// Using LoadStateLoad instead of Networkidle because streaming pages (Metrics, Console)
	// never truly reach network idle state.
	require.NoError(t, page.WaitForLoadState(playwright.PageWaitForLoadStateOptions{
		State:   playwright.LoadStateLoad,
		Timeout: playwright.Float(10000),
	}))
}

func TestE2E_DynamoDB_CreateTable(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_DynamoDB_CreateTable")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)
	waitForSPA(t, page)

	// The dashboard now defaults to Region: All, which renders a flat,
	// cross-region, read/open-only table list with no #table-{name} ids,
	// search, pagination, or purge -- all single-region features. Pin to a
	// concrete region so this create/list flow exercises the single-region
	// view it was written against.
	switchRegion(t, page, "us-east-1")

	// Open create modal
	err = page.Click("#create-table-btn")
	require.NoError(t, err)

	modal := page.Locator("[role='dialog']")
	err = modal.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Fill form
	require.NoError(t, page.Fill("#tableName", "TestTable"))
	require.NoError(t, page.Fill("#partitionKey", "pk"))

	// Submit
	require.NoError(t, page.Click("#confirm-create-table-btn"))

	// Verify table appears in list
	tableCard := page.Locator("#table-TestTable")
	err = tableCard.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Verify in backend
	_, err = stack.DDBHandler.Backend.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	assert.NoError(t, err)
}

func TestE2E_DynamoDB_DeleteTable(t *testing.T) {
	stack := newStack(t)
	stack.CreateDDBTable(t, "Movies")

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_DynamoDB_DeleteTable")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Region: All's cross-region list has no #table-{name} ids -- see the
	// comment in TestE2E_DynamoDB_CreateTable.
	switchRegion(t, page, "us-east-1")

	// Wait for table card and click it to open detail
	tableCard := page.Locator("#table-Movies")
	err = tableCard.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
	err = tableCard.First().Click()
	require.NoError(t, err)

	// Wait for detail view with Delete Table button
	deleteBtn := page.Locator("button:has-text('Delete Table')")
	err = deleteBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	err = deleteBtn.Click()
	require.NoError(t, err)
	confirmDialog := page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Delete')").Click()
	require.NoError(t, err)

	// Should return to table list after deletion
	createBtn := page.Locator("#create-table-btn")
	require.Eventually(t, func() bool {
		visible, _ := createBtn.IsVisible()

		return visible
	}, 5*time.Second, 200*time.Millisecond)

	// Verify deleted in backend
	_, err = stack.DDBHandler.Backend.DescribeTable(t.Context(), &dynamodb.DescribeTableInput{
		TableName: aws.String("Movies"),
	})
	assert.Error(t, err)
}

func TestE2E_S3_CreateBucket(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_S3_CreateBucket")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/s3")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Open create modal
	err = page.Click("#create-bucket-btn")
	require.NoError(t, err)

	modal := page.Locator("[role='dialog']")
	err = modal.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	require.NoError(t, page.Fill("#bucketName", "test-bucket"))
	require.NoError(t, page.Click("#confirm-create-bucket-btn"))

	// Verify bucket appears
	bucketCard := page.Locator("#bucket-test-bucket")
	err = bucketCard.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Verify in backend
	output, err := stack.S3Backend.ListBuckets(t.Context(), &s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Len(t, output.Buckets, 1)
	assert.Equal(t, "test-bucket", *output.Buckets[0].Name)
}

func TestE2E_S3_DeleteBucket(t *testing.T) {
	stack := newStack(t)
	stack.CreateS3Bucket(t, "trash-bucket")

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_S3_DeleteBucket")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/s3")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Wait for bucket to appear
	bucketCard := page.Locator("#bucket-trash-bucket")
	err = bucketCard.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Click the Delete button on the bucket card
	err = page.Locator("button:has-text('Delete')").First().Click(
		playwright.LocatorClickOptions{Force: playwright.Bool(true)},
	)
	require.NoError(t, err)
	confirmDialog := page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Delete')").Click()
	require.NoError(t, err)

	// Verify bucket disappears
	require.Eventually(t, func() bool {
		visible, _ := page.Locator("text=trash-bucket").First().IsVisible()

		return !visible
	}, 5*time.Second, 200*time.Millisecond)
}

func TestE2E_S3_UploadFile(t *testing.T) {
	stack := newStack(t)
	stack.CreateS3Bucket(t, "upload-bucket")

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_S3_UploadFile")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/s3")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Open bucket detail
	bucketCard := page.Locator("#bucket-upload-bucket")
	err = bucketCard.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
	err = bucketCard.First().Click()
	require.NoError(t, err)

	// Wait for detail view
	uploadBtn := page.Locator("#upload-file-btn")
	err = uploadBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Open upload modal
	err = uploadBtn.Click()
	require.NoError(t, err)

	modal := page.Locator("[role='dialog']")
	err = modal.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Set file
	err = page.SetInputFiles("#file_input", []playwright.InputFile{
		{
			Name:     "test-upload.txt",
			MimeType: "text/plain",
			Buffer:   []byte("hello gopherstack"),
		},
	})
	require.NoError(t, err)

	// Submit
	require.NoError(t, page.Click("[role='dialog'] button:has-text('Upload')"))

	// Verify file in backend
	require.Eventually(t, func() bool {
		_, e := stack.S3Backend.GetObject(t.Context(), &s3.GetObjectInput{
			Bucket: aws.String("upload-bucket"),
			Key:    aws.String("test-upload.txt"),
		})

		return e == nil
	}, 5*time.Second, 200*time.Millisecond)
}

func TestE2E_DynamoDB_ItemCRUD(t *testing.T) {
	stack := newStack(t)
	stack.CreateDDBTable(t, "Items")

	server := httptest.NewServer(stack.Echo)
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

	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Region: All's cross-region list has no #table-{name} ids -- see the
	// comment in TestE2E_DynamoDB_CreateTable.
	switchRegion(t, page, "us-east-1")

	// Open the table detail
	tableCard := page.Locator("#table-Items")
	err = tableCard.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
	err = tableCard.First().Click()
	require.NoError(t, err)

	// Wait for detail view
	newItemBtn := page.Locator("button:has-text('New Item')")
	err = newItemBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Create a new item
	err = newItemBtn.Click()
	require.NoError(t, err)

	modal := page.Locator("[role='dialog']")
	err = modal.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	require.NoError(t, page.Fill("#newItemJson", `{"id": "test-1", "name": "Gopher"}`))
	require.NoError(t, page.Click("[role='dialog'] button:has-text('Create')"))

	// Switch to Scan tab and execute scan
	err = page.Click("button:has-text('Scan')")
	require.NoError(t, err)

	err = page.Click("button:has-text('Execute Scan')")
	require.NoError(t, err)

	// Verify item appears in scan results
	itemRow := page.Locator("text=test-1")
	err = itemRow.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	nameCell := page.Locator("text=Gopher")
	err = nameCell.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(3000),
	})
	require.NoError(t, err)
}

func TestE2E_S3_FolderNavigation(t *testing.T) {
	stack := newStack(t)
	stack.CreateS3Bucket(t, "nav-bucket")

	_, err := stack.S3Backend.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("nav-bucket"),
		Key:    aws.String("logs/2024/app.log"),
		Body:   strings.NewReader("log content"),
	})
	require.NoError(t, err)
	_, err = stack.S3Backend.PutObject(t.Context(), &s3.PutObjectInput{
		Bucket: aws.String("nav-bucket"),
		Key:    aws.String("readme.md"),
		Body:   strings.NewReader("readme"),
	})
	require.NoError(t, err)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

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

	_, err = page.Goto(server.URL + "/dashboard/s3")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Open the bucket
	err = page.Locator("#bucket-nav-bucket").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
	err = page.Locator("#bucket-nav-bucket").First().Click()
	require.NoError(t, err)

	// Verify file browser shows root contents
	err = page.Locator("text=readme.md").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Navigate into logs/ folder
	logsFolder := page.Locator("text=logs/")
	err = logsFolder.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
	err = logsFolder.First().Click()
	require.NoError(t, err)

	// Drill into 2024/
	err = page.Locator("text=2024/").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
	err = page.Locator("text=2024/").First().Click()
	require.NoError(t, err)

	// Verify app.log appears
	err = page.Locator("text=app.log").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
}

func TestE2E_DynamoDB_Search(t *testing.T) {
	stack := newStack(t)
	stack.CreateDDBTable(t, "Alpha")
	stack.CreateDDBTable(t, "Beta")
	stack.CreateDDBTable(t, "Gamma")

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_DynamoDB_Search")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Region: All's cross-region list has no search box filtering and no
	// #table-{name} ids -- see the comment in TestE2E_DynamoDB_CreateTable.
	switchRegion(t, page, "us-east-1")

	// Wait for tables to load
	err = page.Locator("text=Alpha").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Search for Beta
	require.NoError(t, page.Fill("#table-search", "Beta"))

	// Verify only Beta is visible
	require.Eventually(t, func() bool {
		visible, _ := page.Locator("#table-Beta").First().IsVisible()
		alphaVisible, _ := page.Locator("#table-Alpha").First().IsVisible()

		return visible && !alphaVisible
	}, 3*time.Second, 200*time.Millisecond, "Search should filter to Beta only")
}

func TestE2E_S3_Search(t *testing.T) {
	stack := newStack(t)
	stack.CreateS3Bucket(t, "bucket-alpha")
	stack.CreateS3Bucket(t, "bucket-beta")
	stack.CreateS3Bucket(t, "bucket-gamma")

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_S3_Search")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/s3")
	require.NoError(t, err)
	waitForSPA(t, page)

	err = page.Locator("text=bucket-alpha").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Search for beta
	require.NoError(t, page.Fill("#bucket-search", "beta"))

	require.Eventually(t, func() bool {
		visible, _ := page.Locator("#bucket-bucket-beta").First().IsVisible()
		alphaVisible, _ := page.Locator("#bucket-bucket-alpha").First().IsVisible()

		return visible && !alphaVisible
	}, 3*time.Second, 200*time.Millisecond, "Search should filter to beta only")
}

func TestE2E_MetricsDashboard(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

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

	_, err = page.Goto(server.URL + "/dashboard")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Click metrics link using ID
	err = page.Locator("#nav-metrics").Click()
	require.NoError(t, err)

	// Verify header
	header := page.Locator("h1:has-text('Performance Metrics')")
	err = header.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Verify runtime metrics labels (Connect-RPC streaming populates these)
	// We might need to wait longer for the stream to start
	goroutinesLabel := page.Locator("text=Goroutines")
	err = goroutinesLabel.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(20000),
	})
	require.NoError(t, err)

	heapLabel := page.Locator("text=Heap Memory")
	err = heapLabel.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}

func TestE2E_Console(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_Console")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Click console link using ID
	err = page.Locator("#nav-console").Click()
	require.NoError(t, err)

	// Verify console header
	header := page.Locator("h1:has-text('Live API Console')")
	err = header.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Verify auto-refresh toggle exists
	autoToggle := page.Locator("[role='switch']")
	err = autoToggle.First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Verify clear button exists
	clearBtn := page.Locator("button:has-text('Clear')")
	require.NoError(t, clearBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	}))
}

func TestE2E_DynamoDB_Pagination(t *testing.T) {
	stack := newStack(t)

	const tableCount = 15
	for i := 1; i <= tableCount; i++ {
		name := fmt.Sprintf("page-test-%02d", i)
		stack.CreateDDBTable(t, name)
	}

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_DynamoDB_Pagination")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Region: All's cross-region list has no pagination -- see the comment
	// in TestE2E_DynamoDB_CreateTable.
	switchRegion(t, page, "us-east-1")

	// Wait for tables to load
	err = page.Locator("text=page-test-01").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Click next page
	nextBtn := page.Locator("button:has-text('Next')")
	err = nextBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(3000),
	})
	require.NoError(t, err)
	err = nextBtn.Click()
	require.NoError(t, err)

	// Verify remaining tables on page 2
	err = page.Locator("text=page-test-15").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
}

func TestE2E_S3_Pagination(t *testing.T) {
	stack := newStack(t)

	const bucketCount = 15
	for i := 1; i <= bucketCount; i++ {
		name := fmt.Sprintf("page-bucket-%02d", i)
		stack.CreateS3Bucket(t, name)
	}

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_S3_Pagination")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/s3")
	require.NoError(t, err)
	waitForSPA(t, page)

	err = page.Locator("text=page-bucket-01").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Navigate to next page
	nextBtn := page.Locator("button:has-text('Next')")
	err = nextBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(3000),
	})
	require.NoError(t, err)
	err = nextBtn.Click()
	require.NoError(t, err)

	// Verify remaining buckets on page 2
	err = page.Locator("text=page-bucket-15").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
}

func TestE2E_DynamoDB_PurgeAll(t *testing.T) {
	stack := newStack(t)
	stack.CreateDDBTable(t, "purge-one")
	stack.CreateDDBTable(t, "purge-two")

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_DynamoDB_PurgeAll")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)
	waitForSPA(t, page)

	// purgeAll() only iterates the single-region table list -- under
	// Region: All it silently does nothing. See the comment in
	// TestE2E_DynamoDB_CreateTable.
	switchRegion(t, page, "us-east-1")

	// Wait for tables
	err = page.Locator("text=purge-one").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	err = page.Click("#purge-all-btn")
	require.NoError(t, err)
	confirmDialog := page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Delete')").Click()
	require.NoError(t, err)

	// Wait for tables to disappear
	require.Eventually(t, func() bool {
		visible, _ := page.Locator("text=purge-one").First().IsVisible()

		return !visible
	}, 5*time.Second, 200*time.Millisecond)

	// Verify backend is empty
	tables, err := stack.DDBHandler.Backend.ListTables(t.Context(), &dynamodb.ListTablesInput{})
	require.NoError(t, err)
	assert.Empty(t, tables.TableNames)
}

func TestE2E_S3_PurgeAll(t *testing.T) {
	stack := newStack(t)
	stack.CreateS3Bucket(t, "purge-one")
	stack.CreateS3Bucket(t, "purge-two")

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_S3_PurgeAll")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/s3")
	require.NoError(t, err)
	waitForSPA(t, page)

	err = page.Locator("text=purge-one").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	err = page.Click("#purge-all-btn")
	require.NoError(t, err)
	confirmDialog := page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Delete')").Click()
	require.NoError(t, err)

	// Wait for buckets to be removed
	require.Eventually(t, func() bool {
		visible, _ := page.Locator("text=purge-one").First().IsVisible()

		return !visible
	}, 5*time.Second, 200*time.Millisecond)

	// Verify backend is empty
	output, err := stack.S3Backend.ListBuckets(t.Context(), &s3.ListBucketsInput{})
	require.NoError(t, err)
	assert.Empty(t, output.Buckets)
}

func TestE2E_ThemeSelector(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_ThemeSelector")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Default theme should be light (no dark class)
	hasDark, err := page.Evaluate("() => document.documentElement.classList.contains('dark')")
	require.NoError(t, err)
	assert.False(t, hasDark.(bool), "Should start with light theme")

	// Open theme dropdown
	themeBtn := page.Locator("#theme-selector")
	err = themeBtn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
	err = themeBtn.Click()
	require.NoError(t, err)

	// Select Dark theme
	err = page.Click("button[data-theme='dark']")
	require.NoError(t, err)

	hasDark, err = page.Evaluate("() => document.documentElement.classList.contains('dark')")
	require.NoError(t, err)
	assert.True(t, hasDark.(bool), "Dark theme should add dark class")

	hasThemeDark, err := page.Evaluate("() => document.documentElement.classList.contains('theme-dark')")
	require.NoError(t, err)
	assert.True(t, hasThemeDark.(bool), "Dark theme should add theme-dark class")

	// Select Ocean theme
	err = themeBtn.Click()
	require.NoError(t, err)
	err = page.Click("button[data-theme='ocean']")
	require.NoError(t, err)

	hasThemeOcean, err := page.Evaluate("() => document.documentElement.classList.contains('theme-ocean')")
	require.NoError(t, err)
	assert.True(t, hasThemeOcean.(bool), "Ocean theme should add theme-ocean class")

	// Select GitHub theme
	err = themeBtn.Click()
	require.NoError(t, err)
	err = page.Click("button[data-theme='github']")
	require.NoError(t, err)

	hasThemeGithub, err := page.Evaluate("() => document.documentElement.classList.contains('theme-github')")
	require.NoError(t, err)
	assert.True(t, hasThemeGithub.(bool), "GitHub theme should add theme-github class")

	hasDark, err = page.Evaluate("() => document.documentElement.classList.contains('dark')")
	require.NoError(t, err)
	assert.True(t, hasDark.(bool), "GitHub Dark is a dark theme")

	// Select GitHub Light theme
	err = themeBtn.Click()
	require.NoError(t, err)
	err = page.Click("button[data-theme='github-light']")
	require.NoError(t, err)

	hasThemeGithubLight, err := page.Evaluate("() => document.documentElement.classList.contains('theme-github-light')")
	require.NoError(t, err)
	assert.True(t, hasThemeGithubLight.(bool), "GitHub Light theme should add theme-github-light class")

	hasDark, err = page.Evaluate("() => document.documentElement.classList.contains('dark')")
	require.NoError(t, err)
	assert.False(t, hasDark.(bool), "GitHub Light is a light theme, no dark class")

	// Select Light theme
	err = themeBtn.Click()
	require.NoError(t, err)
	err = page.Click("button[data-theme='light']")
	require.NoError(t, err)

	hasThemeLight, err := page.Evaluate("() => document.documentElement.classList.contains('theme-light')")
	require.NoError(t, err)
	assert.True(t, hasThemeLight.(bool), "Light theme should add theme-light class")
}

func TestE2E_SidebarNavigation(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_SidebarNavigation")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Verify sidebar is present
	sidebar := page.Locator("#sidebar")
	err = sidebar.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: aws.Float64(5000),
	})
	require.NoError(t, err)

	// Click DynamoDB in sidebar using its ID
	err = page.Click("#nav-dynamodb")
	require.NoError(t, err)

	// Should navigate to DynamoDB page
	err = page.Locator("text=Create Table").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Navigate to S3 via sidebar using its ID
	err = page.Click("#nav-s3")
	require.NoError(t, err)

	err = page.Locator("text=Create Bucket").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}
