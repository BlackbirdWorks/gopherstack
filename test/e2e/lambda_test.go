//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	lambdabackend "github.com/blackbirdworks/gopherstack/lambda"
)

// TestE2E_LambdaDashboard_EmptyState verifies that the Lambda dashboard renders
// the function list page correctly when no functions exist.
func TestE2E_LambdaDashboard_EmptyState(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_LambdaDashboard_EmptyState")
		}
	}()

	// Navigate to the Lambda dashboard.
	_, err = page.Goto(server.URL + "/dashboard/lambda")
	require.NoError(t, err)

	// The page should render a heading containing "Lambda".
	err = page.Locator("h1:has-text('Lambda')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// No functions exist, so the page should say "No Lambda functions" or show an empty table.
	body, err := page.TextContent("body")
	require.NoError(t, err)
	assert.Contains(t, body, "Lambda", "expected 'Lambda' on the page")
}

// TestE2E_LambdaDashboard_FunctionList verifies that the Lambda dashboard shows a pre-seeded
// function and allows navigating to the function detail page.
func TestE2E_LambdaDashboard_FunctionList(t *testing.T) {
	stack := newStack(t)

	// Pre-seed a Lambda function in the backend so the dashboard can display it.
	require.NoError(t, stack.LambdaHandler.Backend.CreateFunction(&lambdabackend.FunctionConfiguration{
		FunctionName: "e2e-test-fn",
		PackageType:  lambdabackend.PackageTypeImage,
		ImageURI:     "my-registry/my-image:v1",
		Runtime:      "",
		Handler:      "",
		MemorySize:   128, //nolint:mnd // 128 MB default
		Timeout:      15,  //nolint:mnd // 15s default
	}))

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_LambdaDashboard_FunctionList")
		}
	}()

	// Navigate to the Lambda dashboard.
	_, err = page.Goto(server.URL + "/dashboard/lambda")
	require.NoError(t, err)

	// The function name should appear in the list.
	err = page.Locator("text=e2e-test-fn").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Click the function to navigate to the detail page.
	err = page.Click("a:has-text('e2e-test-fn')")
	require.NoError(t, err)

	err = page.WaitForLoadState(playwright.PageWaitForLoadStateOptions{
		State: playwright.LoadStateNetworkidle,
	})
	require.NoError(t, err)

	// The detail page should show the function name.
	body, err := page.TextContent("body")
	require.NoError(t, err)
	assert.Contains(t, body, "e2e-test-fn")
}

// TestE2E_LambdaDashboard_FunctionDetail_ZipFunction verifies that the detail page for a
// Zip function shows runtime and handler fields.
func TestE2E_LambdaDashboard_FunctionDetail_ZipFunction(t *testing.T) {
	stack := newStack(t)

	// Pre-seed a Zip Lambda function.
	require.NoError(t, stack.LambdaHandler.Backend.CreateFunction(&lambdabackend.FunctionConfiguration{
		FunctionName: "zip-detail-fn",
		PackageType:  lambdabackend.PackageTypeZip,
		Runtime:      "python3.12",
		Handler:      "index.handler",
		MemorySize:   256, //nolint:mnd // 256 MB
		Timeout:      30,  //nolint:mnd // 30s
	}))

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
			saveScreenshot(t, page, "TestE2E_LambdaDashboard_FunctionDetail_ZipFunction")
		}
	}()

	// Navigate directly to the function detail page.
	_, err = page.Goto(server.URL + "/dashboard/lambda/function?name=zip-detail-fn")
	require.NoError(t, err)

	err = page.Locator("text=zip-detail-fn").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	body, err := page.TextContent("body")
	require.NoError(t, err)
	assert.Contains(t, body, "python3.12", "expected runtime on detail page")
	assert.Contains(t, body, "index.handler", "expected handler on detail page")
	assert.Contains(t, body, "Zip", "expected PackageType=Zip on detail page")
}

// TestE2E_LambdaDashboard_NavLink verifies the Lambda nav item appears in the sidebar.
func TestE2E_LambdaDashboard_NavLink(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)

	// The Lambda nav link should be visible in the sidebar.
	lambdaLink, err := page.QuerySelector("a[href='/dashboard/lambda']")
	require.NoError(t, err)
	require.NotNil(t, lambdaLink, "Lambda nav link not found in sidebar")

	err = lambdaLink.Click()
	require.NoError(t, err)

	err = page.WaitForURL("**/dashboard/lambda", playwright.PageWaitForURLOptions{
		Timeout: playwright.Float(5000),
	})
	require.NoError(t, err)

	body, err := page.TextContent("body")
	require.NoError(t, err)
	assert.Contains(t, body, "Lambda")
}
