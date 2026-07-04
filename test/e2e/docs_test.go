//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDocsDashboard verifies the Docs page renders with service list and GitHub link.
func TestDocsDashboard(t *testing.T) {
	stack := newStack(t)

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
			saveScreenshot(t, page, "TestDocsDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/docs")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Documentation")
	assert.Contains(t, content, "Supported Services")
	assert.Contains(t, content, "github.com/BlackbirdWorks/gopherstack")
	assert.Contains(t, content, "GitHub")
}

// TestDocsDashboard_ServiceLinks verifies that service links point to their dashboards.
func TestDocsDashboard_ServiceLinks(t *testing.T) {
	stack := newStack(t)

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
			saveScreenshot(t, page, "TestDocsDashboard_ServiceLinks")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/docs")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// There should be a link to the S3 dashboard.
	s3Link := page.Locator("a[href='/dashboard/s3']")
	count, err := s3Link.Count()
	require.NoError(t, err)
	assert.Greater(t, count, 0, "expected at least one link to the S3 dashboard")

	// There should be a link to the Lambda dashboard.
	lambdaLink := page.Locator("a[href='/dashboard/lambda']")
	count, err = lambdaLink.Count()
	require.NoError(t, err)
	assert.Greater(t, count, 0, "expected at least one link to the Lambda dashboard")
}

// TestDocsDashboard_StatsCards verifies the stats cards render.
func TestDocsDashboard_StatsCards(t *testing.T) {
	stack := newStack(t)

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
			saveScreenshot(t, page, "TestDocsDashboard_StatsCards")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/docs")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Services with Dashboard UI")
	assert.Contains(t, content, "Service Categories")
}

// TestDocsDashboard_NavDocsLink verifies the Docs link exists in the top navigation.
func TestDocsDashboard_NavDocsLink(t *testing.T) {
	stack := newStack(t)

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
			saveScreenshot(t, page, "TestDocsDashboard_NavDocsLink")
		}
	}()

	// Start from the home page.
	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)

	waitForSPA(t, page)

	// The top nav should have a Docs link.
	docsLink := page.Locator("#nav-docs").First()
	require.NoError(t, docsLink.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	}))
}
