//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestServerlessRepoDashboard verifies the Serverless Application Repository dashboard UI renders application data.
func TestServerlessRepoDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ServerlessRepoHandler.Backend.CreateApplication(
		"e2e-test-app",
		"A test serverless application",
		"test-author",
		"https://github.com/example/repo",
		"1.0.0",
		map[string]string{"Environment": "test"},
	)
	require.NoError(t, err)

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
			saveScreenshot(t, page, "TestServerlessRepoDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/serverlessrepo")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Serverless Application Repository')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-app")
	assert.Contains(t, content, "+ Create Application")
}

// TestServerlessRepoDashboard_Empty verifies the Serverless Application Repository dashboard renders correctly with no applications.
func TestServerlessRepoDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestServerlessRepoDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/serverlessrepo")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Serverless Application Repository')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No applications found")
}
