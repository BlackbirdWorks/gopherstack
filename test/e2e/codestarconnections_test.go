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

// TestCodeStarConnectionsDashboard verifies the CodeStar Connections dashboard renders connections.
func TestCodeStarConnectionsDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.CodeStarConnectionsHandler.Backend.CreateConnection("e2e-test-conn", "GitHub", "", nil)
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
			saveScreenshot(t, page, "TestCodeStarConnectionsDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codestarconnections")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeStar Connections')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-conn")
}

// TestCodeStarConnectionsDashboard_Empty verifies the CodeStar Connections dashboard renders with no resources.
func TestCodeStarConnectionsDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodeStarConnectionsDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codestarconnections")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeStar Connections')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "AWS CodeStar Connections")
}

// TestCodeStarConnectionsDashboard_CreateConnection verifies creating a connection via the dashboard.
func TestCodeStarConnectionsDashboard_CreateConnection(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodeStarConnectionsDashboard_CreateConnection")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codestarconnections")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeStar Connections')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('+ Connection')").Click()
	require.NoError(t, err)

	err = page.Locator("#conn-name").Fill("e2e-new-connection")
	require.NoError(t, err)

	err = page.Locator("button:has-text('Create')").Last().Click()
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeStar Connections')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-new-connection")
}

// TestCodeStarConnectionsDashboard_CreateHost verifies creating a host via the dashboard.
func TestCodeStarConnectionsDashboard_CreateHost(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodeStarConnectionsDashboard_CreateHost")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codestarconnections")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeStar Connections')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('+ Host')").Click()
	require.NoError(t, err)

	err = page.Locator("#host-name").Fill("e2e-new-host")
	require.NoError(t, err)

	err = page.Locator("#host-endpoint").Fill("https://github.example.com")
	require.NoError(t, err)

	err = page.Locator("button:has-text('Create')").Last().Click()
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeStar Connections')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-new-host")
}
