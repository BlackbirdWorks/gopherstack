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

// TestCodeConnectionsDashboard verifies the CodeConnections dashboard UI renders connections.
func TestCodeConnectionsDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.CodeConnectionsHandler.Backend.CreateConnection("e2e-test-conn", "GitHub", nil)
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
			saveScreenshot(t, page, "TestCodeConnectionsDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codeconnections")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeConnections')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-conn")
}

// TestCodeConnectionsDashboard_Empty verifies the CodeConnections dashboard empty state renders correctly.
func TestCodeConnectionsDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCodeConnectionsDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/codeconnections")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('AWS CodeConnections')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "AWS CodeConnections")
}
