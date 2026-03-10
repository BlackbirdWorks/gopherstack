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

// TestAPIGatewayManagementAPIDashboard verifies the API Gateway Management API dashboard renders correctly.
func TestAPIGatewayManagementAPIDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestAPIGatewayManagementAPIDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/apigatewaymanagementapi")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('API Gateway Management API')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "API Gateway Management API")
	assert.Contains(t, content, "PostToConnection")
	assert.Contains(t, content, "GetConnection")
	assert.Contains(t, content, "DeleteConnection")
	assert.Contains(t, content, "Supported Operations")
}

// TestAPIGatewayManagementAPIDashboard_CreateConnection verifies creating a simulated connection via the dashboard.
func TestAPIGatewayManagementAPIDashboard_CreateConnection(t *testing.T) {
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
			saveScreenshot(t, page, "TestAPIGatewayManagementAPIDashboard_CreateConnection")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/apigatewaymanagementapi")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('API Gateway Management API')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create connection modal.
	err = page.Locator("button:has-text('+ Simulate Connection')").Click()
	require.NoError(t, err)

	// Fill in the connection ID.
	err = page.Locator("input[name='connectionId']").Fill("e2e-conn-001")
	require.NoError(t, err)

	// Submit the form.
	err = page.Locator("button[type='submit']:has-text('Create')").Last().Click()
	require.NoError(t, err)

	// Wait for the connection row to appear in the table after the form submit and redirect.
	connRow := page.Locator("td:has-text('e2e-conn-001')")
	err = connRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-conn-001")
}

// TestAPIGatewayManagementAPIDashboard_Snippet verifies the code snippet renders.
func TestAPIGatewayManagementAPIDashboard_Snippet(t *testing.T) {
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
			saveScreenshot(t, page, "TestAPIGatewayManagementAPIDashboard_Snippet")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/apigatewaymanagementapi")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('API Gateway Management API')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "apigatewaymanagementapi")
}
