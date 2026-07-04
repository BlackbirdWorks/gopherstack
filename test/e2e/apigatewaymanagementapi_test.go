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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "API Gateway Management API")
	assert.Contains(t, content, "PostToConnection")
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open simulate connection modal.
	err = page.Locator("button:has-text('Simulate')").First().Click()
	require.NoError(t, err)

	// Fill in the connection ID — first input in the modal.
	err = page.Locator("input[placeholder='Connection ID']").Fill("e2e-conn-001")
	require.NoError(t, err)

	// Submit the form.
	err = page.Locator("button[type='submit']:has-text('Create')").Last().Click()
	require.NoError(t, err)

	// Wait for the new connection card to appear in the connections list.
	connRow := page.Locator("button:has-text('e2e-conn-001')").First()
	err = connRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-conn-001")
}
