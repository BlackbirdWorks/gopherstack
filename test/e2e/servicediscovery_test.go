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

// TestServiceDiscoveryDashboard verifies the Service Discovery dashboard UI renders namespace data.
func TestServiceDiscoveryDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ServiceDiscoveryHandler.Backend.CreateHTTPNamespace("e2e-namespace", "e2e test namespace", nil)
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
			saveScreenshot(t, page, "TestServiceDiscoveryDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/servicediscovery")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Service Discovery')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-namespace")
	assert.Contains(t, content, "Create Namespace")
}

// TestServiceDiscoveryDashboard_Empty verifies the Service Discovery dashboard renders with no namespaces.
func TestServiceDiscoveryDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestServiceDiscoveryDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/servicediscovery")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Service Discovery')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No namespaces created yet")
}
