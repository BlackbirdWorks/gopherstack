//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/chaos"
)

// TestChaosDashboard verifies the Chaos Engineering dashboard renders correctly.
func TestChaosDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestChaosDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/chaos")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Fault Rules")
	assert.Contains(t, content, "Network Effects")
	assert.Contains(t, content, "Activity Log")
	assert.Contains(t, content, "Add Fault")
}

// TestChaosDashboard_WithExistingFault verifies faults seeded via backend appear in the UI.
func TestChaosDashboard_WithExistingFault(t *testing.T) {
	stack := newStack(t)

	// Seed a fault directly in the store.
	stack.FaultStore.SetRules([]chaos.FaultRule{
		{
			Service:     "s3",
			Probability: 0.5,
			Error:       &chaos.FaultError{StatusCode: 503, Code: "ServiceUnavailable"},
		},
	})

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
			saveScreenshot(t, page, "TestChaosDashboard_WithExistingFault")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/chaos")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// The page should show the s3 fault.
	err = page.Locator("div:has-text('s3')").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(20000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "s3")
	assert.Contains(t, content, "50% probability")
}

// TestChaosDashboard_AddFault verifies the Add Fault form works end-to-end.
func TestChaosDashboard_AddFault(t *testing.T) {
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
			saveScreenshot(t, page, "TestChaosDashboard_AddFault")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/chaos")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Click "Add Fault" button to open the form.
	err = page.Locator("button:has-text('Add Fault')").Click()
	require.NoError(t, err)

	// Fill in the service name.
	err = page.Locator("input[placeholder='s3, dynamodb, lambda, etc']").Fill("dynamodb")
	require.NoError(t, err)

	// Submit the fault.
	err = page.Locator("button:has-text('Add')").First().Click()
	require.NoError(t, err)

	// Wait for the fault to appear.
	err = page.Locator("text=dynamodb").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "dynamodb")
}

// TestChaosDashboard_NoError verifies that the chaos page does not show an error on load.
func TestChaosDashboard_NoError(t *testing.T) {
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
			saveScreenshot(t, page, "TestChaosDashboard_NoError")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/chaos")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	// The page should NOT show the "Failed to load chaos state" error.
	assert.NotContains(t, content, "Failed to load chaos state")
}
