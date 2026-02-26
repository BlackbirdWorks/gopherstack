//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestRoute53Dashboard verifies the Route 53 dashboard UI: list zones page renders.
func TestRoute53Dashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestRoute53Dashboard")
		}
	}()

	// Navigate to the Route 53 dashboard page.
	_, err = page.Goto(server.URL + "/dashboard/route53")
	require.NoError(t, err)

	// Wait for the Route 53 page header to appear.
	err = page.Locator("h1:has-text('Route 53')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Verify the "Create Hosted Zone" button is present.
	btn := page.Locator("button:has-text('Create Hosted Zone')")
	err = btn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
