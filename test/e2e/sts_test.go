//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestSTSDashboard verifies the STS dashboard UI.
func TestSTSDashboard(t *testing.T) {
	stack := newIntegrationStack(t)

	server := httptest.NewServer(stack.handler)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.s3Handler.Endpoint = u.Host
	}

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSTSDashboard")
		}
	}()

	// Navigate to the STS dashboard page.
	_, err = page.Goto(server.URL + "/dashboard/sts")
	require.NoError(t, err)

	// Wait for the page heading to appear.
	err = page.Locator("h1:has-text('STS Security Token Service')").WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Verify the mock account ID is shown.
	err = page.Locator("#sts-account:has-text('123456789012')").WaitFor(
		playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(60000),
		},
	)
	require.NoError(t, err)
}
