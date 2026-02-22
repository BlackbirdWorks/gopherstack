//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestSSMDashboard UI behavior.
func TestSSMDashboard(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_SSMDashboard")
		}
	}()

	// Navigate to the Dashboard SSM page
	_, err = page.Goto(server.URL + "/dashboard/ssm")
	require.NoError(t, err)

	// Wait for layout and SSM table
	err = page.Locator("h1:has-text('SSM Parameter Store')").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)})
	require.NoError(t, err)

	// Step 1: Create a Parameter via HTMX Modal
	err = page.Click("button:has-text('Create parameter')")
	require.NoError(t, err)

	modal := page.Locator("#modal-container")

	// Fill the PutParameter form (Playwright waits for visibility natively)
	require.NoError(t, page.Fill("#name", "test-database-password"))

	_, err = page.SelectOption("#type", playwright.SelectOptionValues{
		Values: &[]string{"SecureString"},
	})
	require.NoError(t, err)

	require.NoError(t, page.Fill("#value", "super_secret_e2e_pass"))
	require.NoError(t, page.Fill("#description", "E2E Test Password"))

	// Submit
	err = modal.Locator("button[type='submit']").Click()
	require.NoError(t, err)

	// HTMX redir adds latency
	time.Sleep(500 * time.Millisecond)

	// Verify the new parameter appears
	paramRow := page.Locator("td:has-text('test-database-password')").First()
	err = paramRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 2: Edit the Parameter
	err = page.Click("button:has-text('Edit')")
	require.NoError(t, err)

	require.NoError(t, page.Fill("#value", "new_super_secret"))

	err = modal.Locator("button[type='submit']").Click()
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Wait for parameter list back
	err = paramRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 3: Delete the Parameter via custom UI confirm dialog
	err = page.Click("button:has-text('Delete')")
	require.NoError(t, err)

	// Click the custom "Confirm" button in the modal
	err = page.Click("button:has-text('Confirm')")
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Verify the empty state text is rendered
	err = page.Locator("td:has-text('No parameters found')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
