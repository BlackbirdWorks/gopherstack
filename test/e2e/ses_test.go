//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestSESDashboard verifies the SES dashboard UI: inbox renders and shows sent emails.
func TestSESDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestSESDashboard")
		}
	}()

	// Navigate to the SES dashboard page.
	_, err = page.Goto(server.URL + "/dashboard/ses")
	require.NoError(t, err)

	// Wait for the SES Inbox header to appear.
	err = page.Locator("h1:has-text('SES Inbox')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Verify the "Verify Identity" button is present.
	btn := page.Locator("button:has-text('Verify Identity')")
	err = btn.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Send an email via SES so that it appears in the inbox.
	_, err = stack.SESHandler.Backend.SendEmail(
		"sender@example.com",
		[]string{"recipient@example.com"},
		"Hello from SES",
		"<b>Hello World</b>",
		"Hello World",
	)
	require.NoError(t, err)

	// Reload the page to see the new email.
	_, err = page.Reload()
	require.NoError(t, err)

	// Wait for the email subject to appear in the inbox table.
	err = page.Locator("text=Hello from SES").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
