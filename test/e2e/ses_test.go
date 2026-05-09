//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ses"
)

// TestSESDashboard verifies the rewritten SES dashboard identities and send-email flows.
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

	// Wait for the SES page to render.
	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Seed a verified identity and confirm it appears on the identities tab.
	require.NoError(t, stack.SESHandler.Backend.VerifyEmailIdentity("sender@example.com"))

	identityRow := page.Locator("text=sender@example.com")
	err = page.Locator("button:has-text('Refresh')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('Refresh')").Click()
	require.NoError(t, err)

	err = identityRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Success")

	// Use the current Send Email tab and verify the backend receives the message.
	err = page.Locator("button:has-text('Send Email')").First().Click()
	require.NoError(t, err)

	err = page.Locator("#send-from").Fill("sender@example.com")
	require.NoError(t, err)

	err = page.Locator("#send-to").Fill("recipient@example.com")
	require.NoError(t, err)

	err = page.Locator("#send-subject").Fill("Hello from SES UI")
	require.NoError(t, err)

	err = page.Locator("#send-body").Fill("Hello World")
	require.NoError(t, err)

	err = page.Locator("button:has-text('Send Email')").Last().Click()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		backend, ok := stack.SESHandler.Backend.(*ses.InMemoryBackend)
		if !ok {
			return false
		}

		emails := backend.ListEmails()
		if len(emails) != 1 {
			return false
		}

		return emails[0].From == "sender@example.com" &&
			emails[0].Subject == "Hello from SES UI" &&
			len(emails[0].To) == 1 &&
			emails[0].To[0] == "recipient@example.com"
	}, 10*time.Second, 200*time.Millisecond)

	err = page.Locator("text=Email sent successfully").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
