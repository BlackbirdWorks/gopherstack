//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestSQSDashboard tests the SQS dashboard UI.
func TestSQSDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestE2E_SQSDashboard")
		}
	}()

	// Navigate to the Dashboard SQS page
	_, err = page.Goto(server.URL + "/dashboard/sqs")
	require.NoError(t, err)

	// Wait for the SQS Queues heading
	err = page.Locator("h1:has-text('SQS Queues')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 1: Create a queue via the Create queue modal
	err = page.Click("button:has-text('Create queue')")
	require.NoError(t, err)

	// Fill the queue name
	require.NoError(t, page.Fill("#queue_name", "test-sqs-queue"))
	require.NoError(t, page.Fill("#visibility_timeout", "30"))

	// Submit the form
	err = page.Click("button[type='submit']")
	require.NoError(t, err)

	// Wait for redirect back to queue list
	time.Sleep(500 * time.Millisecond)

	// Verify the new queue appears in the list
	queueRow := page.Locator("td:has-text('test-sqs-queue')").First()
	err = queueRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 2: Purge the queue messages
	err = page.Click("button:has-text('Purge')")
	require.NoError(t, err)

	// Click the custom Confirm button in the confirmation dialog
	err = page.Click("#global_confirm_proceed")
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Queue should still exist after purge
	err = queueRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 3: Delete the queue
	err = page.Click("button:has-text('Delete')")
	require.NoError(t, err)

	err = page.Click("#global_confirm_proceed")
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Verify the empty state text is rendered
	err = page.Locator("td:has-text('No queues found')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
