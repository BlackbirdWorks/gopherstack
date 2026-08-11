//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/mxschmitt/playwright-go"
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
	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 1: Create a queue via the create modal
	err = page.Click("button:has-text('Create Queue')")
	require.NoError(t, err)

	// Fill the queue form
	require.NoError(t, page.Fill("input[placeholder='e.g. order-processing']", "test-sqs-queue"))
	require.NoError(t, page.Fill("input[type='number']", "30"))

	// Submit the form
	err = page.Click("button[type='submit']")
	require.NoError(t, err)

	// Verify the new queue appears in the list and select the actual clickable card
	queueCard := page.Locator("div[role='button']:has-text('test-sqs-queue')").First()
	err = queueCard.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
	err = queueCard.Click()
	require.NoError(t, err)

	// Step 2: Purge the queue messages
	purgeButton := page.Locator("button:has-text('Purge')").First()
	err = purgeButton.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
	err = purgeButton.Click()
	require.NoError(t, err)
	confirmDialog := page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Purge')").Click()
	require.NoError(t, err)

	// Queue should still exist after purge
	err = queueCard.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 3: Delete the queue from the same queue card
	err = queueCard.Locator("button").First().Click()
	require.NoError(t, err)
	confirmDialog = page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Delete')").Click()
	require.NoError(t, err)

	// Verify the empty state text is rendered
	err = page.Locator("text=No queues found").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
