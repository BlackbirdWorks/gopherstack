//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestSNSDashboard tests the SNS Dashboard UI behavior.
func TestSNSDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestE2E_SNSDashboard")
		}
	}()

	// Navigate to the Dashboard SNS page
	_, err = page.Goto(server.URL + "/dashboard/sns")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 1: Create a Topic via modal.
	err = page.Click("button:has-text('Create Topic')")
	require.NoError(t, err)

	topicInput := page.Locator("input[placeholder='e.g. my-notifications']")
	require.NoError(t, topicInput.Fill("test-notifications"))

	err = page.Click("button[type='submit']:has-text('Create Topic')")
	require.NoError(t, err)

	// Verify the new topic appears in the list.
	topicCard := page.Locator("div[role='button']:has-text('test-notifications')").First()
	err = topicCard.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 2: Select the topic and verify details render.
	err = topicCard.Click()
	require.NoError(t, err)

	err = page.Locator("h2:has-text('test-notifications')").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("h3:has-text('Subscriptions')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 3: Navigate back to list and delete topic.
	_, err = page.Goto(server.URL + "/dashboard/sns")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	deleteBtn := page.Locator("div[role='button']:has-text('test-notifications')").First().Locator("button")
	err = deleteBtn.Click()
	require.NoError(t, err)
	confirmDialog := page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Delete')").Click()
	require.NoError(t, err)

	err = page.Locator("text=No topics found").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}

// TestSNSDashboard_Empty verifies empty state renders correctly.
func TestSNSDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestSNSDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sns")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("text=No topics found").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}
