//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
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

	// Wait for layout and SNS table header
	err = page.Locator("h1:has-text('SNS Topics')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 1: Create a Topic via modal
	err = page.Click("button:has-text('Create topic')")
	require.NoError(t, err)

	// Fill the topic name
	require.NoError(t, page.Fill("#name", "test-notifications"))

	// Submit the form
	err = page.Click("button[type='submit']:has-text('Create')")
	require.NoError(t, err)

	// HTMX redirect adds latency
	time.Sleep(500 * time.Millisecond)

	// Verify the new topic appears in the list
	topicRow := page.Locator("td:has-text('test-notifications')").First()
	err = topicRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 2: Navigate to the topic detail page
	topicLink := page.Locator("a:has-text('test-notifications')").First()
	err = topicLink.Click()
	require.NoError(t, err)

	// Wait for the topic detail page
	err = page.Locator("h1:has-text('Topic Detail')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Verify subscriptions table is shown (even if empty)
	err = page.Locator("h2:has-text('Subscriptions')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 3: Navigate back to SNS index and delete the topic
	_, err = page.Goto(server.URL + "/dashboard/sns")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SNS Topics')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Click("button:has-text('Delete')")
	require.NoError(t, err)

	// Click the custom "Confirm" button in the modal
	err = page.Click("#global_confirm_proceed")
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Verify the empty state text is rendered
	err = page.Locator("td:has-text('No topics found')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}

// TestSNSDashboard_PlatformApplications verifies the Platform Applications section renders correctly.
func TestSNSDashboard_PlatformApplications(t *testing.T) {
	stack := newStack(t)

	// Pre-seed a platform application directly via the backend.
	_, err := stack.SNSHandler.Backend.CreatePlatformApplication("e2e-apns-app", "APNS", map[string]string{
		"PlatformCredential": "fake-apns-key",
	})
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
			saveScreenshot(t, page, "TestSNSDashboard_PlatformApplications")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sns")
	require.NoError(t, err)

	err = page.Locator("h2:has-text('Platform Applications')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-apns-app")
	assert.Contains(t, content, "APNS")
}

// TestSNSDashboard_PlatformApplications_Empty verifies the empty state renders when no platform apps exist.
func TestSNSDashboard_PlatformApplications_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestSNSDashboard_PlatformApplications_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sns")
	require.NoError(t, err)

	err = page.Locator("h2:has-text('Platform Applications')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No platform applications found")
}
