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

// TestIAMDashboard verifies the IAM dashboard UI: create and delete a user.
func TestIAMDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestIAMDashboard")
		}
	}()

	// Navigate to the IAM dashboard page.
	_, err = page.Goto(server.URL + "/dashboard/iam")
	require.NoError(t, err)

	// Wait for the IAM page header to appear.
	err = page.Locator("h1:has-text('IAM')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 1: Create a User via the modal.
	err = page.Click("button:has-text('Create User')")
	require.NoError(t, err)

	// Fill the create user form.
	require.NoError(t, page.Fill("#iam_userName", "e2e-test-user"))

	// Submit via HTMX.
	err = page.Click("button[type='submit']:has-text('Create')")
	require.NoError(t, err)

	// Wait for redirect and the new row to appear.
	time.Sleep(500 * time.Millisecond)

	userRow := page.Locator("td:has-text('e2e-test-user')").First()
	err = userRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 2: Delete the user.
	err = page.Click("button:has-text('Delete')")
	require.NoError(t, err)

	// Click the confirm button in the global confirm modal.
	err = page.Click("#global_confirm_proceed")
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Verify the empty state is shown.
	err = page.Locator("td:has-text('No users')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
