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

// TestSecretsManagerDashboard tests the SecretsManager dashboard UI:
// create secret, view detail, update value, view version history, delete.
func TestSecretsManagerDashboard(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	ctx, err := browser.NewContext()
	require.NoError(t, err)
	defer ctx.Close()

	page, err := ctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestSecretsManagerDashboard")
		}
	}()

	// Navigate to the Secrets Manager dashboard page.
	_, err = page.Goto(server.URL + "/dashboard/secretsmanager")
	require.NoError(t, err)

	// Wait for the Secrets Manager heading.
	err = page.Locator("h1:has-text('Secrets Manager')").WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Step 1: Open the Create Secret form.
	err = page.Click("button:has-text('Create Secret')")
	require.NoError(t, err)

	// Fill name, description, and value.
	require.NoError(t, page.Fill("input[name='name']", "e2e/test-secret"))
	require.NoError(t, page.Fill("input[name='description']", "e2e test secret"))
	require.NoError(t, page.Fill("input[name='secret_string']", "initial-value"))

	// Submit.
	err = page.Locator("#create-secret-form button[type='submit']").Click()
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Verify the new secret appears in the list.
	secretRow := page.Locator("td:has-text('e2e/test-secret')").First()
	err = secretRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 2: Navigate to the secret detail page.
	detailLink := page.Locator("a[href*='/dashboard/secretsmanager/secret?name=']").First()
	err = detailLink.Click()
	require.NoError(t, err)

	// Wait for Secret Detail page.
	err = page.Locator("h1:has-text('Secret Detail')").WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Step 3: Update the secret value.
	require.NoError(t, page.Fill("input[name='secret_string']", "updated-value"))

	err = page.Locator("form[hx-post*='/dashboard/secretsmanager/update'] button[type='submit']").Click()
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Verify version history table is visible (should have at least one row after update).
	err = page.Locator("h2:has-text('Version History')").WaitFor(
		playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(60000),
		},
	)
	require.NoError(t, err)

	// Step 4: Navigate back and delete the secret.
	_, err = page.Goto(server.URL + "/dashboard/secretsmanager")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Secrets Manager')").WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Click Delete button for the secret.
	err = page.Locator("button[hx-delete*='e2e/test-secret']").Click()
	require.NoError(t, err)

	// Confirm deletion in the modal.
	err = page.Click("#global_confirm_proceed")
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	// Verify the empty-state text or the secret is gone.
	err = page.Locator("td:has-text('No secrets found')").WaitFor(
		playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(60000),
		},
	)
	require.NoError(t, err)
}
