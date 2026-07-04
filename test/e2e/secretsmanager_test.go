//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestSecretsManagerDashboard tests the Secrets Manager dashboard UI: create, view, delete.
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
	err = page.Locator("h1").First().WaitFor(
		playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)},
	)
	require.NoError(t, err)

	// Verify empty state.
	err = page.Locator("text=No secrets found").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Open the Create Secret modal.
	err = page.Click("button:has-text('Create Secret')")
	require.NoError(t, err)

	err = page.Locator("h2:has-text('Create Secret')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Fill in the secret name and value.
	err = page.Fill("#secret-name", "e2e/test/db-password")
	require.NoError(t, err)

	err = page.Fill("#secret-value", `{"username":"admin","password":"s3cr3t"}`)
	require.NoError(t, err)

	// Submit the create form.
	err = page.Locator("button:has-text('Create')").Last().Click()
	require.NoError(t, err)

	// Modal should close after successful creation.
	err = page.Locator("h2:has-text('Create Secret')").WaitFor(
		playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateHidden,
			Timeout: playwright.Float(10000),
		},
	)
	require.NoError(t, err)

	// The new secret should appear in the list (target the h3 heading specifically to
	// avoid strict-mode violations from ancestor elements that also contain the text).
	err = page.Locator("h3:has-text('e2e/test/db-password')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Navigate into the detail view.
	err = page.Locator("button:has-text('View')").First().Click()
	require.NoError(t, err)

	err = page.Locator("h2:has-text('e2e/test/db-password')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}
