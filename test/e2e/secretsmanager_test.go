//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestSecretsManagerDashboard tests the current Secrets Manager dashboard UI behavior.
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

	// Open the Create Secret modal.
	err = page.Click("button:has-text('Create Secret')")
	require.NoError(t, err)

	err = page.Locator("h2:has-text('Create Secret')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// In the current UI, create action is an informational flow.
	err = page.Locator("button:has-text('Create')").Last().Click()
	require.NoError(t, err)

	err = page.Locator("h2:has-text('Create Secret')").WaitFor(
		playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateHidden,
			Timeout: playwright.Float(10000),
		},
	)
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	require.Contains(t, content, "No secrets found")
}
