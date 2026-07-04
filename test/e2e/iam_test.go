//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIAMDashboard verifies the IAM dashboard tabs and empty states render.
func TestIAMDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestIAMDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/iam")
	require.NoError(t, err)
	waitForSPA(t, page)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("#empty-state-text").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.TextContent("#empty-state-text")
	require.NoError(t, err)
	assert.Contains(t, content, "No users found")

	err = page.Click("#roles-tab")
	require.NoError(t, err)

	err = page.Locator("#empty-state-text").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err = page.TextContent("#empty-state-text")
	require.NoError(t, err)
	assert.Contains(t, content, "No roles found")

	err = page.Click("#groups-tab")
	require.NoError(t, err)

	err = page.Locator("#empty-state-text").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err = page.TextContent("#empty-state-text")
	require.NoError(t, err)
	assert.Contains(t, content, "No groups found")

	err = page.Click("#policies-tab")
	require.NoError(t, err)

	err = page.Locator("#empty-state-text").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err = page.TextContent("#empty-state-text")
	require.NoError(t, err)
	assert.Contains(t, content, "No policies found")

	pageContent, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, pageContent, "IAM")
}
