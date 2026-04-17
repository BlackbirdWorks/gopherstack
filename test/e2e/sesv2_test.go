//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestSESv2Dashboard_NotFound verifies the removed legacy SESv2 route does not render stale UI.
func TestSESv2Dashboard_NotFound(t *testing.T) {
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
			saveScreenshot(t, page, "TestSESv2Dashboard_NotFound")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sesv2")
	require.NoError(t, err)

	err = page.Locator("text=404").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(30000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	require.Contains(t, content, "Not Found")
}
