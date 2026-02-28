//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSWFDashboard verifies the SWF dashboard UI renders domains.
func TestSWFDashboard(t *testing.T) {
	stack := newStack(t)

	err := stack.SWFHandler.Backend.RegisterDomain("test-domain", "an e2e test domain")
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
			saveScreenshot(t, page, "TestSWFDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/swf")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SWF Domains')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-domain")
}

// TestSWFDashboard_Empty verifies the empty state renders correctly.
func TestSWFDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestSWFDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/swf")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SWF Domains')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No domains registered")
}
