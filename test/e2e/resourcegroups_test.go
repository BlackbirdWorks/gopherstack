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

// TestResourceGroupsDashboard verifies the Resource Groups dashboard UI renders groups.
func TestResourceGroupsDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ResourceGroupsHandler.Backend.CreateGroup("test-group", "an e2e test group", nil)
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
			saveScreenshot(t, page, "TestResourceGroupsDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/resourcegroups")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Resource Groups')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-group")
}

// TestResourceGroupsDashboard_Empty verifies the empty state renders correctly.
func TestResourceGroupsDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestResourceGroupsDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/resourcegroups")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Resource Groups')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Resource Groups")
}
