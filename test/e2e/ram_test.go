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

// TestRAMDashboard verifies the RAM dashboard UI renders resource share data.
func TestRAMDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.RAMHandler.Backend.CreateResourceShare("e2e-test-share", true, map[string]string{
		"Environment": "test",
	}, nil, nil)
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
			saveScreenshot(t, page, "TestRAMDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ram")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('RAM Resource Shares')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-share")
	assert.Contains(t, content, "Create Resource Share")
}

// TestRAMDashboard_Empty verifies the RAM dashboard renders correctly with no resource shares.
func TestRAMDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestRAMDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ram")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('RAM Resource Shares')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No resource shares found")
}
