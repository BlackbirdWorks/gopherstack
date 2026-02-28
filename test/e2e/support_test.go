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

// TestSupportDashboard verifies the Support dashboard UI renders support cases.
func TestSupportDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.SupportHandler.Backend.CreateCase(
		"Test support case", "amazon-s3", "data-management", "low", "I need help with S3.",
	)
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
			saveScreenshot(t, page, "TestSupportDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/support")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Support Cases')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Test support case")
	assert.Contains(t, content, "amazon-s3")
}

// TestSupportDashboard_Empty verifies the empty state renders correctly.
func TestSupportDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestSupportDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/support")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Support Cases')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No support cases")
}

// TestSupportDashboard_CreateCase verifies creating a support case via the UI.
func TestSupportDashboard_CreateCase(t *testing.T) {
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
			saveScreenshot(t, page, "TestSupportDashboard_CreateCase")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/support")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Support Cases')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open modal and create a new case
	err = page.Click("button:has-text('+ Create Case')")
	require.NoError(t, err)

	require.NoError(t, page.Fill("input[name='subject']", "UI created case"))
	require.NoError(t, page.Click("button[type='submit']:has-text('Create')"))

	// After redirect, verify case appears in list
	err = page.Locator("h1:has-text('Support Cases')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "UI created case")
}
