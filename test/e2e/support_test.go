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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No support cases found")
}

// TestSupportDashboard_CreateCase verifies support cases are rendered in the current UI.
func TestSupportDashboard_CreateCase(t *testing.T) {
	stack := newStack(t)

	_, err := stack.SupportHandler.Backend.CreateCase(
		"UI created case", "amazon-s3", "data-management", "low", "Case created by backend seed.",
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
			saveScreenshot(t, page, "TestSupportDashboard_CreateCase")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/support")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "UI created case")
}
