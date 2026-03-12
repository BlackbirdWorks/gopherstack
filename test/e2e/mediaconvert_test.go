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

// TestMediaConvertDashboard verifies the MediaConvert dashboard UI renders queue data.
func TestMediaConvertDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.MediaConvertHandler.Backend.CreateQueue(
		"e2e-test-queue",
		"e2e test queue",
		"",
		"",
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
			saveScreenshot(t, page, "TestMediaConvertDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/mediaconvert")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('MediaConvert')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-queue")
}

// TestMediaConvertDashboard_Empty verifies the MediaConvert dashboard empty state renders correctly.
func TestMediaConvertDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestMediaConvertDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/mediaconvert")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('MediaConvert')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No queues found")
}
