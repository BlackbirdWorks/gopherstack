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

// TestXrayDashboard verifies the X-Ray dashboard UI renders groups.
func TestXrayDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.XrayHandler.Backend.CreateGroup("test-group", `service("my-service")`)
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
			saveScreenshot(t, page, "TestXrayDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/xray")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('X-Ray Groups')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-group")
}

// TestXrayDashboard_Empty verifies the empty state renders correctly.
func TestXrayDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestXrayDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/xray")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('X-Ray Groups')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No X-Ray groups found")
}

// TestXrayDashboard_CreateGroup verifies the create group form works.
func TestXrayDashboard_CreateGroup(t *testing.T) {
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
			saveScreenshot(t, page, "TestXrayDashboard_CreateGroup")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/xray")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('X-Ray Groups')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create group modal.
	err = page.Locator("button:has-text('+ Create Group')").Click()
	require.NoError(t, err)

	// Fill in the group name.
	err = page.Locator("#group_name").Fill("e2e-group")
	require.NoError(t, err)

	// Submit the form.
	err = page.Locator("button:has-text('Create Group')").Last().Click()
	require.NoError(t, err)

	// Wait for redirect back to index.
	err = page.Locator("h1:has-text('X-Ray Groups')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-group")
}
