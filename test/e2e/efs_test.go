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

// TestEFSDashboard verifies the EFS dashboard UI renders file systems.
func TestEFSDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.EFSHandler.Backend.CreateFileSystem("e2e-test-token", "generalPurpose", "bursting", false, map[string]string{
		"Name": "e2e-test-fs",
	})
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
			saveScreenshot(t, page, "TestEFSDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/efs")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('EFS File Systems')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-token")
}

// TestEFSDashboard_Empty verifies the EFS dashboard empty state renders correctly.
func TestEFSDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestEFSDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/efs")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('EFS File Systems')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No EFS file systems")
}

// TestEFSDashboard_CreateDelete verifies the EFS dashboard create and delete workflows.
func TestEFSDashboard_CreateDelete(t *testing.T) {
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
			saveScreenshot(t, page, "TestEFSDashboard_CreateDelete")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/efs")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('EFS File Systems')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create modal.
	err = page.Locator("button:has-text('+ Create File System')").Click()
	require.NoError(t, err)

	// Fill in creation token.
	err = page.Locator("input#creation_token").Fill("e2e-new-token")
	require.NoError(t, err)

	// Submit form.
	err = page.Locator("button[type='submit']:has-text('Create')").Click()
	require.NoError(t, err)

	err = page.Locator("h1:has-text('EFS File Systems')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-new-token")

	// Delete the file system.
	err = page.Locator("button:has-text('Delete')").First().Click()
	require.NoError(t, err)

	err = page.Locator("h1:has-text('EFS File Systems')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
