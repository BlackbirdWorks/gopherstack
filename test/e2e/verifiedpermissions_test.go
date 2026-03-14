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

// TestVerifiedPermissionsDashboard verifies the Verified Permissions dashboard UI renders policy stores.
func TestVerifiedPermissionsDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.VerifiedPermissionsHandler.Backend.CreatePolicyStore("My Test Store", nil)
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
			saveScreenshot(t, page, "TestVerifiedPermissionsDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/verifiedpermissions")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Verified Permissions')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "My Test Store")
}

// TestVerifiedPermissionsDashboard_Empty verifies the empty state renders correctly.
func TestVerifiedPermissionsDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestVerifiedPermissionsDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/verifiedpermissions")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Verified Permissions')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No policy stores found")
}

// TestVerifiedPermissionsDashboard_CreatePolicyStore verifies the create policy store form works.
func TestVerifiedPermissionsDashboard_CreatePolicyStore(t *testing.T) {
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
			saveScreenshot(t, page, "TestVerifiedPermissionsDashboard_CreatePolicyStore")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/verifiedpermissions")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Verified Permissions')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create policy store modal
	err = page.Locator("button:has-text('+ Create Policy Store')").Click()
	require.NoError(t, err)

	// Fill in description
	err = page.Locator("#description").Fill("E2E Test Store")
	require.NoError(t, err)

	// Submit the form
	err = page.Locator("button:has-text('Create Policy Store')").Last().Click()
	require.NoError(t, err)

	// Wait for redirect back to index
	err = page.Locator("h1:has-text('Verified Permissions')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "E2E Test Store")
}
