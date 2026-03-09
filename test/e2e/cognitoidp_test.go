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

// TestCognitoIDPDashboard verifies the Cognito IDP dashboard UI renders user pools.
func TestCognitoIDPDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.CognitoIDPHandler.Backend.CreateUserPool("e2e-test-pool")
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
			saveScreenshot(t, page, "TestCognitoIDPDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cognitoidp")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Cognito User Pools')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-pool")
}

// TestCognitoIDPDashboard_Empty verifies the empty state renders correctly.
func TestCognitoIDPDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCognitoIDPDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cognitoidp")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Cognito User Pools')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Cognito User Pools")
}

// TestCognitoIDPDashboard_CreateAndDeleteUserPool verifies the create-pool and delete-pool UI flows.
func TestCognitoIDPDashboard_CreateAndDeleteUserPool(t *testing.T) {
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
			saveScreenshot(t, page, "TestCognitoIDPDashboard_CreateAndDeleteUserPool")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cognitoidp")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Cognito User Pools')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open modal.
	err = page.Locator("button:has-text('+ Create User Pool')").Click()
	require.NoError(t, err)

	// Fill in pool name and submit.
	err = page.Locator("input[name='name']").Fill("ui-created-pool")
	require.NoError(t, err)

	err = page.Locator("button:has-text('Create User Pool')").Last().Click()
	require.NoError(t, err)

	// Wait for the pool row to appear in the table after the redirect.
	poolRow := page.Locator("td:has-text('ui-created-pool')")
	err = poolRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Delete the pool.
	err = page.Locator("form[action='/dashboard/cognitoidp/user-pool/delete'] button").Click()
	require.NoError(t, err)

	// Wait for the pool row to disappear after the redirect.
	err = poolRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateHidden,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
