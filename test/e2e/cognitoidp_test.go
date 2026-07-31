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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open modal. The rebuilt page's header action button reads "Create user
	// pool" (no leading "+", lowercase) and only appears while the User Pools
	// tab (the default) is active.
	err = page.Locator("button:has-text('Create user pool')").Click()
	require.NoError(t, err)

	// Fill in pool name and submit. The rebuilt modal's field has no `name`
	// attribute; it is addressed by id="pool-name". Scope the submit click to
	// the open <dialog> — its footer button just reads "Create" (the modal
	// title, "Create User Pool", is what the old selector actually matched),
	// and the shared Modal keeps closed dialogs' children mounted so an
	// unscoped locator risks matching more than the open one.
	err = page.Locator("input#pool-name").Fill("ui-created-pool")
	require.NoError(t, err)

	openModal := page.Locator("dialog[open]")
	err = openModal.Locator("button:has-text('Create')").Click()
	require.NoError(t, err)

	// Wait for the pool row to appear in the table after the rewritten page refresh.
	poolRow := page.Locator("tr:has-text('ui-created-pool')").First()
	err = poolRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Delete the pool from the matching row. The rebuilt table's row actions
	// are icon-only buttons (no visible "Delete" text, only a title/aria-label)
	// that open a shared confirm dialog (role="alertdialog") rather than
	// deleting immediately.
	err = poolRow.Locator("button[aria-label='Delete pool ui-created-pool']").Click()
	require.NoError(t, err)

	confirmDialog := page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Delete')").Click()
	require.NoError(t, err)

	// Wait for the pool row to disappear after the redirect.
	err = poolRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateHidden,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
