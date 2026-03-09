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

// TestSESv2Dashboard verifies the SES v2 dashboard UI: identities and configuration sets render correctly.
func TestSESv2Dashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.SESv2Handler.Backend.CreateEmailIdentity("e2e-sender@example.com")
	require.NoError(t, err)

	_, err = stack.SESv2Handler.Backend.CreateConfigurationSet("e2e-config-set")
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
			saveScreenshot(t, page, "TestSESv2Dashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sesv2")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SES v2')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-sender@example.com")
	assert.Contains(t, content, "e2e-config-set")
}

// TestSESv2Dashboard_Empty verifies the SES v2 dashboard empty state renders correctly.
func TestSESv2Dashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestSESv2Dashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sesv2")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SES v2')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Email Identities")
	assert.Contains(t, content, "Configuration Sets")
}

// TestSESv2Dashboard_CreateAndDeleteIdentity verifies the create-identity and delete-identity UI flows.
func TestSESv2Dashboard_CreateAndDeleteIdentity(t *testing.T) {
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
			saveScreenshot(t, page, "TestSESv2Dashboard_CreateAndDeleteIdentity")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sesv2")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SES v2')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open modal.
	err = page.Locator("button:has-text('+ Create Identity')").Click()
	require.NoError(t, err)

	// Fill in identity and submit.
	err = page.Locator("input[name='identity']").Fill("ui-created@example.com")
	require.NoError(t, err)

	err = page.Locator("button:has-text('Create Identity')").Last().Click()
	require.NoError(t, err)

	// Wait for the identity row to appear in the table after the redirect.
	identityRow := page.Locator("td:has-text('ui-created@example.com')")
	err = identityRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Delete the identity.
	err = page.Locator("form[action='/dashboard/sesv2/identity/delete'] button").Click()
	require.NoError(t, err)

	// Wait for the identity row to disappear after the redirect.
	err = identityRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateHidden,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}

// TestSESv2Dashboard_CreateAndDeleteConfigurationSet verifies the create and delete configuration set UI flows.
func TestSESv2Dashboard_CreateAndDeleteConfigurationSet(t *testing.T) {
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
			saveScreenshot(t, page, "TestSESv2Dashboard_CreateAndDeleteConfigurationSet")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sesv2")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SES v2')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open modal and create a configuration set.
	err = page.Locator("button:has-text('+ Create Configuration Set')").Click()
	require.NoError(t, err)

	err = page.Locator("input[name='name']").Fill("ui-config-set")
	require.NoError(t, err)

	err = page.Locator("button:has-text('Create Configuration Set')").Last().Click()
	require.NoError(t, err)

	// Wait for the configuration set row to appear in the table after the redirect.
	configRow := page.Locator("td:has-text('ui-config-set')")
	err = configRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Delete the configuration set.
	err = page.Locator("form[action='/dashboard/sesv2/configuration-set/delete'] button").Click()
	require.NoError(t, err)

	// Wait for the configuration set row to disappear after the redirect.
	err = configRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateHidden,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
