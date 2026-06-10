//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestSSMDashboard UI behavior.
func TestSSMDashboard(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	if u, err := url.Parse(server.URL); err == nil {
		stack.S3Handler.Endpoint = u.Host
	}

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_SSMDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/ssm")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)})
	require.NoError(t, err)

	// Step 1: Create a parameter via modal.
	err = page.Click("button:has-text('Create Parameter')")
	require.NoError(t, err)

	err = page.Locator("h2:has-text('Create Parameter')").
		WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)})
	require.NoError(t, err)

	require.NoError(t, page.Fill("input[placeholder='e.g. /myapp/database/password']", "test-database-password"))
	_, err = page.SelectOption("select", playwright.SelectOptionValues{Values: &[]string{"SecureString"}})
	require.NoError(t, err)
	require.NoError(t, page.Fill("textarea[placeholder='Parameter value...']", "super_secret_e2e_pass"))
	require.NoError(t, page.Fill("input[placeholder='Parameter description...']", "E2E Test Password"))

	err = page.Click("button[type='submit']:has-text('Create')")
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	paramRow := page.Locator("button:has-text('test-database-password')").First()
	err = paramRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 2: Select and edit the parameter.
	err = paramRow.Click()
	require.NoError(t, err)

	err = page.Click("button:has-text('Edit')")
	require.NoError(t, err)

	err = page.Locator("h2:has-text('Edit Parameter')").
		WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)})
	require.NoError(t, err)

	require.NoError(t, page.Fill("textarea[placeholder='Parameter value...']", "new_super_secret"))

	err = page.Click("button[type='submit']:has-text('Update')")
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	err = paramRow.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Step 3: Delete the selected parameter.
	err = page.Click("button:has-text('Delete')")
	require.NoError(t, err)
	confirmDialog := page.Locator("[role='alertdialog']")
	err = confirmDialog.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
	err = confirmDialog.Locator("button:has-text('Delete')").Click()
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)

	err = page.Locator("text=No parameters found").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
