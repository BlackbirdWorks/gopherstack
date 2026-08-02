//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestRDSDataDashboard verifies the restored RDSData dashboard loads correctly.
func TestRDSDataDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestRDSDataDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/rdsdata")
	require.NoError(t, err)

	err = page.Locator("text=RDS Data").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(30000),
	})
	require.NoError(t, err)
}

// TestRDSDataDashboard_SQLRunner verifies the rebuilt four-tab console
// (Query Console, Transactions, Statement History, ExecuteSql (legacy)) is
// visible. "SQL Runner" and "Transaction Browser" were the previous page's
// tab labels -- the rebuild renamed them and added a fourth tab for the
// deprecated ExecuteSql op; Tabs.svelte renders each as role="tab", not
// role="button", so the locators below match on the real role too.
func TestRDSDataDashboard_SQLRunner(t *testing.T) {
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
			saveScreenshot(t, page, "TestRDSDataDashboard_SQLRunner")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/rdsdata")
	require.NoError(t, err)

	err = page.GetByRole("tab", playwright.PageGetByRoleOptions{Name: "Query Console"}).
		WaitFor(playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(30000),
		})
	require.NoError(t, err)

	err = page.GetByRole("tab", playwright.PageGetByRoleOptions{Name: "Transactions"}).
		WaitFor(playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(10000),
		})
	require.NoError(t, err)

	err = page.GetByRole("tab", playwright.PageGetByRoleOptions{Name: "Statement History"}).
		WaitFor(playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(10000),
		})
	require.NoError(t, err)

	err = page.GetByRole("tab", playwright.PageGetByRoleOptions{Name: "ExecuteSql (legacy)"}).
		WaitFor(playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(10000),
		})
	require.NoError(t, err)
}
