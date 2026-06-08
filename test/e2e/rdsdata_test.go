//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
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

// TestRDSDataDashboard_SQLRunner verifies the SQL Runner tab is visible.
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

	err = page.GetByRole("button", playwright.PageGetByRoleOptions{Name: "SQL Runner"}).
		WaitFor(playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(30000),
		})
	require.NoError(t, err)

	err = page.GetByRole("button", playwright.PageGetByRoleOptions{Name: "Transaction Browser"}).
		WaitFor(playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(10000),
		})
	require.NoError(t, err)

	err = page.GetByRole("button", playwright.PageGetByRoleOptions{Name: "Statement History"}).
		WaitFor(playwright.LocatorWaitForOptions{
			State:   playwright.WaitForSelectorStateVisible,
			Timeout: playwright.Float(10000),
		})
	require.NoError(t, err)
}
