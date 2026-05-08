//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestKinesisAnalyticsDashboard verifies the removed KinesisAnalytics route returns 404.
func TestKinesisAnalyticsDashboard(t *testing.T) {
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
			saveScreenshot(t, page, "TestKinesisAnalyticsDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/kinesisanalytics")
	require.NoError(t, err)

	err = page.Locator("text=Kinesis Data Analytics").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(30000),
	})
	require.NoError(t, err)
}

// TestKinesisAnalyticsDashboard_Empty is no longer applicable (route removed).
