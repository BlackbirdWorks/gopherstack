//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cwbackend "github.com/blackbirdworks/gopherstack/services/cloudwatch"
)

// TestCloudWatchDashboard verifies the CloudWatch dashboard UI renders metrics.
func TestCloudWatchDashboard(t *testing.T) {
	stack := newStack(t)

	err := stack.CloudWatchHandler.Backend.PutMetricData("TestNamespace", []cwbackend.MetricDatum{
		{MetricName: "TestMetric", Value: 42.0, Timestamp: time.Now()},
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
			saveScreenshot(t, page, "TestCloudWatchDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cloudwatch")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CloudWatch')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "CloudWatch")
	assert.Contains(t, content, "TestNamespace")
}

// TestCloudWatchDashboard_Empty verifies the empty state renders correctly.
func TestCloudWatchDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestCloudWatchDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/cloudwatch")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('CloudWatch')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "CloudWatch")
}
