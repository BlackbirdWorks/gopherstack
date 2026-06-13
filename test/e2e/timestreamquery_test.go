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

// TestTimestreamQueryDashboard verifies the Timestream Query dashboard UI renders scheduled queries.
func TestTimestreamQueryDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.TimestreamQueryHandler.Backend.CreateScheduledQuery(
		t.Context(),
		"e2e-test-query",
		"SELECT 1 FROM test_db.test_table",
		"rate(1 hour)",
		"arn:aws:iam::000000000000:role/e2e-role",
		"", "", "", "",
		map[string]string{"Environment": "test"},
	)
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
			saveScreenshot(t, page, "TestTimestreamQueryDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/timestream")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Click("button:has-text('Scheduled Queries')")
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-query")
	assert.Contains(t, content, "ENABLED")
}

// TestTimestreamQueryDashboard_Empty verifies the empty state renders correctly.
func TestTimestreamQueryDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestTimestreamQueryDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/timestream")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Click("button:has-text('Scheduled Queries')")
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No scheduled queries found")
}

// TestTimestreamQueryDashboard_Create verifies scheduled queries are displayed on the current dashboard.
func TestTimestreamQueryDashboard_Create(t *testing.T) {
	stack := newStack(t)

	_, err := stack.TimestreamQueryHandler.Backend.CreateScheduledQuery(
		t.Context(),
		"ui-test-query",
		"SELECT 1",
		"rate(1 hour)",
		"arn:aws:iam::000000000000:role/e2e-role",
		"", "", "", "",
		nil,
	)
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
			saveScreenshot(t, page, "TestTimestreamQueryDashboard_Create")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/timestream")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Click("button:has-text('Scheduled Queries')")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "ui-test-query")
}
