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

// TestKinesisAnalyticsV2Dashboard verifies the Kinesis Data Analytics v2 dashboard renders with application list.
func TestKinesisAnalyticsV2Dashboard(t *testing.T) {
	stack := newStack(t)

	// Seed an application via the backend.
	_, err := stack.KinesisAnalyticsV2Handler.Backend.CreateApplication(
		"my-test-app",
		"FLINK-1_18",
		"arn:aws:iam::000000000000:role/service-role",
		"",
		"",
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
			saveScreenshot(t, page, "TestKinesisAnalyticsV2Dashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/kinesisanalyticsv2")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amazon Kinesis Data Analytics')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Applications")
	assert.Contains(t, content, "my-test-app")
}

// TestKinesisAnalyticsV2Dashboard_CreateApplication verifies creating an application via the dashboard form.
func TestKinesisAnalyticsV2Dashboard_CreateApplication(t *testing.T) {
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
			saveScreenshot(t, page, "TestKinesisAnalyticsV2Dashboard_CreateApplication")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/kinesisanalyticsv2")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amazon Kinesis Data Analytics')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create application modal.
	err = page.Locator("button:has-text('+ Application')").Click()
	require.NoError(t, err)

	// Fill in name field.
	err = page.Locator("#create-application-modal input[name='name']").Fill("e2e-analytics-app")
	require.NoError(t, err)

	// Submit.
	err = page.Locator("#create-application-modal button[type='submit']").Click()
	require.NoError(t, err)

	// Wait for redirect back to kinesisanalyticsv2 page.
	err = page.Locator("h1:has-text('Amazon Kinesis Data Analytics')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-analytics-app")
}
