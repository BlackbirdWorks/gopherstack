//go:build e2e
// +build e2e

package e2e_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestKinesisAnalyticsV2Dashboard verifies the Kinesis Data Analytics v2 dashboard renders with application list.
func TestKinesisAnalyticsV2Dashboard(t *testing.T) {
	stack := newStack(t)

	// Seed an application via the backend.
	err := stack.KinesisHandler.Backend.CreateStream(context.Background(), &kinesisbackend.CreateStreamInput{
		StreamName: "my-test-stream",
		ShardCount: 1,
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
			saveScreenshot(t, page, "TestKinesisAnalyticsV2Dashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/kinesis")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Kinesis Data Streams")
	assert.Contains(t, content, "my-test-stream")
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

	_, err = page.Goto(server.URL + "/dashboard/kinesis")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create stream modal.
	err = page.Locator("button:has-text('Create Stream')").Click()
	require.NoError(t, err)

	// Fill in stream name.
	err = page.Locator("input[placeholder='e.g. user-events']").Fill("e2e-analytics-stream")
	require.NoError(t, err)

	// Submit.
	err = page.Locator("button[type='submit']:has-text('Create Stream')").Click()
	require.NoError(t, err)

	err = page.Locator("text=e2e-analytics-stream").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Wait for redirect back to kinesisanalyticsv2 page.
	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-analytics-stream")
}
