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

// TestTranscribeDashboard verifies the Transcribe dashboard UI renders transcription jobs.
func TestTranscribeDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.TranscribeHandler.Backend.StartTranscriptionJob(
		"e2e-test-job", "en-US", "s3://my-bucket/audio.mp3",
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
			saveScreenshot(t, page, "TestTranscribeDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/transcribe")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Transcribe Jobs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-job")
	assert.Contains(t, content, "COMPLETED")
}

// TestTranscribeDashboard_Empty verifies the empty state renders correctly.
func TestTranscribeDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestTranscribeDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/transcribe")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Transcribe Jobs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No transcription jobs")
}

// TestTranscribeDashboard_StartJob verifies creating a transcription job via the UI.
func TestTranscribeDashboard_StartJob(t *testing.T) {
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
			saveScreenshot(t, page, "TestTranscribeDashboard_StartJob")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/transcribe")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Transcribe Jobs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open modal and start a new job
	err = page.Click("button:has-text('+ Start Job')")
	require.NoError(t, err)

	require.NoError(t, page.Fill("input[name='jobName']", "ui-created-job"))
	require.NoError(t, page.Click("button[type='submit']:has-text('Start')"))

	// After redirect, verify job appears in list
	err = page.Locator("h1:has-text('Transcribe Jobs')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "ui-created-job")
}
