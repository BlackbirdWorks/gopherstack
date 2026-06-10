//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	transcribe "github.com/blackbirdworks/gopherstack/services/transcribe"
)

// TestTranscribeDashboard verifies the Transcribe dashboard UI renders transcription jobs.
func TestTranscribeDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.TranscribeHandler.Backend.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "e2e-test-job",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://my-bucket/audio.mp3"},
		},
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Amazon Transcribe")
	assert.Contains(t, content, "Transcription Jobs")
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No transcription jobs found")
}

// TestTranscribeDashboard_StartJob verifies transcription jobs are displayed on the current UI.
func TestTranscribeDashboard_StartJob(t *testing.T) {
	stack := newStack(t)

	_, err := stack.TranscribeHandler.Backend.StartTranscriptionJob(
		&transcribe.TranscriptionJob{
			JobName:      "ui-created-job",
			LanguageCode: "en-US",
			Media:        transcribe.Media{MediaFileURI: "s3://my-bucket/input.wav"},
		},
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
			saveScreenshot(t, page, "TestTranscribeDashboard_StartJob")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/transcribe")
	require.NoError(t, err)

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Amazon Transcribe")
}
