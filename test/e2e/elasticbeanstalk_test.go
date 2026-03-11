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

// TestElasticbeanstalkDashboard verifies the Elastic Beanstalk dashboard UI renders applications and environments.
func TestElasticbeanstalkDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.ElasticbeanstalkHandler.Backend.CreateApplication("e2e-app", "E2E test application", map[string]string{"env": "e2e"})
	require.NoError(t, err)

	_, err = stack.ElasticbeanstalkHandler.Backend.CreateEnvironment(
		"e2e-app", "e2e-env",
		"64bit Amazon Linux 2023 v4.0.0 running Python 3.11",
		"E2E test environment",
		map[string]string{"env": "e2e"},
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
			saveScreenshot(t, page, "TestElasticbeanstalkDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/elasticbeanstalk")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Elastic Beanstalk')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-app")
	assert.Contains(t, content, "e2e-env")
}

// TestElasticbeanstalkDashboard_Empty verifies the Elastic Beanstalk dashboard empty state renders correctly.
func TestElasticbeanstalkDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestElasticbeanstalkDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/elasticbeanstalk")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Elastic Beanstalk')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No applications")
	assert.Contains(t, content, "No environments")
}
