//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sagemakerbackend "github.com/blackbirdworks/gopherstack/services/sagemaker"
)

// TestSageMakerDashboard verifies the SageMaker dashboard UI renders model data.
func TestSageMakerDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.SageMakerHandler.Backend.CreateModel(
		"e2e-test-model",
		"arn:aws:iam::000000000000:role/test-sagemaker-role",
		&sagemakerbackend.ContainerDefinition{
			Image: "123456789012.dkr.ecr.us-east-1.amazonaws.com/my-image:latest",
		},
		nil,
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
			saveScreenshot(t, page, "TestSageMakerDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sagemaker")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SageMaker Models')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-model")
	assert.Contains(t, content, "+ Create Model")
}

// TestSageMakerDashboard_Empty verifies the SageMaker dashboard renders correctly with no models.
func TestSageMakerDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestSageMakerDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/sagemaker")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('SageMaker Models')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No models found")
}
