//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	taggingbackend "github.com/blackbirdworks/gopherstack/services/resourcegroupstaggingapi"
)

const testTaggingARN = "arn:aws:s3:::e2e-tagged-bucket"

// TestResourceGroupsTaggingAPIDashboard verifies the Resource Groups Tagging API dashboard
// renders tagged resources seeded via a registered resource provider.
func TestResourceGroupsTaggingAPIDashboard(t *testing.T) {
	stack := newStack(t)

	stack.ResourceGroupsTaggingHandler.Backend.RegisterProvider(func() []taggingbackend.TaggedResource {
		return []taggingbackend.TaggedResource{
			{
				ResourceARN:  testTaggingARN,
				ResourceType: "s3:bucket",
				Tags:         map[string]string{"env": "e2e"},
			},
		}
	})

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
			saveScreenshot(t, page, "TestResourceGroupsTaggingAPIDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/resourcegroupstaggingapi")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Resource Groups Tagging API')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, testTaggingARN)
	assert.Contains(t, content, "env=e2e")
}

// TestResourceGroupsTaggingAPIDashboard_Empty verifies the empty state renders correctly.
func TestResourceGroupsTaggingAPIDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestResourceGroupsTaggingAPIDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/resourcegroupstaggingapi")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Resource Groups Tagging API')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No tagged resources found")
}
