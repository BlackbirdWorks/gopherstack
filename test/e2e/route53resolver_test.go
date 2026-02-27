//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	route53resolverbackend "github.com/blackbirdworks/gopherstack/route53resolver"
)

// TestRoute53ResolverDashboard verifies the Route53 Resolver dashboard UI renders endpoints.
func TestRoute53ResolverDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.Route53ResolverHandler.Backend.CreateResolverEndpoint(
		"test-endpoint",
		"INBOUND",
		"vpc-12345",
		[]route53resolverbackend.IPAddress{
			{SubnetID: "subnet-1", IP: "10.0.0.10"},
		},
	)
	require.NoError(t, err)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestRoute53ResolverDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/route53resolver")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Route53 Resolver')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "test-endpoint")
	assert.Contains(t, content, "INBOUND")
}

// TestRoute53ResolverDashboard_Empty verifies the empty state renders correctly.
func TestRoute53ResolverDashboard_Empty(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestRoute53ResolverDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/route53resolver")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Route53 Resolver')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No resolver endpoints")
}

// TestRoute53ResolverDashboard_CreateAndDelete verifies creating and deleting an endpoint via the UI.
func TestRoute53ResolverDashboard_CreateAndDelete(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	context, err := browser.NewContext()
	require.NoError(t, err)
	defer context.Close()

	page, err := context.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestRoute53ResolverDashboard_CreateAndDelete")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/route53resolver")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Route53 Resolver')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open the create modal.
	err = page.Locator("button:has-text('+ Create Endpoint')").Click()
	require.NoError(t, err)

	// Fill in the form.
	err = page.Locator("#name").Fill("ui-test-endpoint")
	require.NoError(t, err)

	// Submit the form.
	err = page.Locator("button[type=submit]:has-text('Create')").Click()
	require.NoError(t, err)

	// Wait for redirect back.
	err = page.WaitForURL("**/dashboard/route53resolver", playwright.PageWaitForURLOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "ui-test-endpoint")

	// Delete the endpoint.
	err = page.Locator("form[action='/dashboard/route53resolver/delete'] button[type=submit]").First().Click()
	require.NoError(t, err)

	err = page.WaitForURL("**/dashboard/route53resolver", playwright.PageWaitForURLOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err = page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No resolver endpoints")
}
