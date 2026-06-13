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

	route53resolverbackend "github.com/blackbirdworks/gopherstack/services/route53resolver"
)

// TestRoute53ResolverDashboard verifies the Route53 Resolver dashboard UI renders endpoints.
func TestRoute53ResolverDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.Route53ResolverHandler.Backend.CreateResolverEndpoint(
		t.Context(),
		"test-endpoint",
		"INBOUND",
		"vpc-12345",
		[]route53resolverbackend.IPAddress{{
			SubnetID: "subnet-1",
			IP:       "10.0.0.10",
		}},
		[]string{"sg-12345"},
		"IPV4",
		nil, "", "", "",
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)})
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No resolver endpoints found")
}

// TestRoute53ResolverDashboard_CreateAndDelete verifies list updates after backend create/delete while viewing UI.
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

	err = page.Locator("h1").First().WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)})
	require.NoError(t, err)

	createdEndpoint, err := stack.Route53ResolverHandler.Backend.CreateResolverEndpoint(
		t.Context(),
		"ui-test-endpoint",
		"OUTBOUND",
		"vpc-12345",
		[]route53resolverbackend.IPAddress{{
			SubnetID: "subnet-2",
			IP:       "10.0.0.11",
		}},
		[]string{"sg-12345"},
		"IPV4",
		nil, "", "", "",
	)
	require.NoError(t, err)

	err = page.Locator("button:has-text('Refresh')").Click()
	require.NoError(t, err)

	err = page.Locator("text=ui-test-endpoint").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Verify tab interaction still works after refresh.
	err = page.Locator("button:has-text('Rules')").Click()
	require.NoError(t, err)

	err = page.Locator("text=No resolver rules found").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	err = page.Locator("button:has-text('Endpoints')").Click()
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)
	err = page.Locator("text=ui-test-endpoint").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	require.NoError(t, stack.Route53ResolverHandler.Backend.DeleteResolverEndpoint(t.Context(), createdEndpoint.ID))

	err = page.Locator("button:has-text('Refresh')").Click()
	require.NoError(t, err)

	err = page.Locator("text=No resolver endpoints found").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)
}
