//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

// TestEC2Dashboard verifies the EC2 dashboard UI renders with instances, security groups, and VPCs.
func TestEC2Dashboard(t *testing.T) {
	stack := newStack(t)

	// Seed some EC2 data so the dashboard has something to show.
	_, err := stack.EC2Handler.Backend.RunInstances("ami-12345678", "t3.micro", "", 1)
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
			saveScreenshot(t, page, "TestEC2Dashboard")
		}
	}()

	// Navigate to the EC2 dashboard page.
	_, err = page.Goto(server.URL + "/dashboard/ec2")
	require.NoError(t, err)

	// Wait for the EC2 header to appear.
	err = page.Locator("h1:has-text('EC2')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Verify the Instances tab is present.
	err = page.Locator("button:has-text('Instances')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Verify the Security Groups tab is present.
	err = page.Locator("button:has-text('Security Groups')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Verify the VPCs & Subnets tab is present.
	err = page.Locator("button:has-text('VPCs')").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)
}
