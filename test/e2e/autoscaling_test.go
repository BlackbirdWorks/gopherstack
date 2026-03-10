//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/autoscaling"
	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestAutoscalingDashboard verifies the Autoscaling dashboard UI renders groups and launch configurations.
func TestAutoscalingDashboard(t *testing.T) {
	stack := newStack(t)

	_, err := stack.AutoscalingHandler.Backend.CreateAutoScalingGroup(autoscaling.CreateAutoScalingGroupInput{
		AutoScalingGroupName: "e2e-test-asg",
		MinSize:              1,
		MaxSize:              5,
		DesiredCapacity:      2,
		AvailabilityZones:    []string{"us-east-1a"},
	})
	require.NoError(t, err)

	_, err = stack.AutoscalingHandler.Backend.CreateLaunchConfiguration(autoscaling.CreateLaunchConfigurationInput{
		LaunchConfigurationName: "e2e-test-lc",
		ImageID:                 "ami-12345678",
		InstanceType:            "t2.micro",
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
			saveScreenshot(t, page, "TestAutoscalingDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/autoscaling")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Auto Scaling')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-test-asg")
	assert.Contains(t, content, "e2e-test-lc")
}

// TestAutoscalingDashboard_Empty verifies the Autoscaling dashboard empty state renders correctly.
func TestAutoscalingDashboard_Empty(t *testing.T) {
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
			saveScreenshot(t, page, "TestAutoscalingDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/autoscaling")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Auto Scaling')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Auto Scaling")
}
