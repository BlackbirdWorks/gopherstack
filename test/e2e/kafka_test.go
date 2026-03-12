//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	kafkabackend "github.com/blackbirdworks/gopherstack/services/kafka"
)

// TestKafkaDashboard verifies the Kafka (MSK) dashboard renders with cluster list.
func TestKafkaDashboard(t *testing.T) {
	stack := newStack(t)

	// Seed a cluster via the backend.
	_, err := stack.KafkaHandler.Backend.CreateCluster(
		"my-test-cluster",
		"3.5.1",
		1,
		kafkabackend.BrokerNodeGroupInfo{
			ClientSubnets: []string{"subnet-00000000"},
			InstanceType:  "kafka.m5.large",
		},
		nil,
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
			saveScreenshot(t, page, "TestKafkaDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/kafka")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amazon MSK')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "Clusters")
	assert.Contains(t, content, "my-test-cluster")
}

// TestKafkaDashboard_CreateCluster verifies creating a cluster via the dashboard form.
func TestKafkaDashboard_CreateCluster(t *testing.T) {
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
			saveScreenshot(t, page, "TestKafkaDashboard_CreateCluster")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/kafka")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('Amazon MSK')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	// Open create cluster modal.
	err = page.Locator("button:has-text('+ Cluster')").Click()
	require.NoError(t, err)

	// Fill in name field.
	err = page.Locator("#create-cluster-modal input[name='name']").Fill("e2e-cluster")
	require.NoError(t, err)

	// Submit.
	err = page.Locator("#create-cluster-modal button[type='submit']").Click()
	require.NoError(t, err)

	// Wait for redirect back to kafka page.
	err = page.Locator("h1:has-text('Amazon MSK')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "e2e-cluster")
}
