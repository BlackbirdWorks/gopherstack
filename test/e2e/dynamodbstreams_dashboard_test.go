//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/mxschmitt/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDynamoDBStreamsDashboard verifies the DynamoDB dashboard renders stream configuration for a table.
func TestDynamoDBStreamsDashboard(t *testing.T) {
	stack := newStack(t)

	ctx := t.Context()

	_, err := stack.DDBClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("streams-dashboard-e2e"),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	bctx, err := browser.NewContext()
	require.NoError(t, err)
	defer bctx.Close()

	page, err := bctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestDynamoDBStreamsDashboard")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)

	// Region: All's cross-region list has no #table-{name} ids -- see the
	// comment in TestE2E_DynamoDB_CreateTable in e2e_test.go.
	switchRegion(t, page, "us-east-1")

	err = page.Locator("text=streams-dashboard-e2e").First().WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	err = page.Locator("#table-streams-dashboard-e2e").Click()
	require.NoError(t, err)

	err = page.Locator("text=DynamoDB Streams Configuration").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "streams-dashboard-e2e")
	assert.Contains(t, content, "NEW_AND_OLD_IMAGES")
}

// TestDynamoDBStreamsDashboard_Empty verifies the DynamoDB dashboard empty state when no tables exist.
func TestDynamoDBStreamsDashboard_Empty(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	bctx, err := browser.NewContext()
	require.NoError(t, err)
	defer bctx.Close()

	page, err := bctx.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestDynamoDBStreamsDashboard_Empty")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/dynamodb")
	require.NoError(t, err)

	err = page.Locator("text=No tables found").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No tables found")
}
