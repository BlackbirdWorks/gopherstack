//go:build e2e
// +build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDynamoDBStreamsDashboard verifies the DynamoDB Streams dashboard renders active streams.
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

	_, err = page.Goto(server.URL + "/dashboard/dynamodbstreams")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('DynamoDB Streams')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "streams-dashboard-e2e")
	assert.Contains(t, content, "arn:aws:dynamodb")
}

// TestDynamoDBStreamsDashboard_Empty verifies the DynamoDB Streams dashboard renders correctly when no streams are active.
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

	_, err = page.Goto(server.URL + "/dashboard/dynamodbstreams")
	require.NoError(t, err)

	err = page.Locator("h1:has-text('DynamoDB Streams')").WaitFor(playwright.LocatorWaitForOptions{
		Timeout: playwright.Float(60000),
	})
	require.NoError(t, err)

	content, err := page.Content()
	require.NoError(t, err)
	assert.Contains(t, content, "No active DynamoDB Streams")
}
