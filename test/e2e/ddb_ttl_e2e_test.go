//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/playwright-community/playwright-go"
	"github.com/stretchr/testify/require"
)

func TestE2E_DynamoDB_TTL(t *testing.T) {
	stack := newStack(t)

	server := httptest.NewServer(stack.Echo)
	defer server.Close()

	page, err := browser.NewPage()
	require.NoError(t, err)
	defer page.Close()

	defer func() {
		if t.Failed() {
			saveScreenshot(t, page, "TestE2E_DynamoDB_TTL")
		}
	}()

	tableName := "ttl-e2e-table"
	ctx := t.Context()

	// 1. Create table via SDK
	_, err = stack.DDBClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// 2. Navigate to table detail page
	_, err = page.Goto(server.URL + "/dashboard/dynamodb/table/" + tableName)
	require.NoError(t, err)

	// 3. Verify initial TTL status (DISABLED)
	require.NoError(t, page.Locator("text=TTL Status").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))
	require.NoError(t, page.Locator("span.badge", playwright.PageLocatorOptions{
		HasText: "DISABLED",
	}).First().WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))

	// 4. Configure TTL via UI
	require.NoError(t, page.Fill("input[name='attributeName']", "ttl_attr"))
	require.NoError(t, page.Check("input[name='enabled']"))
	require.NoError(t, page.Click("button:has-text('Update TTL')"))

	// 5. Verify success toast and UI update
	require.NoError(t, page.Locator("text=TTL enabled successfully").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))
	require.NoError(t, page.Locator("span.badge-success", playwright.PageLocatorOptions{
		HasText: "ENABLED (ttl_attr)",
	}).WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))

	// 6. Verify TTL status via SDK
	desc, err := stack.DDBClient.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	require.Equal(t, types.TimeToLiveStatusEnabled, desc.TimeToLiveDescription.TimeToLiveStatus)
	require.Equal(t, "ttl_attr", *desc.TimeToLiveDescription.AttributeName)

	// 7. Disable TTL via UI
	require.NoError(t, page.Uncheck("input[name='enabled']"))
	require.NoError(t, page.Click("button:has-text('Update TTL')"))

	// 8. Verify success toast and UI update
	require.NoError(t, page.Locator("text=TTL disabled successfully").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))
	require.NoError(t, page.Locator("span.badge", playwright.PageLocatorOptions{
		HasText: "DISABLED",
	}).First().WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))

	// 9. Verify TTL status via SDK again
	desc, err = stack.DDBClient.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	require.Equal(t, types.TimeToLiveStatusDisabled, desc.TimeToLiveDescription.TimeToLiveStatus)
}
