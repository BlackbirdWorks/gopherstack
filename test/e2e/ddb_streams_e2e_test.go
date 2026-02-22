//go:build e2e

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

func TestE2E_DynamoDB_Streams(t *testing.T) {
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
			saveScreenshot(t, page, "TestE2E_DynamoDB_Streams")
		}
	}()

	tableName := "streams-e2e-table"
	ctx := t.Context()

	// 1. Create table via SDK (Streams DISABLED)
	_, err = stack.DDBClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// 2. Navigate to table detail page
	_, err = page.Goto(server.URL + "/dashboard/dynamodb/table/" + tableName)
	require.NoError(t, err)

	// 3. Verify initial Streams status (DISABLED)
	streamsCard := page.Locator("div.grid > div:has-text('Streams')")
	require.NoError(t, streamsCard.WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))
	require.NoError(t, streamsCard.Locator("span:has-text('DISABLED')").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))

	// 4. Enable Streams via UI
	_, err = page.SelectOption("select[name='viewType']", playwright.SelectOptionValues{Values: &[]string{"NEW_AND_OLD_IMAGES"}})
	require.NoError(t, err)
	require.NoError(t, page.Check("#streams-enabled"))
	require.NoError(t, page.Click("button:has-text('Update Streams')"))

	// 5. Verify success toast and UI update
	require.NoError(t, page.Locator("text=Streams enabled successfully").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))
	streamsCard = page.Locator("div.grid > div:has-text('Streams')")
	require.NoError(t, streamsCard.Locator("span:has-text('ENABLED')").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))

	// 6. Generate an event via SDK
	_, err = stack.DDBClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]ddbtypes.AttributeValue{
			"id": &ddbtypes.AttributeValueMemberS{Value: "test-item"},
		},
	})
	require.NoError(t, err)

	// 7. Click on "Stream Events" tab
	require.NoError(t, page.Click("#streams-tab"))

	// 8. Verify the INSERT event appears in the table
	require.NoError(t, page.Locator("table >> text=INSERT").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))

	// 9. Disable Streams via UI
	require.NoError(t, page.Click("#overview-tab"))
	require.NoError(t, page.Uncheck("#streams-enabled"))
	require.NoError(t, page.Click("button:has-text('Update Streams')"))

	// 10. Verify success toast and UI update
	require.NoError(t, page.Locator("text=Streams disabled successfully").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))
	streamsCard = page.Locator("div.grid > div:has-text('Streams')")
	require.NoError(t, streamsCard.Locator("span:has-text('DISABLED')").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))

	// 11. Verify SDK status
	desc, err := stack.DDBClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	assert.Nil(t, desc.Table.StreamSpecification)
}
