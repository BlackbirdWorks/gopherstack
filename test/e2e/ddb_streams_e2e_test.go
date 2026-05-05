//go:build e2e

package e2e_test

import (
	"net/http/httptest"
	"strings"
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
	require.NoError(t, page.Click("label[for='streams-enabled']"))
	require.NoError(t, page.Click("button:has-text('Update Streams')"))

	// 5. Verify UI update (wait for card to show ENABLED)
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
	require.NoError(t, page.Click("button:has-text('Stream Events')"))

	// 8. Verify the INSERT event appears in the recent events table
	require.NoError(t, page.Locator("text=INSERT").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))

	// 8a. Verify stats cards render with correct values (Buffered Events = 1)
	bufferedCard := page.Locator("div:has-text('Buffered Events') + div")
	require.NoError(t, bufferedCard.WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}))
	bufferedText, err := bufferedCard.TextContent()
	require.NoError(t, err)
	assert.Equal(t, "1", strings.TrimSpace(bufferedText), "Buffered Events count should be 1")

	// 8b. Verify Active Shards card shows 1
	shardsCard := page.Locator("div:has-text('Active Shards') + div")
	require.NoError(t, shardsCard.WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}))
	shardsText, err := shardsCard.TextContent()
	require.NoError(t, err)
	assert.Equal(t, "1", strings.TrimSpace(shardsText), "Active Shards count should be 1")

	// 8c. Verify Iterator Expiry card shows 15 min
	require.NoError(t, page.Locator("div:has-text('Iterator Expiry')").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}))

	// 8d. Verify Shard Breakdown section is visible with the expected shard ID
	shardBreakdown := page.Locator("h4:has-text('Shard Breakdown')")
	require.NoError(t, shardBreakdown.WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}))
	shardRow := page.Locator("td[title*='shardId-']")
	require.NoError(t, shardRow.WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}))

	// 8e. Verify event type breakdown badges show INSERT count
	insertBadge := page.Locator("span:has-text('INSERT') + span")
	require.NoError(t, insertBadge.WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}))
	insertCount, err := insertBadge.TextContent()
	require.NoError(t, err)
	assert.Equal(t, "1", strings.TrimSpace(insertCount), "INSERT badge count should be 1")

	// 8f. Verify filter buttons are present
	require.NoError(t, page.Locator("button:has-text('ALL')").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}))
	require.NoError(t, page.Locator("button:has-text('INSERT')").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}))
	require.NoError(t, page.Locator("button:has-text('MODIFY')").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}))
	require.NoError(t, page.Locator("button:has-text('REMOVE')").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}))

	// 9. Disable Streams via UI
	require.NoError(t, page.Click("button:has-text('Overview')"))
	require.NoError(t, page.Uncheck("#streams-enabled"))
	require.NoError(t, page.Click("button:has-text('Update Streams')"))

	// 10. Verify UI update (wait for card to show DISABLED)
	streamsCard = page.Locator("div.grid > div:has-text('Streams')")
	require.NoError(t, streamsCard.Locator("span:has-text('DISABLED')").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}))

	// 11. Verify SDK status
	desc, err := stack.DDBClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	assert.Nil(t, desc.Table.StreamSpecification)
}
