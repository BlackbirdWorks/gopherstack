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
	require.NoError(
		t,
		streamsCard.Locator("span:has-text('DISABLED')").
			WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}),
	)

	// 4. Enable Streams via UI
	_, err = page.SelectOption(
		"select[name='viewType']",
		playwright.SelectOptionValues{Values: &[]string{"NEW_AND_OLD_IMAGES"}},
	)
	require.NoError(t, err)
	require.NoError(t, page.Click("label[for='streams-enabled']"))
	require.NoError(t, page.Click("button:has-text('Update Streams')"))

	// 5. Verify UI update (wait for card to show ENABLED)
	streamsCard = page.Locator("div.grid > div:has-text('Streams')")
	require.NoError(
		t,
		streamsCard.Locator("span:has-text('ENABLED')").
			WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}),
	)

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
	require.NoError(
		t,
		page.Locator("text=INSERT").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}),
	)

	// 8a. Verify stats cards render (Buffered Events, Active Shards, Iterator Expiry labels present)
	require.NoError(
		t,
		page.GetByText("Buffered Events").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}),
	)
	require.NoError(
		t,
		page.GetByText("Active Shards").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}),
	)
	require.NoError(
		t,
		page.GetByText("Iterator Expiry").
			First().
			WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}),
	)

	// 8b. Verify Buffered Events count is at least 1 (use xpath sibling to get value)
	bufferedCount := page.GetByText("Buffered Events").Locator("xpath=following-sibling::div[1]")
	bufferedText, err := bufferedCount.TextContent()
	require.NoError(t, err)
	bufferedN := strings.TrimSpace(bufferedText)
	assert.NotEqual(t, "0", bufferedN, "Buffered Events count should be > 0 after PutItem")
	assert.NotEmpty(t, bufferedN, "Buffered Events count should not be empty")

	// 8c. Verify Active Shards card shows 1
	shardsCount := page.GetByText("Active Shards").Locator("xpath=following-sibling::div[1]")
	shardsText, err := shardsCount.TextContent()
	require.NoError(t, err)
	assert.Equal(t, "1", strings.TrimSpace(shardsText), "Active Shards count should be 1")

	// 8d. Verify Shard Breakdown section is visible with the expected shard ID
	require.NoError(
		t,
		page.GetByText("Shard Breakdown").WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}),
	)
	require.NoError(
		t,
		page.Locator("td[title*='shardId-']").
			WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}),
	)

	// 8e. Verify event type breakdown: INSERT badge is present
	require.NoError(
		t,
		page.GetByText("INSERT").First().WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}),
	)

	// 8f. Verify filter buttons are present using role-based locators
	require.NoError(
		t,
		page.GetByRole("button", playwright.PageGetByRoleOptions{Name: "ALL"}).
			WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}),
	)
	require.NoError(
		t,
		page.GetByRole("button", playwright.PageGetByRoleOptions{Name: "MODIFY"}).
			WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}),
	)
	require.NoError(
		t,
		page.GetByRole("button", playwright.PageGetByRoleOptions{Name: "REMOVE"}).
			WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(10000)}),
	)

	// 9. Disable Streams via UI
	require.NoError(t, page.Click("button:has-text('Overview')"))
	require.NoError(t, page.Uncheck("#streams-enabled"))
	require.NoError(t, page.Click("button:has-text('Update Streams')"))

	// 10. Verify UI update (wait for card to show DISABLED)
	streamsCard = page.Locator("div.grid > div:has-text('Streams')")
	require.NoError(
		t,
		streamsCard.Locator("span:has-text('DISABLED')").
			WaitFor(playwright.LocatorWaitForOptions{Timeout: playwright.Float(60000)}),
	)

	// 11. Verify SDK status
	desc, err := stack.DDBClient.DescribeTable(ctx, &dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	assert.Nil(t, desc.Table.StreamSpecification)
}
