//go:build e2e

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

// TestE2E_DynamoDBPartiQL verifies that the PartiQL tab on the DynamoDB table detail page
// renders correctly and can execute a SELECT statement.
func TestE2E_DynamoDBPartiQL(t *testing.T) {
	stack := newStack(t)

	// Create a table to test against.
	stack.CreateDDBTable(t, "PartiQLTestTable")

	// Put a test item so the SELECT returns results.
	_, err := stack.DDBClient.PutItem(t.Context(), &dynamodb.PutItemInput{
		TableName: aws.String("PartiQLTestTable"),
		Item: map[string]ddbtypes.AttributeValue{
			"id":   &ddbtypes.AttributeValueMemberS{Value: "item-1"},
			"name": &ddbtypes.AttributeValueMemberS{Value: "hello partiql"},
		},
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
			saveScreenshot(t, page, "TestE2E_DynamoDBPartiQL")
		}
	}()

	// Navigate to the DynamoDB table detail page.
	_, err = page.Goto(server.URL + "/dashboard/dynamodb/table/PartiQLTestTable")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Wait for the table detail page to load.
	err = page.Locator("#overview-tab").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Click the PartiQL tab using its ID.
	err = page.Locator("#partiql-tab").Click()
	require.NoError(t, err)

	// The partiql textarea should be present
	textarea := page.Locator("textarea[name='statement']")
	err = textarea.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Type a SELECT statement.
	require.NoError(t, textarea.Fill(`SELECT * FROM "PartiQLTestTable"`))

	require.NoError(t, page.Locator("#partiql-execute").Click())

	// Wait for results to appear in the output div
	err = page.Locator("#partiql-output").Locator("text=item-1").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Results should appear in the pre tag inside the output div.
	body, err := page.Locator("#partiql-output pre").TextContent()
	require.NoError(t, err)
	assert.Contains(t, body, "item-1", "expected item ID in PartiQL results")
}

// TestE2E_DynamoDBPartiQL_TabVisible verifies the PartiQL tab is visible in the table detail tabs.
func TestE2E_DynamoDBPartiQL_TabVisible(t *testing.T) {
	stack := newStack(t)
	stack.CreateDDBTable(t, "TabVisibleTable")

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
			saveScreenshot(t, page, "TestE2E_DynamoDBPartiQL_TabVisible")
		}
	}()

	_, err = page.Goto(server.URL + "/dashboard/dynamodb/table/TabVisibleTable")
	require.NoError(t, err)
	waitForSPA(t, page)

	// Wait for the page to load.
	err = page.Locator("#partiql-tab").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// The PartiQL tab should be present.
	partiqlTab := page.Locator("#partiql-tab")
	tabText, err := partiqlTab.TextContent()
	require.NoError(t, err)
	assert.Contains(t, tabText, "PartiQL")
}
