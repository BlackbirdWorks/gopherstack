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

	// Wait for the table detail page to load.
	err = page.Locator("text=PartiQLTestTable").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// Click the PartiQL tab.
	partiqlTab, err := page.QuerySelector("#partiql-tab")
	require.NoError(t, err)
	require.NotNil(t, partiqlTab, "PartiQL tab not found")

	err = partiqlTab.Click()
	require.NoError(t, err)

	err = page.Locator("#partiql-container").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(5000),
	})
	require.NoError(t, err)

	// The partiql-form should be present after HTMX loads it.
	err = page.Locator("textarea[name='statement']").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateAttached,
		Timeout: playwright.Float(8000),
	})
	require.NoError(t, err)

	// Type a SELECT statement.
	require.NoError(t, page.Fill("textarea[name='statement']", `SELECT * FROM "PartiQLTestTable"`))

	// Submit the form.
	err = page.Click("button:has-text('Execute')")
	require.NoError(t, err)

	// Wait for results to appear in the output div.
	err = page.Locator("#partiql-output").WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateAttached,
		Timeout: playwright.Float(8000),
	})
	require.NoError(t, err)

	err = page.WaitForLoadState(playwright.PageWaitForLoadStateOptions{
		State: playwright.LoadStateNetworkidle,
	})
	require.NoError(t, err)

	// Results should appear.
	body, err := page.TextContent("body")
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

	// Wait for the page to load.
	err = page.Locator("text=TabVisibleTable").First().WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateVisible,
		Timeout: playwright.Float(10000),
	})
	require.NoError(t, err)

	// The PartiQL tab should be present.
	partiqlTab := page.Locator("#partiql-tab")
	err = partiqlTab.WaitFor(playwright.LocatorWaitForOptions{
		State:   playwright.WaitForSelectorStateAttached,
		Timeout: playwright.Float(5000),
	})
	require.NoError(t, err)

	tabText, err := partiqlTab.TextContent()
	require.NoError(t, err)
	assert.Contains(t, tabText, "PartiQL")
}
