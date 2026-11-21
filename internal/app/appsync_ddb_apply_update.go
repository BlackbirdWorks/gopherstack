package app

import (
	"fmt"
	"maps"

	appsyncbackend "github.com/blackbirdworks/gopherstack/services/appsync"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/expr"
	ddbmodels "github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// ApplyUpdateRaw evaluates a DynamoDB UpdateExpression against a copy of item and returns the result.
func (a *dynamoDBAdapter) ApplyUpdateRaw(
	item map[string]any, update appsyncbackend.DDBExpression,
) (map[string]any, error) {
	parsed, err := expr.NewParser(expr.NewLexer(update.Expression)).ParseUpdate()
	if err == nil {
		err = expr.CheckReservedWords(parsed)
	}

	if err != nil {
		return nil, fmt.Errorf("invalid UpdateExpression: %w", err)
	}

	sdkItem, err := ddbmodels.ToSDKItem(item)
	if err != nil {
		return nil, fmt.Errorf("appsync ddb adapter: marshal item: %w", err)
	}

	values := maps.Clone(update.Values)
	if values == nil {
		values = map[string]any{}
	}

	ev := &expr.Evaluator{
		Item: ddbmodels.FromSDKItem(sdkItem), AttrNames: update.Names, AttrValues: values,
	}

	if err = ev.ApplyUpdate(parsed); err != nil {
		return nil, fmt.Errorf("apply UpdateExpression: %w", err)
	}

	return ev.Item, nil
}
