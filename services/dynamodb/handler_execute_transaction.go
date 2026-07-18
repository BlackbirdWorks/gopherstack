// Package dynamodb implements the AWS DynamoDB mock service.
// handler_execute_transaction.go implements the wire-JSON handler for
// ExecuteTransaction. Routing (dispatch) stays in handler.go; this is the
// leaf implementation it calls into. Backend logic lives in
// execute_transaction.go.
package dynamodb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdkDDB "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

type executeTransactionStatementWire struct {
	Statement  string           `json:"Statement"`
	Parameters []map[string]any `json:"Parameters,omitempty"`
}

type executeTransactionInput struct {
	ClientRequestToken     string                            `json:"ClientRequestToken,omitempty"`
	ReturnConsumedCapacity string                            `json:"ReturnConsumedCapacity,omitempty"`
	TransactStatements     []executeTransactionStatementWire `json:"TransactStatements"`
}

type executeTransactionItemResponse struct {
	Item map[string]any `json:"Item,omitempty"`
}

type executeTransactionOutput struct {
	ConsumedCapacity []map[string]any                 `json:"ConsumedCapacity,omitempty"`
	Responses        []executeTransactionItemResponse `json:"Responses,omitempty"`
}

func (h *DynamoDBHandler) handleExecuteTransaction(ctx context.Context, body []byte) (any, error) {
	var req executeTransactionInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	stmts := make([]types.ParameterizedStatement, 0, len(req.TransactStatements))

	for _, s := range req.TransactStatements {
		sdkParams := make([]types.AttributeValue, 0, len(s.Parameters))

		for _, p := range s.Parameters {
			av, err := models.ToSDKAttributeValue(p)
			if err != nil {
				return nil, fmt.Errorf("converting parameter: %w", err)
			}

			sdkParams = append(sdkParams, av)
		}

		stmt := s.Statement
		stmts = append(stmts, types.ParameterizedStatement{
			Statement:  &stmt,
			Parameters: sdkParams,
		})
	}

	out, err := h.Backend.ExecuteTransaction(ctx, &sdkDDB.ExecuteTransactionInput{
		TransactStatements:     stmts,
		ReturnConsumedCapacity: types.ReturnConsumedCapacity(req.ReturnConsumedCapacity),
	})
	if err != nil {
		return nil, err
	}

	responses := make([]executeTransactionItemResponse, 0, len(out.Responses))

	for _, r := range out.Responses {
		resp := executeTransactionItemResponse{}
		if r.Item != nil {
			resp.Item = models.FromSDKItem(r.Item)
		}

		responses = append(responses, resp)
	}

	var consumedCapacity []map[string]any
	if req.ReturnConsumedCapacity != "" &&
		types.ReturnConsumedCapacity(
			req.ReturnConsumedCapacity,
		) != types.ReturnConsumedCapacityNone {
		for _, cc := range out.ConsumedCapacity {
			entry := map[string]any{"TableName": aws.ToString(cc.TableName)}
			if cc.CapacityUnits != nil {
				entry["CapacityUnits"] = *cc.CapacityUnits
			}
			consumedCapacity = append(consumedCapacity, entry)
		}
	}

	return &executeTransactionOutput{Responses: responses, ConsumedCapacity: consumedCapacity}, nil
}
