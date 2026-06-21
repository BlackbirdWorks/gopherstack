// Command bootstrap is a Go (provided.al2) Lambda that chains DynamoDB events.
//
// It is wired to the streams of three DynamoDB tables (chain-1, chain-2,
// chain-3) via event source mappings. When an item is INSERTed into chain-N
// the table's stream invokes this function; the handler copies the new item
// into chain-(N+1), adding a "hop" attribute. Inserting one seed item into
// chain-1 therefore produces a 3-link chain of DynamoDB events:
//
//	put chain-1  ->  stream -> lambda -> put chain-2
//	                          stream -> lambda -> put chain-3   (terminal)
//
// The DynamoDB client targets the gopherstack mock via AWS_ENDPOINT_URL.
package main

import (
	"context"
	"log"
	"strconv"
	"strings"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// tableChain defines the next table for each source table. A table absent from
// the map is terminal (the chain stops there).
var tableChain = map[string]string{
	"chain-1": "chain-2",
	"chain-2": "chain-3",
}

func handler(ctx context.Context, e events.DynamoDBEvent) error {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return err
	}
	ddb := dynamodb.NewFromConfig(cfg)

	for _, rec := range e.Records {
		if rec.EventName != "INSERT" {
			continue // only react to new items, not our own no-op churn
		}
		src := tableFromStreamARN(rec.EventSourceArn)
		next, ok := tableChain[src]
		if !ok {
			log.Printf("chain terminal at %s (id=%s)", src, stringAttr(rec.Change.NewImage, "id"))
			continue
		}

		id := stringAttr(rec.Change.NewImage, "id")
		hop := intAttr(rec.Change.NewImage, "hop")
		log.Printf("hop %d: %s -> %s (id=%s)", hop, src, next, id)

		_, err := ddb.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(next),
			Item: map[string]ddbtypes.AttributeValue{
				"id":   &ddbtypes.AttributeValueMemberS{Value: id},
				"hop":  &ddbtypes.AttributeValueMemberN{Value: strconv.Itoa(hop + 1)},
				"from": &ddbtypes.AttributeValueMemberS{Value: src},
			},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// tableFromStreamARN extracts the table name from a DynamoDB stream ARN like
// arn:aws:dynamodb:us-east-1:000000000000:table/chain-1/stream/2026-...
// Splitting on "/" yields [".../table", "chain-1", "stream", "<label>"], so the
// table name is the segment immediately preceding "stream".
func tableFromStreamARN(arn string) string {
	parts := strings.Split(arn, "/")
	for i, p := range parts {
		if p == "stream" && i > 0 {
			return parts[i-1]
		}
	}
	return ""
}

func stringAttr(img map[string]events.DynamoDBAttributeValue, key string) string {
	if v, ok := img[key]; ok {
		return v.String()
	}
	return ""
}

func intAttr(img map[string]events.DynamoDBAttributeValue, key string) int {
	if v, ok := img[key]; ok {
		if n, err := strconv.Atoi(v.Number()); err == nil {
			return n
		}
	}
	return 0
}

func main() { lambda.Start(handler) }
