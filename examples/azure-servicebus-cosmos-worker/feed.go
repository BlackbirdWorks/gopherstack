package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
)

type feedDoc struct {
	ID       string  `json:"id"`
	Customer string  `json:"customer"`
	Amount   float64 `json:"amount"`
}

func feedOptions(token string) *azcosmos.ChangeFeedOptions {
	o := &azcosmos.ChangeFeedOptions{FeedRange: &azcosmos.FeedRange{MinInclusive: "", MaxExclusive: "FF"}}
	if token != "" {
		o.Continuation = &token
	}

	return o
}

func printDocs(items [][]byte) (int, error) {
	for _, raw := range items {
		var d feedDoc
		if err := json.Unmarshal(raw, &d); err != nil {
			return 0, err
		}

		fmt.Printf("change feed: %s customer=%s amount=%.2f\n", d.ID, d.Customer, d.Amount)
	}

	return len(items), nil
}

// runFeed reads the change feed from the beginning until it is caught up (304).
func runFeed(ctx context.Context, want int) error {
	c, err := openOrders(ctx)
	if err != nil {
		return err
	}

	seen, token := 0, ""

	for {
		page, rerr := c.ReadChangeFeed(ctx, feedOptions(token))
		if rerr != nil {
			return rerr
		}

		n, perr := printDocs(page.Items)
		if perr != nil {
			return perr
		}

		seen += n
		token = page.ContinuationToken

		if page.RawResponse.StatusCode == http.StatusNotModified {
			break
		}
	}

	fmt.Printf("feed reader saw %d document(s)\n", seen)

	if seen < want {
		return fmt.Errorf("expected at least %d documents, saw %d", want, seen)
	}

	return nil
}
