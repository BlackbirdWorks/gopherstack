package ssm

import (
	"fmt"
	"hash/fnv"
	"time"
)

// --- Inventory deletion job records ---

// InventoryDeletionSummary summarises the outcome of a DeleteInventory job.
type InventoryDeletionSummary struct {
	SummaryItems   []any `json:"SummaryItems,omitempty"`
	RemainingCount int   `json:"RemainingCount"`
	TotalCount     int   `json:"TotalCount"`
}

// InventoryDeletion is a record of a DeleteInventory job, returned by
// DescribeInventoryDeletions.
type InventoryDeletion struct {
	DeletionSummary   *InventoryDeletionSummary `json:"DeletionSummary,omitempty"`
	DeletionStartTime time.Time                 `json:"DeletionStartTime"`
	DeletionID        string                    `json:"DeletionId"`
	TypeName          string                    `json:"TypeName"`
	LastStatus        string                    `json:"LastStatus"`
	LastStatusMessage string                    `json:"LastStatusMessage,omitempty"`
}

// --- Default (AWS-managed) patch baselines ---

// defaultBaselineID derives a stable, realistically-shaped patch baseline ID for
// the AWS-managed default baseline of the given OS. AWS provides a managed
// default baseline per operating system; returning a deterministic, well-shaped
// ID (rather than a fabricated all-zeros ID) keeps GetDefaultPatchBaseline
// stable across calls and consistent for a given OS.
func defaultBaselineID(os string) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte("AWS-DefaultPatchBaseline:" + os))

	return fmt.Sprintf("pb-%017x", h.Sum64())
}
