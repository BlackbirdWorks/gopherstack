package textract

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// syntheticExpenseDocument builds a realistic expense document.
func syntheticExpenseDocument(documentURI string) ExpenseDocument {
	blocks := syntheticBlocks(documentURI)

	summaryFields := []ExpenseField{
		{
			Type:           &ExpenseType{Text: "VENDOR_NAME", Confidence: confidenceExpenseHigh},
			ValueDetection: &ExpenseDetection{Text: "Acme Corp", Confidence: confidenceExpenseHigh},
			PageNumber:     1,
		},
		{
			Type:           &ExpenseType{Text: "TOTAL", Confidence: confidenceExpenseHigh},
			ValueDetection: &ExpenseDetection{Text: "$123.45", Confidence: confidenceExpenseHigh},
			PageNumber:     1,
			Currency:       &ExpenseCurrency{Code: "USD", Confidence: confidenceExpenseHigh},
		},
		{
			Type:           &ExpenseType{Text: "INVOICE_RECEIPT_DATE", Confidence: confidenceExpenseMed},
			ValueDetection: &ExpenseDetection{Text: "2024-01-01", Confidence: confidenceExpenseMed},
			PageNumber:     1,
		},
	}

	lineItemGroups := []LineItemGroup{
		{
			LineItemGroupIndex: 1,
			LineItems: []LineItem{
				{
					LineItemExpenseFields: []ExpenseField{
						{
							Type:           &ExpenseType{Text: "ITEM", Confidence: confidenceExpenseLI},
							ValueDetection: &ExpenseDetection{Text: "Widget A", Confidence: confidenceExpenseLI},
							PageNumber:     1,
						},
						{
							Type:           &ExpenseType{Text: "PRICE", Confidence: confidenceExpenseLI},
							ValueDetection: &ExpenseDetection{Text: "$45.00", Confidence: confidenceExpenseLI},
							PageNumber:     1,
						},
					},
				},
				{
					LineItemExpenseFields: []ExpenseField{
						{
							Type:           &ExpenseType{Text: "ITEM", Confidence: confidenceExpenseLI2},
							ValueDetection: &ExpenseDetection{Text: "Widget B", Confidence: confidenceExpenseLI2},
							PageNumber:     1,
						},
						{
							Type:           &ExpenseType{Text: "PRICE", Confidence: confidenceExpenseLI2},
							ValueDetection: &ExpenseDetection{Text: "$78.45", Confidence: confidenceExpenseLI2},
							PageNumber:     1,
						},
					},
				},
			},
		},
	}

	return ExpenseDocument{
		ExpenseIndex:   1,
		Blocks:         blocks,
		SummaryFields:  summaryFields,
		LineItemGroups: lineItemGroups,
	}
}

// AnalyzeExpense performs a synchronous expense analysis and returns expense documents.
func (b *InMemoryBackend) AnalyzeExpense(_ context.Context, documentURI string) []ExpenseDocument {
	doc := syntheticExpenseDocument(documentURI)

	return []ExpenseDocument{doc}
}

// StartExpenseAnalysis creates an async expense analysis job.
func (b *InMemoryBackend) StartExpenseAnalysis(ctx context.Context, documentURI string) (*ExpenseJob, error) {
	return b.StartExpenseAnalysisWithOptions(ctx, documentURI, nil, nil, "", "")
}

// StartExpenseAnalysisWithOptions creates an async expense analysis job with
// full options, including ClientRequestToken dedup: real AWS returns the same
// JobId when the same token is reused (see the docs on
// StartExpenseAnalysisInput.ClientRequestToken).
func (b *InMemoryBackend) StartExpenseAnalysisWithOptions(
	ctx context.Context,
	documentURI string,
	outputConfig *OutputConfig,
	notificationChannel *NotificationChannel,
	jobTag, clientRequestToken string,
) (*ExpenseJob, error) {
	region := getRegion(ctx, b.region)

	var result *ExpenseJob
	var done bool
	var key string

	func() {
		b.mu.Lock("StartExpenseAnalysis")
		defer b.mu.Unlock()

		// Idempotency: if token already seen, return existing job.
		if clientRequestToken != "" {
			if existingID, ok := b.expenseClientTokenToJobIDStore(region)[clientRequestToken]; ok {
				if existing, ok2 := b.expenseJobs.Get(regionKey(region, existingID)); ok2 {
					result = cloneExpenseJob(existing)
					done = true

					return
				}
			}
		}

		jobID := uuid.NewString()
		job := &ExpenseJob{
			Region:              region,
			JobID:               jobID,
			JobStatus:           jobStatusInProgress,
			CreationTime:        time.Now(),
			ExpenseDocuments:    []ExpenseDocument{syntheticExpenseDocument(documentURI)},
			OutputConfig:        outputConfig,
			NotificationChannel: notificationChannel,
			JobTag:              jobTag,
			ClientRequestToken:  clientRequestToken,
		}
		b.expenseJobs.Put(job)
		trimExpenseJobsIfNeeded(b.expenseJobs, b.expenseJobsByRegion, region, b.maxJobs)

		if clientRequestToken != "" {
			b.expenseClientTokenToJobIDStore(region)[clientRequestToken] = jobID
		}

		if b.asyncJobDelay == 0 {
			job.JobStatus = jobStatusSucceeded
			result = cloneExpenseJob(job)
			done = true

			return
		}

		key = expenseJobKey(job)
	}()

	if done {
		return result, nil
	}

	b.runDelayed(b.asyncJobDelay, func() {
		b.mu.Lock("StartExpenseAnalysis-complete")
		defer b.mu.Unlock()

		if j, ok := b.expenseJobs.Get(key); ok {
			j.JobStatus = jobStatusSucceeded
		}
	})

	func() {
		b.mu.RLock("StartExpenseAnalysis-read")
		defer b.mu.RUnlock()

		stored, _ := b.expenseJobs.Get(key)
		result = cloneExpenseJob(stored)
	}()

	return result, nil
}

// GetExpenseAnalysis retrieves the results of an expense analysis job.
// Returns a deep clone so callers may safely mutate the returned value.
func (b *InMemoryBackend) GetExpenseAnalysis(ctx context.Context, jobID string) (*ExpenseJob, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetExpenseAnalysis")
	defer b.mu.RUnlock()

	job, ok := b.expenseJobs.Get(regionKey(region, jobID))
	if !ok {
		return nil, fmt.Errorf("%w: expense job %s not found", ErrJobNotFound, jobID)
	}

	return cloneExpenseJob(job), nil
}

// cloneExpenseJob returns a deep copy of an ExpenseJob.
func cloneExpenseJob(j *ExpenseJob) *ExpenseJob {
	cp := *j
	cp.ExpenseDocuments = make([]ExpenseDocument, len(j.ExpenseDocuments))

	for i, doc := range j.ExpenseDocuments {
		docCopy := doc
		docCopy.Blocks = make([]Block, len(doc.Blocks))
		copy(docCopy.Blocks, doc.Blocks)
		docCopy.SummaryFields = make([]ExpenseField, len(doc.SummaryFields))
		copy(docCopy.SummaryFields, doc.SummaryFields)
		docCopy.LineItemGroups = make([]LineItemGroup, len(doc.LineItemGroups))
		copy(docCopy.LineItemGroups, doc.LineItemGroups)
		cp.ExpenseDocuments[i] = docCopy
	}

	return &cp
}

func trimExpenseJobsIfNeeded(
	t *store.Table[ExpenseJob], byRegion *store.Index[ExpenseJob], region string, maxJobs int,
) {
	entries := slices.Clone(byRegion.Get(region))
	if len(entries) <= maxJobs {
		return
	}

	sort.Slice(entries, func(i, k int) bool { return entries[i].CreationTime.Before(entries[k].CreationTime) })

	excess := len(entries) - maxJobs
	for i := range excess {
		t.Delete(expenseJobKey(entries[i]))
	}
}
