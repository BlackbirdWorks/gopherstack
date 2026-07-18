package textract

import "context"

// syntheticIDDocument builds a realistic identity document.
func syntheticIDDocument(documentURI string, index int) IdentityDocument {
	blocks := syntheticBlocks(documentURI)

	fields := []IdentityDocumentField{
		{
			Type:           &AnalyzeIDDetections{Text: "FIRST_NAME", Confidence: confidenceExpenseHigh},
			ValueDetection: &AnalyzeIDDetections{Text: "JANE", Confidence: confidenceExpenseHigh},
		},
		{
			Type:           &AnalyzeIDDetections{Text: "LAST_NAME", Confidence: confidenceExpenseHigh},
			ValueDetection: &AnalyzeIDDetections{Text: "DOE", Confidence: confidenceExpenseHigh},
		},
		{
			Type: &AnalyzeIDDetections{Text: "DATE_OF_BIRTH", Confidence: confidenceExpenseMed},
			ValueDetection: &AnalyzeIDDetections{
				Text:            "1990-01-15",
				Confidence:      confidenceExpenseMed,
				NormalizedValue: &NormalizedValue{Value: "1990-01-15", ValueType: "DATE"},
			},
		},
		{
			Type: &AnalyzeIDDetections{Text: "EXPIRATION_DATE", Confidence: confidenceExpenseMed},
			ValueDetection: &AnalyzeIDDetections{
				Text:            "2030-12-31",
				Confidence:      confidenceExpenseMed,
				NormalizedValue: &NormalizedValue{Value: "2030-12-31", ValueType: "DATE"},
			},
		},
		{
			Type:           &AnalyzeIDDetections{Text: "DOCUMENT_NUMBER", Confidence: confidenceExpenseHigh},
			ValueDetection: &AnalyzeIDDetections{Text: "D12345678", Confidence: confidenceExpenseHigh},
		},
	}

	return IdentityDocument{
		DocumentIndex:          index,
		Blocks:                 blocks,
		IdentityDocumentFields: fields,
	}
}

// AnalyzeID performs a synchronous ID analysis and returns identity documents.
func (b *InMemoryBackend) AnalyzeID(_ context.Context, documentURIs []string) []IdentityDocument {
	docs := make([]IdentityDocument, 0, len(documentURIs))
	for i, uri := range documentURIs {
		docs = append(docs, syntheticIDDocument(uri, i+1))
	}

	return docs
}
