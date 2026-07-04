package comprehend_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_ListJobs_Pagination verifies NextToken/MaxResults on async job list operations.
func TestParity_ListJobs_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	for i := range 5 {
		request(t, h, "StartSentimentDetectionJob", map[string]any{
			"JobName":           fmt.Sprintf("job-%d", i),
			"LanguageCode":      "en",
			"DataAccessRoleArn": "arn:aws:iam::123456789012:role/role",
			"InputDataConfig":   map[string]any{"S3Uri": "s3://bucket/input"},
			"OutputDataConfig":  map[string]any{"S3Uri": "s3://bucket/output"},
		})
	}

	tests := []struct {
		input         map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			input:         map[string]any{},
			wantLen:       5,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			input:         map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out := request(t, h, "ListSentimentDetectionJobs", tt.input)
			jobs, _ := out["SentimentDetectionJobPropertiesList"].([]any)
			assert.Len(t, jobs, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListJobs_FullPagination walks all pages collecting all jobs.
func TestParity_ListJobs_FullPagination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	const total = 5
	for i := range total {
		request(t, h, "StartEntitiesDetectionJob", map[string]any{
			"JobName":           fmt.Sprintf("entjob-%d", i),
			"LanguageCode":      "en",
			"DataAccessRoleArn": "arn:aws:iam::123456789012:role/role",
			"InputDataConfig":   map[string]any{"S3Uri": "s3://bucket/input"},
			"OutputDataConfig":  map[string]any{"S3Uri": "s3://bucket/output"},
		})
	}

	seen := map[string]bool{}
	token := ""
	pages := 0

	for {
		inp := map[string]any{"MaxResults": float64(2)}
		if token != "" {
			inp["NextToken"] = token
		}

		out := request(t, h, "ListEntitiesDetectionJobs", inp)
		jobs, _ := out["EntitiesDetectionJobPropertiesList"].([]any)
		assert.LessOrEqual(t, len(jobs), 2)

		for _, j := range jobs {
			jm := j.(map[string]any)
			name := jm["JobName"].(string)
			assert.False(t, seen[name], "job %s seen twice", name)
			seen[name] = true
		}

		pages++
		require.Less(t, pages, 10)

		tok, _ := out["NextToken"].(string)
		token = tok
		if token == "" {
			break
		}
	}

	assert.Len(t, seen, total)
	assert.GreaterOrEqual(t, pages, 3)
}

// TestParity_ListResources_Pagination verifies NextToken/MaxResults on resource list operations.
func TestParity_ListResources_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	for i := range 4 {
		request(t, h, "CreateDocumentClassifier", map[string]any{
			"DocumentClassifierName": fmt.Sprintf("classifier-%d", i),
			"LanguageCode":           "en",
			"DataAccessRoleArn":      "arn:aws:iam::123456789012:role/role",
			"InputDataConfig":        map[string]any{"S3Uri": "s3://bucket/data"},
		})
	}

	tests := []struct {
		input         map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			input:         map[string]any{},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			input:         map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out := request(t, h, "ListDocumentClassifiers", tt.input)
			classifiers, _ := out["DocumentClassifierPropertiesList"].([]any)
			assert.Len(t, classifiers, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListEntityRecognizers_Pagination verifies pagination on ListEntityRecognizers.
func TestParity_ListEntityRecognizers_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	for i := range 4 {
		request(t, h, "CreateEntityRecognizer", map[string]any{
			"RecognizerName":    fmt.Sprintf("recognizer-%d", i),
			"LanguageCode":      "en",
			"DataAccessRoleArn": "arn:aws:iam::123456789012:role/role",
			"InputDataConfig": map[string]any{
				"EntityTypes": []map[string]any{{"Type": "PERSON"}},
			},
		})
	}

	tests := []struct {
		input         map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			input:         map[string]any{},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			input:         map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out := request(t, h, "ListEntityRecognizers", tt.input)
			recognizers, _ := out["EntityRecognizerPropertiesList"].([]any)
			assert.Len(t, recognizers, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListDocumentClassifierSummaries_Pagination verifies pagination on summaries.
func TestParity_ListDocumentClassifierSummaries_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	for i := range 4 {
		request(t, h, "CreateDocumentClassifier", map[string]any{
			"DocumentClassifierName": fmt.Sprintf("sumclassifier-%d", i),
			"LanguageCode":           "en",
			"DataAccessRoleArn":      "arn:aws:iam::123456789012:role/role",
			"InputDataConfig":        map[string]any{"S3Uri": "s3://bucket/data"},
		})
	}

	tests := []struct {
		input         map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			input:         map[string]any{},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			input:         map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out := request(t, h, "ListDocumentClassifierSummaries", tt.input)
			summaries, _ := out["DocumentClassifierSummariesList"].([]any)
			assert.Len(t, summaries, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListEntityRecognizerSummaries_Pagination verifies pagination on recognizer summaries.
func TestParity_ListEntityRecognizerSummaries_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	for i := range 4 {
		request(t, h, "CreateEntityRecognizer", map[string]any{
			"RecognizerName":    fmt.Sprintf("sumrecognizer-%d", i),
			"LanguageCode":      "en",
			"DataAccessRoleArn": "arn:aws:iam::123456789012:role/role",
			"InputDataConfig": map[string]any{
				"EntityTypes": []map[string]any{{"Type": "PERSON"}},
			},
		})
	}

	tests := []struct {
		input         map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			input:         map[string]any{},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			input:         map[string]any{"MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out := request(t, h, "ListEntityRecognizerSummaries", tt.input)
			summaries, _ := out["EntityRecognizerSummariesList"].([]any)
			assert.Len(t, summaries, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}

// TestParity_ListFlywheelIterations_Pagination verifies pagination on ListFlywheelIterationHistory.
func TestParity_ListFlywheelIterations_Pagination(t *testing.T) {
	t.Parallel()

	h := newHandler()

	flyOut := request(t, h, "CreateFlywheel", map[string]any{
		"FlywheelName":      "test-flywheel",
		"DataAccessRoleArn": "arn:aws:iam::123456789012:role/role",
		"DataLakeS3Uri":     "s3://bucket/lake",
	})
	flywheelArn, _ := flyOut["FlywheelArn"].(string)
	require.NotEmpty(t, flywheelArn)

	for range 4 {
		request(t, h, "StartFlywheelIteration", map[string]any{
			"FlywheelArn": flywheelArn,
		})
	}

	tests := []struct {
		input         map[string]any
		name          string
		wantLen       int
		wantNextToken bool
	}{
		{
			name:          "no_limit_returns_all",
			input:         map[string]any{"FlywheelArn": flywheelArn},
			wantLen:       4,
			wantNextToken: false,
		},
		{
			name:          "page1_two_items",
			input:         map[string]any{"FlywheelArn": flywheelArn, "MaxResults": float64(2)},
			wantLen:       2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			out := request(t, h, "ListFlywheelIterationHistory", tt.input)
			iters, _ := out["FlywheelIterationPropertiesList"].([]any)
			assert.Len(t, iters, tt.wantLen)
			if tt.wantNextToken {
				assert.NotEmpty(t, out["NextToken"])
			} else {
				assert.Empty(t, out["NextToken"])
			}
		})
	}
}
