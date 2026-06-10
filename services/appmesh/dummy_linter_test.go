package appmesh_test

import "testing"

func TestDummyLinter(t *testing.T) {
	t.Parallel()

	datasetARN := ""
	tests := []struct{ name string }{{"A"}, {"B"}}
	for _, tc := range tests {
		t.Logf("Running test: %s", tc.name)
		datasetARN = "my-arn"
	}

	t.Run("DescribeDataset", func(t *testing.T) {
		t.Parallel()
		t.Logf("Dataset ARN: %s", datasetARN)
	})
}
