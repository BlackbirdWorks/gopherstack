package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestGlueResourceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		resourceARN  string
		resourceType string
		want         string
	}{
		{
			name:         "valid_database_arn_with_account",
			resourceARN:  "arn:aws:glue:us-east-1:000000000000:database/my-db",
			resourceType: "database",
			want:         "my-db",
		},
		{
			name:         "valid_database_arn_empty_account",
			resourceARN:  "arn:aws:glue:us-east-1::database/my-db",
			resourceType: "database",
			want:         "my-db",
		},
		{
			name:         "valid_crawler_arn",
			resourceARN:  "arn:aws:glue:us-east-1:000000000000:crawler/my-crawler",
			resourceType: "crawler",
			want:         "my-crawler",
		},
		{
			name:         "valid_job_arn",
			resourceARN:  "arn:aws:glue:us-east-1:000000000000:job/my-job",
			resourceType: "job",
			want:         "my-job",
		},
		{
			name:         "wrong_resource_type",
			resourceARN:  "arn:aws:glue:us-east-1:000000000000:database/my-db",
			resourceType: "crawler",
			want:         "",
		},
		{
			name:         "malformed_arn_too_few_parts",
			resourceARN:  "arn:aws:glue:us-east-1",
			resourceType: "database",
			want:         "",
		},
		{
			name:         "empty_arn",
			resourceARN:  "",
			resourceType: "database",
			want:         "",
		},
		{
			name:         "resource_type_not_in_arn",
			resourceARN:  "arn:aws:glue:us-east-1:000000000000:table/my-table",
			resourceType: "database",
			want:         "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := glue.GlueResourceNameForTest(tt.resourceARN, tt.resourceType)
			assert.Equal(t, tt.want, got)
		})
	}
}
