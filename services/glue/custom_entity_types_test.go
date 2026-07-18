package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestBatchGetCustomEntityTypes_FoundAndMissing(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddCustomEntityTypeInternal(&glue.CustomEntityType{Name: "cet1", RegexString: `\d+`})

	found, missing := b.BatchGetCustomEntityTypes([]string{"cet1", "cet2"})

	assert.Len(t, found, 1)
	assert.Equal(t, "cet1", found[0].Name)
	assert.Len(t, missing, 1)
}
