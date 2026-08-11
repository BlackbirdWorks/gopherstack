package awsconfig_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/awsconfig"
)

func TestGetStoredQuery(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()

	_, _ = b.PutStoredQuery("my-query", "", "", nil)

	q := b.GetStoredQuery("my-query")
	if q == nil || q.QueryName != "my-query" {
		t.Fatalf("GetStoredQuery: got %v", q)
	}
}

func TestGetStoredQuery_NotFound(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	q := b.GetStoredQuery("nonexistent")
	if q != nil {
		t.Fatalf("expected nil, got %v", q)
	}
}

func TestDeleteStoredQuery(t *testing.T) {
	t.Parallel()

	b := awsconfig.NewInMemoryBackend()
	_, _ = b.PutStoredQuery("q1", "", "", nil)
	_ = b.DeleteStoredQuery("q1")

	q := b.GetStoredQuery("q1")
	if q != nil {
		t.Fatal("expected nil after delete")
	}
}
