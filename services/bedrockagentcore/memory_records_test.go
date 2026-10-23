package bedrockagentcore_test

import (
	"testing"
	"time"

	sdk "github.com/aws/aws-sdk-go-v2/service/bedrockagentcore"
	"github.com/aws/aws-sdk-go-v2/service/bedrockagentcore/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createRecord(id, text, strategy string, ns ...string) types.MemoryRecordCreateInput {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	in := types.MemoryRecordCreateInput{
		RequestIdentifier: &id, Namespaces: ns, Timestamp: &at,
		Content: &types.MemoryContentMemberText{Value: text},
	}

	if strategy != "" {
		in.MemoryStrategyId = &strategy
	}

	return in
}

func seedRecords(t *testing.T, f *fixture) map[string]string {
	t.Helper()

	out, err := f.client.BatchCreateMemoryRecords(t.Context(), &sdk.BatchCreateMemoryRecordsInput{
		MemoryId: new(testMemory),
		Records: []types.MemoryRecordCreateInput{
			createRecord("r1", "The user prefers dark mode and vim keybindings", "strat-1", "/users/alice"),
			createRecord("r2", "Alice lives in Berlin and works as a nurse", "strat-1", "/users/alice"),
			createRecord("r3", "Bob prefers light mode", "", "/users/bob"),
			createRecord("bad", "x", "strat-missing", "/users/bob"),
		},
	})
	require.NoError(t, err)
	require.Len(t, out.SuccessfulRecords, 3)
	require.Len(t, out.FailedRecords, 1)
	assert.Equal(t, "bad", *out.FailedRecords[0].RequestIdentifier)
	assert.Equal(t, types.MemoryRecordStatusFailed, out.FailedRecords[0].Status)

	ids := map[string]string{}
	for _, r := range out.SuccessfulRecords {
		ids[*r.RequestIdentifier] = *r.MemoryRecordId
	}

	return ids
}

func TestMemoryRecords_Retrieve(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())
	ids := seedRecords(t, f)

	tests := []struct {
		name      string
		namespace string
		query     string
		want      []string
	}{
		{name: "ranked by overlap", namespace: "/users/alice", query: "dark mode vim", want: []string{"r1"}},
		{name: "namespace scoped", namespace: "/users/bob", query: "prefers mode", want: []string{"r3"}},
		{name: "prefix covers children", namespace: "/users", query: "prefers", want: []string{"r3", "r1"}},
		{name: "no lexical overlap", namespace: "/users/alice", query: "quantum", want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := f.client.RetrieveMemoryRecords(t.Context(), &sdk.RetrieveMemoryRecordsInput{
				MemoryId: new(testMemory), Namespace: &tt.namespace,
				SearchCriteria: &types.SearchCriteria{SearchQuery: &tt.query, TopK: new(int32(10))},
			})
			require.NoError(t, err)

			var got []string

			for _, s := range out.MemoryRecordSummaries {
				for k, v := range ids {
					if v == *s.MemoryRecordId {
						got = append(got, k)
					}
				}

				assert.Positive(t, *s.Score)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMemoryRecords_CRUD(t *testing.T) {
	t.Parallel()

	f := newFixture(t, defaultControl())
	ids := seedRecords(t, f)
	r1 := ids["r1"]

	got, err := f.client.GetMemoryRecord(t.Context(), &sdk.GetMemoryRecordInput{
		MemoryId: new(testMemory), MemoryRecordId: &r1, Namespace: new("/users/alice"),
	})
	require.NoError(t, err)
	assert.Equal(t, "strat-1", *got.MemoryRecord.MemoryStrategyId)

	_, err = f.client.GetMemoryRecord(t.Context(), &sdk.GetMemoryRecordInput{
		MemoryId: new(testMemory), MemoryRecordId: &r1, Namespace: new("/users/bob"),
	})
	assert.Equal(t, "ResourceNotFoundException", apiCode(err))

	ts := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)

	upd, err := f.client.BatchUpdateMemoryRecords(t.Context(), &sdk.BatchUpdateMemoryRecordsInput{
		MemoryId: new(testMemory),
		Records: []types.MemoryRecordUpdateInput{
			{MemoryRecordId: &r1, Timestamp: &ts, Content: &types.MemoryContentMemberText{Value: "now loves emacs"}},
			{MemoryRecordId: new("missing"), Timestamp: &ts},
		},
	})
	require.NoError(t, err)
	assert.Len(t, upd.SuccessfulRecords, 1)
	require.Len(t, upd.FailedRecords, 1)
	assert.EqualValues(t, 404, *upd.FailedRecords[0].ErrorCode)

	list, err := f.client.ListMemoryRecords(t.Context(), &sdk.ListMemoryRecordsInput{
		MemoryId: new(testMemory), Namespace: new("/users/alice"),
	})
	require.NoError(t, err)
	require.Len(t, list.MemoryRecordSummaries, 2)

	del, err := f.client.BatchDeleteMemoryRecords(t.Context(), &sdk.BatchDeleteMemoryRecordsInput{
		MemoryId: new(testMemory),
		Records:  []types.MemoryRecordDeleteInput{{MemoryRecordId: &r1}, {MemoryRecordId: new("missing")}},
	})
	require.NoError(t, err)
	assert.Len(t, del.SuccessfulRecords, 1)
	assert.Len(t, del.FailedRecords, 1)

	one, err := f.client.DeleteMemoryRecord(t.Context(), &sdk.DeleteMemoryRecordInput{
		MemoryId: new(testMemory), MemoryRecordId: new(ids["r2"]),
	})
	require.NoError(t, err)
	assert.Equal(t, ids["r2"], *one.MemoryRecordId)
}
