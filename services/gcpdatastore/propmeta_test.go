package gcpdatastore_test

import (
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPropertyMetadataQuery(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seed(t, "Job", seedTasks())

	_, err := e.client.Put(t.Context(), datastore.NameKey("Other", "o", nil), &struct{ Count int }{Count: 1})
	require.NoError(t, err)

	reps := func(t *testing.T, q *datastore.Query) map[string][]string {
		t.Helper()

		var props []datastore.PropertyList

		keys, gerr := e.client.GetAll(t.Context(), q, &props)
		require.NoError(t, gerr)

		out := map[string][]string{}

		for i, k := range keys {
			var r []string

			for _, p := range props[i] {
				if p.Name != "property_representation" {
					continue
				}

				for _, v := range p.Value.([]any) {
					r = append(r, v.(string))
				}
			}

			out[k.Parent.Name+"/"+k.Name] = r
		}

		return out
	}

	all := reps(t, datastore.NewQuery("__property__"))
	assert.Equal(t, []string{"INT64"}, all["Job/Priority"])
	assert.Equal(t, []string{"DOUBLE"}, all["Job/Score"])
	assert.Equal(t, []string{"BOOLEAN"}, all["Job/Done"])
	assert.Equal(t, []string{"STRING"}, all["Job/Tags"])
	assert.Equal(t, []string{"INT64"}, all["Other/Count"])
	assert.NotContains(t, all, "Job/Note", "unindexed properties are not listed")

	scoped := reps(t, datastore.NewQuery("__property__").Ancestor(datastore.NameKey("__kind__", "Other", nil)))
	assert.Len(t, scoped, 1)
	assert.Contains(t, scoped, "Other/Count")

	keys, err := e.client.GetAll(t.Context(), datastore.NewQuery("__property__").KeysOnly(), nil)
	require.NoError(t, err)
	assert.Len(t, keys, 6)
}
