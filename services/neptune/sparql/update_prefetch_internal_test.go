package sparql

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateRemoteIOOutsideStoreLock(t *testing.T) {
	t.Parallel()

	const remoteRows = `{"head":{"vars":["x"]},"results":{"bindings":[{"x":{"type":"uri","value":"urn:remote"}}]}}`

	tests := []struct {
		name    string
		update  string
		wantErr bool
		wantIns int
	}{
		{
			name:    "service endpoint",
			update:  `INSERT { ?x <urn:p> 1 } WHERE { SERVICE <http://remote/sparql> { ?x ?p ?o } }`,
			wantIns: 1,
		},
		{name: "load", update: `LOAD <http://remote/doc.nt>`, wantIns: 1},
		{
			name:    "variable endpoint refused",
			update:  `INSERT { ?x <urn:p> 1 } WHERE { VALUES ?e { <http://remote/sparql> } SERVICE ?e { ?x ?p ?o } }`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := NewStore()
			calls, locked := 0, false

			probe := func() {
				calls++

				if held, _, _ := s.mu.GetLockStatus(); held {
					locked = true
				}
			}

			res, err := s.Update(t.Context(), tt.update, UpdateOptions{
				Service: func(_ context.Context, _, _ string) (*Result, error) {
					probe()

					return ParseSelectJSON([]byte(remoteRows))
				},
				Fetch: func(_ context.Context, _ string) ([]byte, error) {
					probe()

					return []byte(`<urn:a> <urn:b> <urn:c> .`), nil
				},
			})

			assert.False(t, locked, "remote I/O ran while the store write lock was held")

			if tt.wantErr {
				require.Error(t, err)
				assert.Zero(t, calls)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 1, calls)
			assert.Equal(t, tt.wantIns, res.Inserted)
		})
	}
}

func TestUpdatePrefetchBoundsFetchCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		loads     int
		wantCalls int
		wantErr   bool
	}{
		{name: "at cap", loads: maxPrefetchFetches, wantCalls: maxPrefetchFetches},
		{name: "over cap", loads: maxPrefetchFetches + 5, wantCalls: maxPrefetchFetches, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var sb strings.Builder

			for i := range tt.loads {
				if i > 0 {
					sb.WriteString(" ; ")
				}

				fmt.Fprintf(&sb, "LOAD <http://remote/d%d.nt>", i)
			}

			calls := 0
			s := NewStore()

			_, err := s.Update(t.Context(), sb.String(), UpdateOptions{
				Fetch: func(_ context.Context, src string) ([]byte, error) {
					calls++

					return []byte("<urn:a> <urn:b> <" + src + "> ."), nil
				},
			})

			assert.Equal(t, tt.wantCalls, calls)

			if tt.wantErr {
				require.ErrorIs(t, err, errTooManyFetches)
				assert.Zero(t, s.Len())

				return
			}

			require.NoError(t, err)
		})
	}
}
