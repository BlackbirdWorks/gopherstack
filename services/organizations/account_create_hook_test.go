package organizations_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAccountCreateHook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		emails []string
		want   int
	}{
		{name: "one", emails: []string{"a@example.com"}, want: 1},
		{name: "two", emails: []string{"a@example.com", "b@example.com"}, want: 2},
		{name: "duplicate email fails", emails: []string{"a@example.com", "a@example.com"}, want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, _ := newOrgBackend(t)

			var got []string

			b.OnAccountCreate(func(id string) { got = append(got, id) })

			var ids []string

			for _, e := range tt.emails {
				st, err := b.CreateAccount("acct", e, "", "", nil)
				require.NoError(t, err)

				if st.AccountID != "" {
					ids = append(ids, st.AccountID)
				}
			}

			assert.Len(t, got, tt.want)
			assert.Equal(t, ids, got)
		})
	}
}
