package ociretry_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/ociretry"
)

func TestStore(t *testing.T) {
	t.Parallel()

	t0 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		at     time.Time
		name   string
		scope  string
		token  string
		wantOK bool
	}{
		{name: "hit", scope: "a", token: "t", at: t0.Add(time.Hour), wantOK: true},
		{name: "other scope", scope: "b", token: "t", at: t0, wantOK: false},
		{name: "expired", scope: "a", token: "t", at: t0.Add(ociretry.TTL), wantOK: false},
		{name: "empty token", scope: "a", token: "", at: t0, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := ociretry.New()
			s.Remember("a", "t", "v", t0)
			s.Remember("a", "", "ignored", t0)

			got, ok := s.Lookup(tt.scope, tt.token, tt.at)
			assert.Equal(t, tt.wantOK, ok)

			if ok {
				assert.Equal(t, "v", got)
			}

			c := ociretry.New()
			c.Load(s.Entries())
			_, ok = c.Lookup(tt.scope, tt.token, tt.at)
			assert.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestJoinSplit(t *testing.T) {
	t.Parallel()

	id, wr := ociretry.Split(ociretry.Join("ocid1.a", "ocid1.wr"))
	assert.Equal(t, "ocid1.a", id)
	assert.Equal(t, "ocid1.wr", wr)
}
