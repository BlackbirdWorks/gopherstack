package testsnap_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/testsnap"
)

type sample struct {
	M map[string]string
	P *int
	S []string
}

type recorder struct {
	errs int
}

func (r *recorder) Helper() {}

func (r *recorder) Errorf(string, ...any) { r.errs++ }

func TestUnchanged(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate  func(*sample)
		name    string
		wantErr bool
	}{
		{name: "noop", mutate: func(*sample) {}},
		{name: "replace_map", mutate: func(s *sample) { s.M = map[string]string{"x": "y"} }, wantErr: true},
		{name: "mutate_map", mutate: func(s *sample) { s.M["k"] = "changed" }, wantErr: true},
		{name: "mutate_slice", mutate: func(s *sample) { s.S[0] = "changed" }, wantErr: true},
		{name: "mutate_ptr", mutate: func(s *sample) { *s.P = 9 }, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			n := 1
			held := &sample{M: map[string]string{"k": "v"}, P: &n, S: []string{"a"}}
			rec := &recorder{}

			testsnap.Unchanged(rec, held, func() { tt.mutate(held) })

			assert.Equal(t, tt.wantErr, rec.errs > 0, fmt.Sprint(rec.errs))
		})
	}
}
