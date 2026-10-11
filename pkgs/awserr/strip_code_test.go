package awserr_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func TestStripCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name, code, msg, want string
	}{
		{"colon prefix", "ResourceNotFoundException", "ResourceNotFoundException: table x", "table x"},
		{"bare code", "ActivityDoesNotExist", "ActivityDoesNotExist", "Activity does not exist"},
		{"code colon only", "ParameterNotFound", "ParameterNotFound: ", "Parameter not found"},
		{"dotted code", "InvalidId.NotFound", "InvalidId.NotFound", "Invalid id not found"},
		{"unrelated", "NotFound", "NotFoundish thing", "NotFoundish thing"},
		{"clean", "NotFound", "thing missing", "thing missing"},
		{"empty code", "", "x", "x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, awserr.StripCode(tt.code, tt.msg))
		})
	}
}
