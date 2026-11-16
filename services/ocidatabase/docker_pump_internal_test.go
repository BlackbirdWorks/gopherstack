package ocidatabase

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPumpRefusesPasswordsThatBreakTheParameterFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
	}{
		{name: "newline", password: "Passw0rd_ok_12\nschemas=SYS"},
		{name: "carriage return", password: "Passw0rd_ok_12\rx"},
		{name: "nul", password: "Passw0rd_ok_12\x00x"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := &dockerInstance{password: tt.password}
			require.ErrorIs(t, d.pump(t.Context(), "expdp", "schemas=ADMIN"), errBadPassword)
		})
	}
}
