package rds_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func TestMySQLAccountQuoting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		user    string
		want    string
		wantErr bool
	}{
		{name: "plain", user: "admin_1", want: "'admin_1'@'%'"},
		{name: "quote", user: "a'b", wantErr: true},
		{name: "injection", user: "x'@'%'; DROP USER y; --", wantErr: true},
		{name: "empty", user: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := rds.MySQLAccount(tt.user)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
