package kinesisanalyticsv2_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesisanalyticsv2"
)

func TestCreateApplication_DefaultMaintenanceWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		region  string
		runtime string
		want    string
	}{
		{name: "virginia", region: "us-east-1", runtime: "FLINK-1_18", want: "03:00"},
		{name: "oregon", region: "us-west-2", runtime: "FLINK-1_18", want: "06:00"},
		{name: "mumbai", region: "ap-south-1", runtime: "FLINK-1_18", want: "16:30"},
		{name: "sql runtime", region: "us-east-1", runtime: "SQL-1_0", want: ""},
		{name: "unlisted region", region: "eu-west-2", runtime: "FLINK-1_18", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesisanalyticsv2.NewInMemoryBackend("000000000000", tt.region)
			app, err := b.CreateApplication(context.Background(), "app", tt.runtime, "", "", "", nil)
			require.NoError(t, err)
			assert.Equal(t, tt.want, app.MaintenanceWindowStartTime)
		})
	}
}
