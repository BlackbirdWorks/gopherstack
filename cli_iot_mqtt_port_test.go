package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

//nolint:paralleltest // uses t.Setenv via parseCLI
func TestCLI_IoTMQTTPort(t *testing.T) {
	tests := []struct {
		env  map[string]string
		name string
		want int
	}{
		{name: "default", env: nil, want: 1883},
		{name: "env", env: map[string]string{"IOT_MQTT_PORT": "0"}, want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cli := parseCLI(t, tt.env)
			assert.Equal(t, tt.want, cli.GetIoTSettings().MQTTPort)
		})
	}
}
