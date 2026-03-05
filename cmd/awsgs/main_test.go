package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildArgs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		args     []string
		endpoint string
		want     []string
	}{
		{
			name:     "injects_before_service",
			args:     []string{"s3", "ls"},
			endpoint: "http://localhost:8000",
			want:     []string{"--endpoint-url", "http://localhost:8000", "s3", "ls"},
		},
		{
			name:     "endpoint_already_present_no_duplicate",
			args:     []string{"s3", "ls", "--endpoint-url", "http://custom:9000"},
			endpoint: "http://localhost:8000",
			want:     []string{"s3", "ls", "--endpoint-url", "http://custom:9000"},
		},
		{
			name:     "endpoint_equals_form_no_duplicate",
			args:     []string{"s3", "ls", "--endpoint-url=http://custom:9000"},
			endpoint: "http://localhost:8000",
			want:     []string{"s3", "ls", "--endpoint-url=http://custom:9000"},
		},
		{
			name:     "global_flags_before_service",
			args:     []string{"--region", "eu-west-1", "s3", "ls"},
			endpoint: "http://localhost:8000",
			want:     []string{"--endpoint-url", "http://localhost:8000", "--region", "eu-west-1", "s3", "ls"},
		},
		{
			name:     "empty_args",
			args:     []string{},
			endpoint: "http://localhost:8000",
			want:     []string{"--endpoint-url", "http://localhost:8000"},
		},
		{
			name:     "custom_endpoint_injected",
			args:     []string{"sqs", "list-queues"},
			endpoint: "http://localhost:9000",
			want:     []string{"--endpoint-url", "http://localhost:9000", "sqs", "list-queues"},
		},
		{
			name:     "operation_args_preserved",
			args:     []string{"sqs", "create-queue", "--queue-name", "my-queue"},
			endpoint: "http://localhost:8000",
			want:     []string{"--endpoint-url", "http://localhost:8000", "sqs", "create-queue", "--queue-name", "my-queue"},
		},
		{
			name:     "only_flags_no_service",
			args:     []string{"--region", "us-east-1"},
			endpoint: "http://localhost:8000",
			want:     []string{"--endpoint-url", "http://localhost:8000", "--region", "us-east-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := buildArgs(tt.args, tt.endpoint)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestParseAwsgsFlags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		args      []string
		wantHost  string
		wantPort  string
		wantRest  []string
	}{
		{
			name:     "no_flags",
			args:     []string{"s3", "ls"},
			wantHost: defaultHost,
			wantPort: defaultPort,
			wantRest: []string{"s3", "ls"},
		},
		{
			name:     "port_flag",
			args:     []string{"--awsgs-port", "9000", "s3", "ls"},
			wantHost: defaultHost,
			wantPort: "9000",
			wantRest: []string{"s3", "ls"},
		},
		{
			name:     "port_equals_form",
			args:     []string{"--awsgs-port=9000", "s3", "ls"},
			wantHost: defaultHost,
			wantPort: "9000",
			wantRest: []string{"s3", "ls"},
		},
		{
			name:     "host_flag",
			args:     []string{"--awsgs-host", "remote.host", "s3", "ls"},
			wantHost: "remote.host",
			wantPort: defaultPort,
			wantRest: []string{"s3", "ls"},
		},
		{
			name:     "host_equals_form",
			args:     []string{"--awsgs-host=remote.host", "s3", "ls"},
			wantHost: "remote.host",
			wantPort: defaultPort,
			wantRest: []string{"s3", "ls"},
		},
		{
			name:     "both_flags",
			args:     []string{"--awsgs-port", "9000", "--awsgs-host", "myhost", "dynamodb", "list-tables"},
			wantHost: "myhost",
			wantPort: "9000",
			wantRest: []string{"dynamodb", "list-tables"},
		},
		{
			name:     "flags_stripped_from_rest",
			args:     []string{"--awsgs-port", "1234", "sqs", "list-queues"},
			wantHost: defaultHost,
			wantPort: "1234",
			wantRest: []string{"sqs", "list-queues"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			host, port, rest := parseAwsgsFlags(tt.args)
			assert.Equal(t, tt.wantHost, host)
			assert.Equal(t, tt.wantPort, port)
			require.Equal(t, tt.wantRest, rest)
		})
	}
}

func TestResolvePort(t *testing.T) {
	// Not parallel at the top level: t.Setenv requires sequential sub-tests.
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "default_when_no_env",
			env:  map[string]string{},
			want: defaultPort,
		},
		{
			name: "awsgs_port_takes_precedence",
			env:  map[string]string{"AWSGS_PORT": "9000", "GOPHERSTACK_PORT": "7000"},
			want: "9000",
		},
		{
			name: "gopherstack_port_fallback",
			env:  map[string]string{"GOPHERSTACK_PORT": "7000"},
			want: "7000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			got := resolvePort()
			assert.Equal(t, tt.want, got)
		})
	}
}
