package ecs

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// newWrappedTestErr builds an error in the same shape the backend produces:
// fmt.Errorf("%w: detail", awserr.New(code, ...)).
func newWrappedTestErr(code, detail string) error {
	return fmt.Errorf("%w: %s", awserr.New(code, awserr.ErrInvalidParameter), detail)
}

// TestRegisterTaskDefinition_ContainerValidation exercises the structural
// container-definition rules that real AWS ECS enforces and that surface as a
// ClientException.
func TestRegisterTaskDefinition_ContainerValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantCode  string
		wantInMsg string
		input     RegisterTaskDefinitionInput
		wantErr   bool
	}{
		{
			name: "valid single container",
			input: RegisterTaskDefinitionInput{
				Family:               "ok",
				ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx:latest"}},
			},
			wantErr: false,
		},
		{
			name:      "empty container definitions",
			input:     RegisterTaskDefinitionInput{Family: "empty"},
			wantErr:   true,
			wantCode:  "ClientException",
			wantInMsg: "should not be empty",
		},
		{
			name: "missing container name",
			input: RegisterTaskDefinitionInput{
				Family:               "noname",
				ContainerDefinitions: []ContainerDefinition{{Image: "nginx"}},
			},
			wantErr:   true,
			wantCode:  "ClientException",
			wantInMsg: "container name is required",
		},
		{
			name: "missing container image",
			input: RegisterTaskDefinitionInput{
				Family:               "noimg",
				ContainerDefinitions: []ContainerDefinition{{Name: "app"}},
			},
			wantErr:   true,
			wantCode:  "ClientException",
			wantInMsg: "must specify an image",
		},
		{
			name: "invalid container name characters",
			input: RegisterTaskDefinitionInput{
				Family:               "badname",
				ContainerDefinitions: []ContainerDefinition{{Name: "bad name!", Image: "nginx"}},
			},
			wantErr:   true,
			wantCode:  "ClientException",
			wantInMsg: "is invalid",
		},
		{
			name: "duplicate container names",
			input: RegisterTaskDefinitionInput{
				Family: "dup",
				ContainerDefinitions: []ContainerDefinition{
					{Name: "app", Image: "nginx"},
					{Name: "app", Image: "redis"},
				},
			},
			wantErr:   true,
			wantCode:  "ClientException",
			wantInMsg: "used more than once",
		},
		{
			name: "invalid network mode",
			input: RegisterTaskDefinitionInput{
				Family:               "badnet",
				NetworkMode:          "vlan",
				ContainerDefinitions: []ContainerDefinition{{Name: "app", Image: "nginx"}},
			},
			wantErr:   true,
			wantCode:  "ClientException",
			wantInMsg: "network mode",
		},
		{
			name: "awsvpc host port mismatch",
			input: RegisterTaskDefinitionInput{
				Family:      "awsvpc-mismatch",
				NetworkMode: networkModeAwsvpc,
				ContainerDefinitions: []ContainerDefinition{
					{
						Name:         "app",
						Image:        "nginx",
						PortMappings: []PortMapping{{ContainerPort: 80, HostPort: 8080}},
					},
				},
			},
			wantErr:   true,
			wantCode:  "ClientException",
			wantInMsg: "must match",
		},
		{
			name: "awsvpc matching host port ok",
			input: RegisterTaskDefinitionInput{
				Family:      "awsvpc-ok",
				NetworkMode: networkModeAwsvpc,
				ContainerDefinitions: []ContainerDefinition{
					{
						Name:         "app",
						Image:        "nginx",
						PortMappings: []PortMapping{{ContainerPort: 80, HostPort: 80}},
					},
				},
			},
			wantErr: false,
		},
		{
			name: "fargate requires awsvpc",
			input: RegisterTaskDefinitionInput{
				Family:                  "fg-net",
				RequiresCompatibilities: []string{launchTypeFargate},
				CPU:                     "256",
				Memory:                  "512",
				ContainerDefinitions:    []ContainerDefinition{{Name: "app", Image: "nginx"}},
			},
			wantErr:   true,
			wantCode:  "ClientException",
			wantInMsg: "awsvpc is required",
		},
		{
			name: "fargate requires cpu and memory",
			input: RegisterTaskDefinitionInput{
				Family:                  "fg-cpu",
				RequiresCompatibilities: []string{launchTypeFargate},
				NetworkMode:             networkModeAwsvpc,
				ContainerDefinitions:    []ContainerDefinition{{Name: "app", Image: "nginx"}},
			},
			wantErr:   true,
			wantCode:  "ClientException",
			wantInMsg: "CPU and memory are required",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend()

			_, err := b.RegisterTaskDefinition(tt.input)

			if !tt.wantErr {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}

				return
			}

			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if !errors.Is(err, awserr.ErrInvalidParameter) {
				t.Errorf("error should wrap ErrInvalidParameter for 400 routing: %v", err)
			}

			if got := errorCode(err); got != tt.wantCode {
				t.Errorf("errorCode = %q, want %q", got, tt.wantCode)
			}

			if !strings.Contains(err.Error(), tt.wantInMsg) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantInMsg)
			}
		})
	}
}

// TestErrorCode_ReturnsBareExceptionCode verifies that errorCode surfaces only
// the bare AWS exception code (the value placed in __type / x-amzn-errortype)
// rather than the full human-readable message.
func TestErrorCode_ReturnsBareExceptionCode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "client exception",
			err:  ErrClient,
			want: "ClientException",
		},
		{
			name: "wrapped invalid parameter",
			err:  newWrappedTestErr("InvalidParameterException", "family is required"),
			want: "InvalidParameterException",
		},
		{
			name: "wrapped client exception with detail",
			err:  newWrappedTestErr("ClientException", "container name is required"),
			want: "ClientException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := errorCode(tt.err); got != tt.want {
				t.Errorf("errorCode = %q, want %q", got, tt.want)
			}
		})
	}
}
