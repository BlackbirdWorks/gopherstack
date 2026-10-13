package pipes_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"
	pipestypes "github.com/aws/aws-sdk-go-v2/service/pipes/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

func TestCreatePipe_ValidationRealism(t *testing.T) {
	t.Parallel()

	const (
		role   = "arn:aws:iam::123456789012:role/r"
		source = "arn:aws:sqs:us-east-1:123456789012:src"
		target = "arn:aws:sqs:us-east-1:123456789012:dst"
	)

	tests := []struct {
		name    string
		pipe    string
		role    string
		target  string
		desc    string
		wantErr string
	}{
		{name: "valid", pipe: "p", role: role, target: target},
		{name: "dot in name", pipe: "my.pipe", role: role, target: target},
		{name: "space in name", pipe: "my pipe", role: role, target: target, wantErr: "ValidationException"},
		{name: "role not arn", pipe: "p", role: "role", target: target, wantErr: "ValidationException"},
		{name: "role not iam", pipe: "p", role: source, target: target, wantErr: "ValidationException"},
		{name: "target not arn", pipe: "p", role: role, target: "queue", wantErr: "ValidationException"},
		{
			name: "description too long", pipe: "p", role: role, target: target,
			desc: strings.Repeat("x", 513), wantErr: "ValidationException",
		},
		{name: "description at limit", pipe: "p", role: role, target: target, desc: strings.Repeat("x", 512)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestPipesClient(t, pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-east-1")))
			_, err := client.CreatePipe(t.Context(), &pipessdk.CreatePipeInput{
				Name:        aws.String(tt.pipe),
				RoleArn:     aws.String(tt.role),
				Source:      aws.String(source),
				Target:      aws.String(tt.target),
				Description: aws.String(tt.desc),
			})

			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			var ve *pipestypes.ValidationException
			require.ErrorAs(t, err, &ve)
			assert.NotContains(t, ve.ErrorMessage(), "ValidationException:")
		})
	}
}

func TestListPipes_LimitBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		limit   int32
		wantErr bool
	}{
		{name: "min", limit: 1},
		{name: "max", limit: 100},
		{name: "over max", limit: 101, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newTestPipesClient(t, pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-east-1")))
			_, err := client.ListPipes(t.Context(), &pipessdk.ListPipesInput{Limit: aws.Int32(tt.limit)})

			if tt.wantErr {
				var ve *pipestypes.ValidationException
				require.ErrorAs(t, err, &ve)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDescribePipe_NotFoundMessage(t *testing.T) {
	t.Parallel()

	client := newTestPipesClient(t, pipes.NewHandler(pipes.NewInMemoryBackend("123456789012", "us-east-1")))
	_, err := client.DescribePipe(t.Context(), &pipessdk.DescribePipeInput{Name: aws.String("nope")})

	var nf *pipestypes.NotFoundException
	require.ErrorAs(t, err, &nf)
	assert.Equal(t, "pipe nope not found", nf.ErrorMessage())
}
