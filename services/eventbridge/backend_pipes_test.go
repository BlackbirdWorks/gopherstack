package eventbridge_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipe_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	pipe, err := b.CreatePipe(context.Background(), eventbridge.CreatePipeInput{
		Name:      "my-pipe",
		SourceArn: "arn:aws:sqs:us-east-1:123456789012:source-queue",
		TargetArn: "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
		RoleArn:   "arn:aws:iam::123456789012:role/my-pipe-role",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-pipe", pipe.Name)
	assert.NotEmpty(t, pipe.Arn)
	assert.Equal(t, "CREATING", pipe.CurrentState)

	described, err := b.DescribePipe(context.Background(), "my-pipe")
	require.NoError(t, err)
	assert.Equal(t, "my-pipe", described.Name)

	pipes, _, err := b.ListPipes(context.Background(), "", "")
	require.NoError(t, err)
	require.Len(t, pipes, 1)

	updated, err := b.UpdatePipe(context.Background(), eventbridge.UpdatePipeInput{
		Name:        "my-pipe",
		Description: "updated description",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", updated.Description)

	err = b.DeletePipe(context.Background(), "my-pipe")
	require.NoError(t, err)

	_, err = b.DescribePipe(context.Background(), "my-pipe")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestPipe_CreateRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input eventbridge.CreatePipeInput
	}{
		{"missing name", eventbridge.CreatePipeInput{SourceArn: "s", TargetArn: "t", RoleArn: "r"}},
		{"missing source", eventbridge.CreatePipeInput{Name: "p", TargetArn: "t", RoleArn: "r"}},
		{"missing target", eventbridge.CreatePipeInput{Name: "p", SourceArn: "s", RoleArn: "r"}},
		{"missing role", eventbridge.CreatePipeInput{Name: "p", SourceArn: "s", TargetArn: "t"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreatePipe(context.Background(), tt.input)
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

func TestPipe_DuplicateCreateFails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	input := eventbridge.CreatePipeInput{
		Name:      "dup-pipe",
		SourceArn: "arn:aws:sqs:us-east-1:123456789012:q",
		TargetArn: "arn:aws:lambda:us-east-1:123456789012:function:f",
		RoleArn:   "arn:aws:iam::123456789012:role/r",
	}

	_, err := b.CreatePipe(context.Background(), input)
	require.NoError(t, err)

	_, err = b.CreatePipe(context.Background(), input)
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

func TestPipe_ListFiltersPrefix(t *testing.T) {
	t.Parallel()
	b := newBackend()

	for _, name := range []string{"foo-1", "foo-2", "bar-1"} {
		_, err := b.CreatePipe(context.Background(), eventbridge.CreatePipeInput{
			Name:      name,
			SourceArn: "arn:aws:sqs:us-east-1:123456789012:q",
			TargetArn: "arn:aws:lambda:us-east-1:123456789012:function:f",
			RoleArn:   "arn:aws:iam::123456789012:role/r",
		})
		require.NoError(t, err)
	}

	pipes, _, err := b.ListPipes(context.Background(), "foo-", "")
	require.NoError(t, err)
	assert.Len(t, pipes, 2)
}

func TestPipe_DesiredStatePreserved(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePipe(context.Background(), eventbridge.CreatePipeInput{
		Name:         "my-pipe",
		SourceArn:    "arn:aws:sqs:us-east-1:123456789012:q",
		TargetArn:    "arn:aws:lambda:us-east-1:123456789012:function:f",
		RoleArn:      "arn:aws:iam::123456789012:role/r",
		DesiredState: "STOPPED",
	})
	require.NoError(t, err)

	p, err := b.DescribePipe(context.Background(), "my-pipe")
	require.NoError(t, err)
	assert.Equal(t, "STOPPED", p.DesiredState)
}

func TestTags_Pipe(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	pipe, err := b.CreatePipe(context.Background(), eventbridge.CreatePipeInput{
		Name:      "tagged-pipe",
		SourceArn: "arn:aws:sqs:us-east-1:123456789012:source-q",
		TargetArn: "arn:aws:lambda:us-east-1:123456789012:function:fn",
		RoleArn:   "arn:aws:iam::123456789012:role/r",
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "TagResource", map[string]any{
		"ResourceARN": pipe.Arn,
		"Tags":        []map[string]string{{"Key": "stage", "Value": "prod"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": pipe.Arn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "prod")
}
