package eventbridge_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIDestination_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "api-dest-conn",
		AuthorizationType: "API_KEY",
	})
	require.NoError(t, err)

	dst, err := b.CreateAPIDestination(context.Background(), eventbridge.CreateAPIDestinationInput{
		Name:                         "my-api-dest",
		ConnectionArn:                conn.ConnectionArn,
		HTTPMethod:                   "POST",
		InvocationEndpoint:           "https://example.com/webhook",
		InvocationRateLimitPerSecond: 300,
	})
	require.NoError(t, err)
	assert.Equal(t, "my-api-dest", dst.Name)
	assert.Equal(t, "ACTIVE", dst.APIDestinationState)
	assert.Equal(t, 300, dst.InvocationRateLimitPerSecond)

	described, err := b.DescribeAPIDestination(context.Background(), "my-api-dest")
	require.NoError(t, err)
	assert.Equal(t, "POST", described.HTTPMethod)

	updated, err := b.UpdateAPIDestination(context.Background(), eventbridge.UpdateAPIDestinationInput{
		Name:       "my-api-dest",
		HTTPMethod: "PUT",
	})
	require.NoError(t, err)
	assert.Equal(t, "PUT", updated.HTTPMethod)

	dsts, _, err := b.ListAPIDestinations(context.Background(), "my-api-", "")
	require.NoError(t, err)
	assert.Len(t, dsts, 1)

	err = b.DeleteAPIDestination(context.Background(), "my-api-dest")
	require.NoError(t, err)

	_, err = b.DescribeAPIDestination(context.Background(), "my-api-dest")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestAPIDestination_InvalidHTTPMethod(t *testing.T) {
	t.Parallel()

	invalid := []string{"TRACE", "CONNECT", ""}
	for _, method := range invalid {
		t.Run("invalid-"+method, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateAPIDestination(context.Background(), eventbridge.CreateAPIDestinationInput{
				Name:               "dest",
				ConnectionArn:      "arn:aws:events:us-east-1:123456789012:connection/c",
				HTTPMethod:         method,
				InvocationEndpoint: "https://example.com",
			})
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

func TestAPIDestination_ValidHTTPMethods(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name: "ref-conn", AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	conn, _ := b.DescribeConnection(context.Background(), "ref-conn")

	valid := []string{"GET", "HEAD", "POST", "OPTIONS", "PUT", "DELETE", "PATCH"}
	for i, method := range valid {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			bLocal := newBackend()
			_, createErr := bLocal.CreateAPIDestination(context.Background(), eventbridge.CreateAPIDestinationInput{
				Name:               fmt.Sprintf("dest-%d", i),
				ConnectionArn:      conn.ConnectionArn,
				HTTPMethod:         method,
				InvocationEndpoint: "https://example.com/" + method,
			})
			require.NoError(t, createErr)
		})
	}
}

func TestCreateAPIDestination_HTTPMethodValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		method  string
	}{
		{name: "GET allowed", method: "GET"},
		{name: "POST allowed", method: "POST"},
		{name: "PUT allowed", method: "PUT"},
		{name: "DELETE allowed", method: "DELETE"},
		{name: "PATCH allowed", method: "PATCH"},
		{name: "HEAD allowed", method: "HEAD"},
		{name: "OPTIONS allowed", method: "OPTIONS"},
		{name: "INVALID rejected", method: "CONNECT", wantErr: eventbridge.ErrInvalidParameter},
		{name: "empty rejected", method: "", wantErr: eventbridge.ErrInvalidParameter},
		{name: "TRACE rejected", method: "TRACE", wantErr: eventbridge.ErrInvalidParameter},
	}

	connARN := "arn:aws:events:us-east-1:123:connection/test"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateAPIDestination(context.Background(), eventbridge.CreateAPIDestinationInput{
				Name:               "dst-" + tt.name,
				ConnectionArn:      connARN,
				InvocationEndpoint: "https://example.com/hook",
				HTTPMethod:         tt.method,
			})
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestAPIDestinationCRUD(t *testing.T) {
	t.Parallel()

	t.Run("describe not found", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		_, err := b.DescribeAPIDestination(context.Background(), "missing")
		require.ErrorIs(t, err, eventbridge.ErrNotFound)
	})

	t.Run("round trip", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		b.AddAPIDestinationInternal(&eventbridge.APIDestination{
			Name:                "dst1",
			ConnectionArn:       "arn:aws:events:us-east-1:123:connection/c1",
			HTTPMethod:          "POST",
			InvocationEndpoint:  "https://example.com/hook",
			APIDestinationState: "ACTIVE",
		})

		got, err := b.DescribeAPIDestination(context.Background(), "dst1")
		require.NoError(t, err)
		assert.Equal(t, "dst1", got.Name)

		updated, err := b.UpdateAPIDestination(context.Background(), eventbridge.UpdateAPIDestinationInput{
			Name:        "dst1",
			Description: "desc updated",
		})
		require.NoError(t, err)
		assert.Equal(t, "desc updated", updated.Description)

		dsts, _, err := b.ListAPIDestinations(context.Background(), "", "")
		require.NoError(t, err)
		assert.Len(t, dsts, 1)
	})
}

// TestCreateAPIDestination_RequiresName verifies name validation.
func TestCreateAPIDestination_RequiresName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input eventbridge.CreateAPIDestinationInput
	}{
		{
			name: "empty_name",
			input: eventbridge.CreateAPIDestinationInput{
				ConnectionArn:      "arn:x",
				InvocationEndpoint: "https://x",
				HTTPMethod:         "POST",
			},
		},
		{
			name: "empty_connection_arn",
			input: eventbridge.CreateAPIDestinationInput{
				Name:               "d1",
				InvocationEndpoint: "https://x",
				HTTPMethod:         "POST",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackend()
			_, err := b.CreateAPIDestination(context.Background(), tt.input)
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

// TestCreateAPIDestination_DuplicateRejected verifies duplicate names return ErrAlreadyExists.
func TestCreateAPIDestination_DuplicateRejected(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	input := eventbridge.CreateAPIDestinationInput{
		Name:               "my-dest",
		ConnectionArn:      "arn:aws:events:us-east-1:123:connection/my-conn",
		InvocationEndpoint: "https://example.com/api",
		HTTPMethod:         "POST",
	}

	_, err := b.CreateAPIDestination(context.Background(), input)
	require.NoError(t, err)

	_, err = b.CreateAPIDestination(context.Background(), input)
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

// TestCreateAPIDestination_RoundTrip creates an API destination and verifies ARN is set.
func TestCreateAPIDestination_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	input := eventbridge.CreateAPIDestinationInput{
		Name:               "my-dest",
		ConnectionArn:      "arn:aws:events:us-east-1:123:connection/my-conn",
		InvocationEndpoint: "https://example.com/api",
		HTTPMethod:         "POST",
	}

	dst, err := b.CreateAPIDestination(context.Background(), input)
	require.NoError(t, err)

	assert.NotEmpty(t, dst.APIDestinationArn)
	assert.Equal(t, "ACTIVE", dst.APIDestinationState)
	assert.Equal(t, "my-dest", dst.Name)
	assert.Contains(t, dst.APIDestinationArn, "api-destination/my-dest")
	assert.Equal(t, 1, b.APIDestinationCount())
}

// TestDeleteAPIDestination_NotFound verifies deletion of non-existent returns ErrNotFound.
func TestDeleteAPIDestination_NotFound(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	err := b.DeleteAPIDestination(context.Background(), "no-such-dest")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

// TestDeleteAPIDestination_RoundTrip creates then deletes an API destination.
func TestDeleteAPIDestination_RoundTrip(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()
	_, err := b.CreateAPIDestination(context.Background(), eventbridge.CreateAPIDestinationInput{
		Name:               "my-dest",
		ConnectionArn:      "arn:aws:events:us-east-1:123:connection/c1",
		InvocationEndpoint: "https://x.com",
		HTTPMethod:         "POST",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, b.APIDestinationCount())

	err = b.DeleteAPIDestination(context.Background(), "my-dest")
	require.NoError(t, err)
	assert.Equal(t, 0, b.APIDestinationCount())
}
