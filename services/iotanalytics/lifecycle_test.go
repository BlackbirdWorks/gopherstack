package iotanalytics_test

import (
	"context"
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

func TestLifecycleDelay_StatusProgression(t *testing.T) {
	t.Parallel()

	const delay = time.Minute

	tests := []struct {
		create func(*iotanalytics.InMemoryBackend) error
		status func(*iotanalytics.InMemoryBackend) (string, error)
		name   string
		before string
		after  string
	}{
		{
			name: "channel", before: "CREATING", after: "ACTIVE",
			create: func(b *iotanalytics.InMemoryBackend) error {
				_, err := b.CreateChannel(context.Background(), "c", nil, nil, nil)

				return err
			},
			status: func(b *iotanalytics.InMemoryBackend) (string, error) {
				c, err := b.DescribeChannel("c")
				if err != nil {
					return "", err
				}

				return c.Status, nil
			},
		},
		{
			name: "datastore", before: "CREATING", after: "ACTIVE",
			create: func(b *iotanalytics.InMemoryBackend) error {
				_, err := b.CreateDatastore(context.Background(), "d", nil, nil, nil, nil, nil)

				return err
			},
			status: func(b *iotanalytics.InMemoryBackend) (string, error) {
				d, err := b.DescribeDatastore("d")
				if err != nil {
					return "", err
				}

				return d.Status, nil
			},
		},
		{
			name: "dataset_content", before: "CREATING", after: "SUCCEEDED",
			create: func(b *iotanalytics.InMemoryBackend) error {
				b.AddDatasetInternal("ds")
				_, err := b.CreateDatasetContent("ds", "v1")

				return err
			},
			status: func(b *iotanalytics.InMemoryBackend) (string, error) {
				c, err := b.GetDatasetContent("ds", "v1")
				if err != nil {
					return "", err
				}

				return c.Status, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				b := iotanalytics.NewInMemoryBackend()
				b.SetLifecycleDelay(delay)
				require.NoError(t, tt.create(b))

				got, err := tt.status(b)
				require.NoError(t, err)
				assert.Equal(t, tt.before, got)

				time.Sleep(delay)

				got, err = tt.status(b)
				require.NoError(t, err)
				assert.Equal(t, tt.after, got)
			})
		})
	}
}

func TestListPagination_InvalidParams(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{name: "valid", query: "?maxResults=5", want: http.StatusOK},
		{name: "zero", query: "?maxResults=0", want: http.StatusBadRequest},
		{name: "too_big", query: "?maxResults=251", want: http.StatusBadRequest},
		{name: "not_a_number", query: "?maxResults=abc", want: http.StatusBadRequest},
		{name: "bad_token", query: "?nextToken=!!!", want: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, path := range []string{"/channels", "/datastores", "/datasets", "/pipelines"} {
				rec := doRequest(t, h, http.MethodGet, path+tt.query, nil)
				assert.Equal(t, tt.want, rec.Code, path)
			}
		})
	}
}
