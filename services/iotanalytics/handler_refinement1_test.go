package iotanalytics_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/iotanalytics"
)

// doRefinementRequest is a helper to make HTTP requests against the handler.
func doRefinementRequest(
	t *testing.T,
	h *iotanalytics.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *iotanalytics.InMemoryBackend)
		name     string
		wantChan int
		wantDS   int
		wantSet  int
		wantPipe int
	}{
		{
			name: "empty_backend",
		},
		{
			name: "populated_backend_clears_all",
			setup: func(b *iotanalytics.InMemoryBackend) {
				b.AddChannelInternal("c1")
				b.AddDatastoreInternal("d1")
				b.AddDatasetInternal("s1")
				b.AddPipelineInternal("p1")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}

			b.Reset()

			assert.Equal(t, 0, iotanalytics.ChannelCount(b))
			assert.Equal(t, 0, iotanalytics.DatastoreCount(b))
			assert.Equal(t, 0, iotanalytics.DatasetCount(b))
			assert.Equal(t, 0, iotanalytics.PipelineCount(b))
		})
	}
}

func TestRefinement1_MultipleResetCycle(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()

	for i := range 3 {
		b.AddChannelInternal("ch")
		b.AddDatastoreInternal("ds")
		b.Reset()

		assert.Equal(t, 0, iotanalytics.ChannelCount(b), "iteration %d", i)
		assert.Equal(t, 0, iotanalytics.DatastoreCount(b), "iteration %d", i)
	}
}

func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRefinementRequest(t, h, http.MethodPost, "/channels", map[string]any{"channelName": "ch1"})
	doRefinementRequest(t, h, http.MethodPost, "/channels", map[string]any{"channelName": "ch2"})

	h.Reset()

	rec := doRefinementRequest(t, h, http.MethodGet, "/channels", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"channelSummaries":[]`)
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &iotanalytics.Provider{}

	_, err := p.Init(nil)

	require.Error(t, err)
	assert.ErrorIs(t, err, iotanalytics.ErrNilAppContext)
}

func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	assert.Equal(t, 34, iotanalytics.HandlerOpsLen(h))
}

func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	assert.Len(t, ops, 34)
	assert.Len(t, ops, iotanalytics.HandlerOpsLen(h))
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		seedName string
		kind     string
	}{
		{name: "channel", seedName: "my_ch", kind: "channel"},
		{name: "datastore", seedName: "my_ds", kind: "datastore"},
		{name: "dataset", seedName: "my_set", kind: "dataset"},
		{name: "pipeline", seedName: "my_pipe", kind: "pipeline"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()

			switch tt.kind {
			case "channel":
				ch := b.AddChannelInternal(tt.seedName)
				require.NotNil(t, ch)
				assert.Equal(t, tt.seedName, ch.Name)
				assert.Equal(t, 1, iotanalytics.ChannelCount(b))
			case "datastore":
				ds := b.AddDatastoreInternal(tt.seedName)
				require.NotNil(t, ds)
				assert.Equal(t, tt.seedName, ds.Name)
				assert.Equal(t, 1, iotanalytics.DatastoreCount(b))
			case "dataset":
				d := b.AddDatasetInternal(tt.seedName)
				require.NotNil(t, d)
				assert.Equal(t, tt.seedName, d.Name)
				assert.Equal(t, 1, iotanalytics.DatasetCount(b))
			case "pipeline":
				p := b.AddPipelineInternal(tt.seedName)
				require.NotNil(t, p)
				assert.Equal(t, tt.seedName, p.Name)
				assert.Equal(t, 1, iotanalytics.PipelineCount(b))
			}
		})
	}
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()

	assert.Equal(t, 0, iotanalytics.ChannelCount(b))
	assert.Equal(t, 0, iotanalytics.DatastoreCount(b))
	assert.Equal(t, 0, iotanalytics.DatasetCount(b))
	assert.Equal(t, 0, iotanalytics.PipelineCount(b))

	b.AddChannelInternal("c1")
	b.AddChannelInternal("c2")
	assert.Equal(t, 2, iotanalytics.ChannelCount(b))

	b.AddDatastoreInternal("d1")
	assert.Equal(t, 1, iotanalytics.DatastoreCount(b))

	b.AddDatasetInternal("s1")
	assert.Equal(t, 1, iotanalytics.DatasetCount(b))

	b.AddPipelineInternal("p1")
	assert.Equal(t, 1, iotanalytics.PipelineCount(b))
}

func TestRefinement1_SortedListChannels(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	b.AddChannelInternal("zz_channel")
	b.AddChannelInternal("aa_channel")
	b.AddChannelInternal("mm_channel")

	channels := b.ListChannels()
	require.Len(t, channels, 3)
	assert.Equal(t, "aa_channel", channels[0].Name)
	assert.Equal(t, "mm_channel", channels[1].Name)
	assert.Equal(t, "zz_channel", channels[2].Name)
}

func TestRefinement1_SortedListDatastores(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	b.AddDatastoreInternal("z_store")
	b.AddDatastoreInternal("a_store")
	b.AddDatastoreInternal("m_store")

	stores := b.ListDatastores()
	require.Len(t, stores, 3)
	assert.Equal(t, "a_store", stores[0].Name)
	assert.Equal(t, "m_store", stores[1].Name)
	assert.Equal(t, "z_store", stores[2].Name)
}

func TestRefinement1_SortedListDatasets(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	b.AddDatasetInternal("z_set")
	b.AddDatasetInternal("a_set")
	b.AddDatasetInternal("m_set")

	sets := b.ListDatasets()
	require.Len(t, sets, 3)
	assert.Equal(t, "a_set", sets[0].Name)
	assert.Equal(t, "m_set", sets[1].Name)
	assert.Equal(t, "z_set", sets[2].Name)
}

func TestRefinement1_SortedListPipelines(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	b.AddPipelineInternal("z_pipe")
	b.AddPipelineInternal("a_pipe")
	b.AddPipelineInternal("m_pipe")

	pipes := b.ListPipelines()
	require.Len(t, pipes, 3)
	assert.Equal(t, "a_pipe", pipes[0].Name)
	assert.Equal(t, "m_pipe", pipes[1].Name)
	assert.Equal(t, "z_pipe", pipes[2].Name)
}

func TestRefinement1_SortedListTagsForResource(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	ch, err := b.CreateChannel("tagged_ch", map[string]string{
		"zzz": "last",
		"aaa": "first",
		"mmm": "middle",
	}, nil, nil)
	require.NoError(t, err)

	tags, err := b.ListTagsForResource(ch.ARN)
	require.NoError(t, err)
	require.Len(t, tags, 3)
	assert.Equal(t, "aaa", tags[0].Key)
	assert.Equal(t, "mmm", tags[1].Key)
	assert.Equal(t, "zzz", tags[2].Key)
}

func TestRefinement1_ErrAlreadyExists_Channel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRefinementRequest(t, h, http.MethodPost, "/channels", map[string]any{"channelName": "dup_ch"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRefinementRequest(t, h, http.MethodPost, "/channels", map[string]any{"channelName": "dup_ch"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestRefinement1_ErrAlreadyExists_Datastore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRefinementRequest(t, h, http.MethodPost, "/datastores", map[string]any{"datastoreName": "dup_ds"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRefinementRequest(t, h, http.MethodPost, "/datastores", map[string]any{"datastoreName": "dup_ds"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestRefinement1_ErrAlreadyExists_Dataset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRefinementRequest(t, h, http.MethodPost, "/datasets", map[string]any{"datasetName": "dup_set"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRefinementRequest(t, h, http.MethodPost, "/datasets", map[string]any{"datasetName": "dup_set"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestRefinement1_ErrAlreadyExists_Pipeline(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRefinementRequest(t, h, http.MethodPost, "/pipelines", map[string]any{"pipelineName": "dup_pipe"})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doRefinementRequest(t, h, http.MethodPost, "/pipelines", map[string]any{"pipelineName": "dup_pipe"})
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestRefinement1_NotFound_Returns404(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{name: "channel", method: http.MethodGet, path: "/channels/no-such-channel"},
		{name: "datastore", method: http.MethodGet, path: "/datastores/no-such-ds"},
		{name: "dataset", method: http.MethodGet, path: "/datasets/no-such-set"},
		{name: "pipeline", method: http.MethodGet, path: "/pipelines/no-such-pipe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRefinementRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, http.StatusNotFound, rec.Code)
		})
	}
}

func TestRefinement1_DeepCopy_Channel(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	_, err := b.CreateChannel("immutable_ch", map[string]string{"key": "original"}, nil, nil)
	require.NoError(t, err)

	ch, err := b.DescribeChannel("immutable_ch")
	require.NoError(t, err)

	// Mutate the returned copy; stored state should be unchanged.
	ch.Tags["key"] = "mutated"
	ch.Name = "mutated_name"

	ch2, err := b.DescribeChannel("immutable_ch")
	require.NoError(t, err)
	assert.Equal(t, "immutable_ch", ch2.Name)
	assert.Equal(t, "original", ch2.Tags["key"])
}

func TestRefinement1_DeepCopy_Pipeline(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	_, err := b.CreatePipeline("immutable_pipe", map[string]string{"env": "prod"}, nil)
	require.NoError(t, err)

	p, err := b.DescribePipeline("immutable_pipe")
	require.NoError(t, err)

	// Mutate the returned copy.
	p.Tags["env"] = "dev"
	p.Name = "mutated"

	p2, err := b.DescribePipeline("immutable_pipe")
	require.NoError(t, err)
	assert.Equal(t, "immutable_pipe", p2.Name)
	assert.Equal(t, "prod", p2.Tags["env"])
}

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()
	b.AddChannelInternal("saved_ch")
	b.AddDatastoreInternal("saved_ds")
	b.AddDatasetInternal("saved_set")
	b.AddPipelineInternal("saved_pipe")

	snap := b.Snapshot()
	require.NotNil(t, snap)
	require.NotEmpty(t, snap)

	b2 := iotanalytics.NewInMemoryBackend()
	err := b2.Restore(snap)
	require.NoError(t, err)

	assert.Equal(t, 1, iotanalytics.ChannelCount(b2))
	assert.Equal(t, 1, iotanalytics.DatastoreCount(b2))
	assert.Equal(t, 1, iotanalytics.DatasetCount(b2))
	assert.Equal(t, 1, iotanalytics.PipelineCount(b2))

	ch, err := b2.DescribeChannel("saved_ch")
	require.NoError(t, err)
	assert.Equal(t, "saved_ch", ch.Name)
}

func TestRefinement1_NonNilReprocessingSummaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		pipelineName string
	}{
		{name: "new_pipeline_has_empty_map", pipelineName: "fresh_pipe"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()
			p, err := b.CreatePipeline(tt.pipelineName, nil, nil)
			require.NoError(t, err)
			require.NotNil(t, p.Reprocessings)
			assert.Empty(t, p.Reprocessings)
		})
	}
}

func TestRefinement1_ErrSentinels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		err     error
		wantMsg string
	}{
		{
			name:    "ErrValidation",
			err:     iotanalytics.ErrValidation,
			wantMsg: "validation error",
		},
		{
			name:    "ErrAlreadyExists",
			err:     iotanalytics.ErrAlreadyExists,
			wantMsg: "resource already exists",
		},
		{
			name:    "ErrResourceNotFound",
			err:     iotanalytics.ErrResourceNotFound,
			wantMsg: "resource not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Error(t, tt.err)
			assert.Contains(t, tt.err.Error(), tt.wantMsg)
		})
	}
}

func TestRefinement1_TagsInDescribeResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		path   string
		create string
	}{
		{
			name:   "channel_with_tags",
			path:   "/channels/tagged_ch",
			create: "/channels",
			body: map[string]any{
				"channelName": "tagged_ch",
				"tags":        []map[string]string{{"key": "env", "value": "prod"}},
			},
		},
		{
			name:   "pipeline_with_tags",
			path:   "/pipelines/tagged_pipe",
			create: "/pipelines",
			body: map[string]any{
				"pipelineName": "tagged_pipe",
				"tags":         []map[string]string{{"key": "team", "value": "alpha"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRefinementRequest(t, h, http.MethodPost, tt.create, tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doRefinementRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, rec2.Code)
			assert.Contains(t, rec2.Body.String(), `"tags"`)
		})
	}
}

func TestRefinement1_ErrAlreadyExists_BackendDirect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(b *iotanalytics.InMemoryBackend) error
		name   string
	}{
		{
			name: "channel",
			create: func(b *iotanalytics.InMemoryBackend) error {
				_, err := b.CreateChannel("dup", nil, nil, nil)

				return err
			},
		},
		{
			name: "datastore",
			create: func(b *iotanalytics.InMemoryBackend) error {
				_, err := b.CreateDatastore("dup", nil, nil, nil, nil, nil)

				return err
			},
		},
		{
			name: "dataset",
			create: func(b *iotanalytics.InMemoryBackend) error {
				_, err := b.CreateDataset("dup", nil, nil, nil, nil, nil, nil)

				return err
			},
		},
		{
			name: "pipeline",
			create: func(b *iotanalytics.InMemoryBackend) error {
				_, err := b.CreatePipeline("dup", nil, nil)

				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := iotanalytics.NewInMemoryBackend()
			require.NoError(t, tt.create(b))

			err := tt.create(b)
			require.Error(t, err)
			assert.ErrorIs(t, err, iotanalytics.ErrAlreadyExists)
		})
	}
}

func TestRefinement1_ResourceNotFoundForTags(t *testing.T) {
	t.Parallel()

	b := iotanalytics.NewInMemoryBackend()

	_, err := b.ListTagsForResource("arn:aws:iotanalytics:us-east-1:000000000000:channel/no-such")
	require.Error(t, err)
	assert.ErrorIs(t, err, iotanalytics.ErrResourceNotFound)
}
