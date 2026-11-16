package sagemaker_test

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

type multiModelServer struct {
	loads  map[string]int
	hosts  map[string]string
	mu     sync.Mutex
	status int
}

func (m *multiModelServer) handler(spec container.Spec) http.Handler {
	mux := http.NewServeMux()
	modelsDir := mountSource(spec, "/opt/ml/models")

	mux.HandleFunc("/ping", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("POST /models", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Name string `json:"model_name"`
			URL  string `json:"url"`
		}

		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &body, jsonx.V1()); err != nil {
			w.WriteHeader(http.StatusBadRequest)

			return
		}

		m.mu.Lock()
		defer m.mu.Unlock()

		if m.status != 0 {
			w.WriteHeader(m.status)

			return
		}

		m.loads[body.Name]++
		m.hosts[body.Name] = filepath.Join(modelsDir, body.Name, "model")
	})
	mux.HandleFunc("POST /models/{name}/invoke", func(w http.ResponseWriter, r *http.Request) {
		m.mu.Lock()
		host, ok := m.hosts[r.PathValue("name")]
		m.mu.Unlock()

		if !ok {
			w.WriteHeader(http.StatusNotFound)

			return
		}

		in, _ := io.ReadAll(r.Body)
		_, _ = w.Write([]byte(readFileOrEmpty(filepath.Join(host, "weights.txt")) + "|" + string(in) + "|" +
			r.Header.Get("X-Amzn-Sagemaker-Target-Model")))
	})

	return mux
}

func TestEngineEndpoint_MultiModelTargetModel(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	srv := &multiModelServer{loads: map[string]int{}, hosts: map[string]string{}}
	rig.rt.serve = srv.handler

	for _, k := range []string{"a", "b"} {
		requireNoErr(t, rig.s3.PutObjectBytes(t.Context(), "models", "mme/"+k+".tar.gz",
			tarGz(t, map[string]string{"weights.txt": "w-" + k})))
	}

	endpointFixture{
		variants: singleVariant("mme"),
		models: map[string]sagemaker.ContainerDefinition{"mme": {
			Image: "mme-img", ModelDataURL: "s3://models/mme/", Mode: "MultiModel",
		}},
	}.create(t, rig)

	ep := waitEndpoint(t, rig.b, "ep")
	require.Equal(t, "InService", ep.EndpointStatus, ep.FailureReason)

	tests := []struct {
		name      string
		target    string
		want      string
		wantErr   string
		wantTotal int
	}{
		{name: "first model", target: "a.tar.gz", want: "w-a|in|a.tar.gz", wantTotal: 1},
		{name: "second model", target: "b.tar.gz", want: "w-b|in|b.tar.gz", wantTotal: 2},
		{name: "cached model", target: "a.tar.gz", want: "w-a|in|a.tar.gz", wantTotal: 2},
		{name: "missing model", target: "none.tar.gz", wantErr: "not found", wantTotal: 2},
		{name: "no target", wantErr: "TargetModel is required", wantTotal: 2},
	}

	for _, tt := range tests {
		res, err := rig.b.InvokeEndpoint(t.Context(), sagemaker.InvokeRequest{
			EndpointName: "ep", Body: []byte("in"), TargetModel: tt.target,
		})

		if tt.wantErr != "" {
			require.ErrorContains(t, err, tt.wantErr, tt.name)
		} else {
			require.NoError(t, err, tt.name)
			assert.Equal(t, tt.want, string(res.Body), tt.name)
		}

		srv.mu.Lock()
		total := 0
		for _, n := range srv.loads {
			total += n
		}
		srv.mu.Unlock()

		assert.Equal(t, tt.wantTotal, total, tt.name)
	}
}

func TestEngineEndpoint_MultiModelEvictsOnInsufficientStorage(t *testing.T) {
	t.Parallel()

	rig := newEngineRig(t, sagemaker.EngineConfig{})
	srv := &multiModelServer{
		loads:  map[string]int{},
		hosts:  map[string]string{},
		status: http.StatusInsufficientStorage,
	}
	rig.rt.serve = srv.handler

	requireNoErr(t, rig.s3.PutObjectBytes(t.Context(), "models", "mme/a.tar.gz",
		tarGz(t, map[string]string{"weights.txt": "w-a"})))

	endpointFixture{
		variants: singleVariant("mme"),
		models: map[string]sagemaker.ContainerDefinition{"mme": {
			Image: "mme-img", ModelDataURL: "s3://models/mme", Mode: "MultiModel",
		}},
	}.create(t, rig)

	ep := waitEndpoint(t, rig.b, "ep")
	require.Equal(t, "InService", ep.EndpointStatus, ep.FailureReason)

	_, err := rig.b.InvokeEndpoint(t.Context(), sagemaker.InvokeRequest{
		EndpointName: "ep", Body: []byte("in"), TargetModel: "a.tar.gz",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "507")
}
