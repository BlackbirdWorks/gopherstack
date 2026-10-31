package eks_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	ekssdk "github.com/aws/aws-sdk-go-v2/service/eks"
	ekstypes "github.com/aws/aws-sdk-go-v2/service/eks/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/eks"
)

func TestParseVolumeMounts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    []string
		wantErr bool
	}{
		{name: "empty", in: ""},
		{name: "rw_default", in: "/data:/mnt/data", want: []string{"/data:/mnt/data"}},
		{name: "ro", in: "/data:/mnt/data:ro", want: []string{"/data:/mnt/data:ro"}},
		{name: "multiple_cleaned", in: "/a//b:/x, /c:/y:rw", want: []string{"/a/b:/x", "/c:/y:rw"}},
		{name: "relative_host", in: "data:/mnt", wantErr: true},
		{name: "relative_container", in: "/data:mnt", wantErr: true},
		{name: "dotdot_host", in: "/data/../etc:/mnt", wantErr: true},
		{name: "dotdot_container", in: "/data:/mnt/..", wantErr: true},
		{name: "bad_mode", in: "/data:/mnt:rx", wantErr: true},
		{name: "missing_container", in: "/data", wantErr: true},
		{name: "too_many_fields", in: "/a:/b:ro:x", wantErr: true},
		{name: "root_target", in: "/data:/", wantErr: true},
		{name: "duplicate_target", in: "/a:/x,/b:/x", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := eks.ParseVolumeMounts(tt.in)
			if tt.wantErr {
				require.ErrorIs(t, err, eks.ErrValidation)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseLBPorts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      string
		want    []int
		wantErr bool
	}{
		{name: "empty", in: ""},
		{name: "two", in: "8080, 8443", want: []int{8080, 8443}},
		{name: "not_a_number", in: "http", wantErr: true},
		{name: "zero", in: "0", wantErr: true},
		{name: "too_big", in: "70000", wantErr: true},
		{name: "api_port", in: "6443", wantErr: true},
		{name: "duplicate", in: "80,80", wantErr: true},
		{name: "trailing_comma", in: "80,", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := eks.ParseLBPorts(tt.in)
			if tt.wantErr {
				require.ErrorIs(t, err, eks.ErrValidation)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseK3sImageTags(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want    map[string]string
		name    string
		in      string
		wantErr bool
	}{
		{name: "empty", in: "", want: map[string]string{}},
		{name: "one", in: "1.31=v1.31.5-k3s1", want: map[string]string{"1.31": "v1.31.5-k3s1"}},
		{name: "two", in: "1.31=v1.31.5-k3s1,1.32=v1.32.1-k3s2", want: map[string]string{
			"1.31": "v1.31.5-k3s1", "1.32": "v1.32.1-k3s2",
		}},
		{name: "minor_mismatch", in: "1.31=v1.32.1-k3s1", wantErr: true},
		{name: "not_a_tag", in: "1.31=latest", wantErr: true},
		{name: "no_equals", in: "v1.31.5-k3s1", wantErr: true},
		{name: "unsupported_minor", in: "1.5=v1.5.1-k3s1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := eks.ParseK3sImageTags(tt.in)
			if tt.wantErr {
				require.ErrorIs(t, err, eks.ErrInvalidClusterEngineConfig)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDockerCluster_LaunchOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags         map[string]string
		name         string
		version      string
		wantImage    string
		wantMounts   []string
		wantLBPorts  []int
		cfg          eks.ClusterEngineConfig
		wantServLB   bool
		wantNoVolume bool
	}{
		{name: "defaults", version: "1.31", wantImage: "rancher/k3s:v1.31.14-k3s1"},
		{
			name: "tag_mount_and_ports", version: "1.31", wantImage: "rancher/k3s:v1.31.14-k3s1",
			tags: map[string]string{
				eks.TagVolumeMount: "{d}/src:/mnt/src:ro,{d}/b:/mnt/b",
				eks.TagLBPorts:     "8080,8443",
			},
			wantMounts: []string{"{d}/src:/mnt/src:ro", "{d}/b:/mnt/b"}, wantLBPorts: []int{8080, 8443},
			wantServLB: true,
		},
		{
			name: "setting_default_ports", version: "1.31", wantImage: "rancher/k3s:v1.31.14-k3s1",
			cfg:         eks.ClusterEngineConfig{LBPorts: []int{9000}},
			wantLBPorts: []int{9000}, wantServLB: true,
		},
		{
			name: "tag_overrides_default_ports", version: "1.31", wantImage: "rancher/k3s:v1.31.14-k3s1",
			cfg:         eks.ClusterEngineConfig{LBPorts: []int{9000}},
			tags:        map[string]string{eks.TagLBPorts: "7000"},
			wantLBPorts: []int{7000}, wantServLB: true,
		},
		{
			name: "pinned_tag", version: "1.31", wantImage: "rancher/k3s:v1.31.1-k3s9",
			cfg: eks.ClusterEngineConfig{ImageTags: map[string]string{"1.31": "v1.31.1-k3s9"}},
		},
		{
			name: "pin_other_minor_ignored", version: "1.31", wantImage: "rancher/k3s:v1.31.14-k3s1",
			cfg: eks.ClusterEngineConfig{ImageTags: map[string]string{"1.32": "v1.32.1-k3s9"}},
		},
		{
			name: "full_image_override", version: "1.31", wantImage: "registry.test/k3s:custom",
			cfg: eks.ClusterEngineConfig{
				Image: "registry.test/k3s:custom", ImageTags: map[string]string{"1.31": "v1.31.1-k3s9"},
			},
		},
		{name: "override_allows_unmapped", version: "1.20", wantImage: "registry.test/k3s:custom",
			cfg: eks.ClusterEngineConfig{Image: "registry.test/k3s:custom"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.Mkdir(filepath.Join(dir, "src"), 0o750))
			require.NoError(t, os.Mkdir(filepath.Join(dir, "b"), 0o750))

			tags := map[string]string{}
			for k, v := range tt.tags {
				tags[k] = strings.ReplaceAll(v, "{d}", dir)
			}

			for i, m := range tt.wantMounts {
				tt.wantMounts[i] = strings.ReplaceAll(m, "{d}", dir)
			}

			rt := &fakeClusterRuntime{}
			tt.cfg.Probe = newGatedClusterProbe().probe
			tt.cfg.Ports = mustPortAlloc(t)
			tt.cfg.VolumeMountAllow = []string{dir}
			b := dockerClusterBackend(t, rt, tt.cfg)

			_, err := b.CreateCluster("c1", tt.version, "", nil, nil, tags)
			require.NoError(t, err)
			waitSpecs(t, rt)

			spec := rt.createdSpecs()[0]
			assert.Equal(t, tt.wantImage, spec.Image)
			assert.Equal(t, tt.wantMounts, spec.Mounts)
			assert.Equal(t, tt.wantServLB, !strings.Contains(spec.Entrypoint[2], "--disable servicelb"))
			require.Len(t, spec.Ports, 1+len(tt.wantLBPorts))

			var published []eks.PublishedPort

			require.Eventually(t, func() bool {
				published, err = b.PublishedLBPorts("c1")

				return err == nil && len(published) == len(tt.wantLBPorts)
			}, 10*time.Second, 10*time.Millisecond)

			for i, want := range tt.wantLBPorts {
				assert.Equal(t, want, published[i].ContainerPort)
				assert.Equal(t, "127.0.0.1", published[i].Host)
				assert.Equal(t,
					container.PortSpec("127.0.0.1", strconv.Itoa(published[i].HostPort), strconv.Itoa(want)),
					spec.Ports[1+i])
			}
		})
	}
}

func mustPortAlloc(t *testing.T) *portalloc.Allocator {
	t.Helper()

	a, err := portalloc.New(41000, 41400)
	require.NoError(t, err)

	return a
}

func TestEnableClusters_RejectsBadOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  eks.ClusterEngineConfig
	}{
		{name: "bad_tag", cfg: eks.ClusterEngineConfig{ImageTags: map[string]string{"1.31": "latest"}}},
		{
			name: "tag_minor_mismatch",
			cfg:  eks.ClusterEngineConfig{ImageTags: map[string]string{"1.31": "v1.30.1-k3s1"}},
		},
		{name: "api_port", cfg: eks.ClusterEngineConfig{LBPorts: []int{6443}}},
		{name: "port_zero", cfg: eks.ClusterEngineConfig{LBPorts: []int{0}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			tt.cfg.Runtime = &fakeClusterRuntime{}
			tt.cfg.Token = testToken

			require.ErrorIs(t, b.EnableClusters(tt.cfg), eks.ErrInvalidClusterEngineConfig)
		})
	}
}

func TestDockerCluster_CreateRejectsInvalidOptions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags    map[string]string
		name    string
		version string
	}{
		{name: "relative_mount", version: "1.31", tags: map[string]string{eks.TagVolumeMount: "rel:/mnt"}},
		{name: "dotdot_mount", version: "1.31", tags: map[string]string{eks.TagVolumeMount: "/a/../b:/mnt"}},
		{name: "bad_port", version: "1.31", tags: map[string]string{eks.TagLBPorts: "abc"}},
		{name: "unsupported_version", version: "1.20"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeClusterRuntime{}
			b := dockerClusterBackend(t, rt, eks.ClusterEngineConfig{Probe: newGatedClusterProbe().probe})

			_, err := b.CreateCluster("c1", tt.version, "", nil, nil, tt.tags)
			require.ErrorIs(t, err, eks.ErrValidation)
			assert.Equal(t, 0, b.ClusterCount())
			assert.Empty(t, rt.createdSpecs())
		})
	}
}

func TestDockerCluster_StubEngineIgnoresOptionTags(t *testing.T) {
	t.Parallel()

	b := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
	_, err := b.CreateCluster("c1", "1.31", "", nil, nil, map[string]string{eks.TagVolumeMount: "rel:/mnt"})
	require.NoError(t, err)
}

func TestSDK_CreateClusterOptionErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		tags    map[string]string
		name    string
		version string
	}{
		{name: "bad_mount", version: "1.31", tags: map[string]string{eks.TagVolumeMount: "rel:/mnt"}},
		{name: "bad_ports", version: "1.31", tags: map[string]string{eks.TagLBPorts: "99999"}},
		{name: "unsupported_version", version: "1.20"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := dockerClusterBackend(t, &fakeClusterRuntime{}, eks.ClusterEngineConfig{
				Probe: newGatedClusterProbe().probe,
			})
			client := newTestEKSClient(t, eks.NewHandler(b))

			_, err := client.CreateCluster(t.Context(), &ekssdk.CreateClusterInput{
				Name: aws.String("c1"), Version: aws.String(tt.version), Tags: tt.tags,
				RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
				ResourcesVpcConfig: vpcReq(),
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), "InvalidParameterException")
		})
	}
}

func TestServePublishedPorts(t *testing.T) {
	t.Parallel()

	rt := &fakeClusterRuntime{}
	b := dockerClusterBackend(t, rt, eks.ClusterEngineConfig{
		Probe: newGatedClusterProbe().probe, Ports: mustPortAlloc(t),
	})
	_, err := b.CreateCluster("c1", "1.31", "", nil, nil, map[string]string{eks.TagLBPorts: "8080"})
	require.NoError(t, err)

	h := eks.NewHandler(b)
	e := echo.New()
	e.GET("/_gopherstack/eks/clusters/:name/ports", h.ServePublishedPorts)

	tests := []struct {
		name      string
		path      string
		wantCode  int
		wantPorts int
	}{
		{name: "published", path: "c1", wantCode: http.StatusOK, wantPorts: 1},
		{name: "missing", path: "nope", wantCode: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			require.Eventually(t, func() bool {
				p, _ := b.PublishedLBPorts("c1")

				return len(p) == 1
			}, 10*time.Second, 10*time.Millisecond)

			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, httptest.NewRequestWithContext(
				t.Context(), http.MethodGet, "/_gopherstack/eks/clusters/"+tt.path+"/ports", nil))
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var body struct {
				ClusterName string `json:"clusterName"`
				Ports       []struct {
					ContainerPort int `json:"containerPort"`
					HostPort      int `json:"hostPort"`
				} `json:"ports"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "c1", body.ClusterName)
			require.Len(t, body.Ports, tt.wantPorts)
			assert.Equal(t, 8080, body.Ports[0].ContainerPort)
			assert.Positive(t, body.Ports[0].HostPort)
		})
	}
}

func vpcReq() *ekstypes.VpcConfigRequest {
	return &ekstypes.VpcConfigRequest{SubnetIds: []string{"subnet-abc123"}}
}

func pemEncodeCert(der []byte) []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func writeKubeconfig(t *testing.T, dir, body string) string {
	t.Helper()

	p := filepath.Join(dir, "kubeconfig")
	require.NoError(t, os.WriteFile(p, []byte(body), 0o600))

	return p
}

func kubeconfigFixture(server, caData, caFile string) string {
	ca := ""
	if caData != "" {
		ca = "      certificate-authority-data: " + caData + "\n"
	}

	if caFile != "" {
		ca = "      certificate-authority: " + caFile + "\n"
	}

	return `apiVersion: v1
kind: Config
current-context: primary
clusters:
  - name: primary-cluster
    cluster:
      server: ` + server + `
` + ca + `  - name: other-cluster
    cluster:
      server: https://other.example:6443
contexts:
  - name: primary
    context: {cluster: primary-cluster, user: u}
  - name: other
    context: {cluster: other-cluster, user: u}
users:
  - name: u
    user: {token: t}
`
}

func TestExistingCluster_Load(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		kubeContext  string
		body         string
		wantEndpoint string
		wantCA       string
		wantErr      bool
	}{
		{name: "current_context", body: kubeconfigFixture("https://primary.example:6443", "Q0E=", ""),
			wantEndpoint: "https://primary.example:6443", wantCA: "Q0E="},
		{name: "explicit_context", kubeContext: "other", body: kubeconfigFixture("https://p:1", "Q0E=", ""),
			wantEndpoint: "https://other.example:6443"},
		{
			name:        "unknown_context",
			kubeContext: "nope",
			body:        kubeconfigFixture("https://p:1", "Q0E=", ""),
			wantErr:     true,
		},
		{name: "not_yaml", body: ":\n- [", wantErr: true},
		{name: "missing_ca_file", body: kubeconfigFixture("https://p:1", "", "missing.crt"), wantErr: true},
		{name: "relative_ca_file", body: kubeconfigFixture("https://p:1", "", "ca.crt"),
			wantEndpoint: "https://p:1", wantCA: base64.StdEncoding.EncodeToString([]byte("PEM"))},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			require.NoError(t, os.WriteFile(filepath.Join(dir, "ca.crt"), []byte("PEM"), 0o600))

			b := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			err := b.EnableExistingCluster(writeKubeconfig(t, dir, tt.body), tt.kubeContext)

			if tt.wantErr {
				require.ErrorIs(t, err, eks.ErrInvalidKubeconfig)

				return
			}

			require.NoError(t, err)
			_, err = b.CreateCluster("c1", "1.31", "", nil, nil, nil)
			require.NoError(t, err)
			waitStatus(t, b, "ACTIVE")

			d, err := b.DescribeCluster("c1")
			require.NoError(t, err)
			assert.Equal(t, tt.wantEndpoint, d.Endpoint)
			assert.Equal(t, tt.wantCA, d.CertificateAuthority)
		})
	}

	t.Run("missing_file", func(t *testing.T) {
		t.Parallel()

		b := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
		require.ErrorIs(t, b.EnableExistingCluster(filepath.Join(t.TempDir(), "none"), ""), eks.ErrInvalidKubeconfig)
	})
}

func TestExistingCluster_SDKDescribeReachesExternalCluster(t *testing.T) {
	t.Parallel()

	ext := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"gitVersion":"v1.31.0"}`))
	}))
	t.Cleanup(ext.Close)

	caPEM := string(pemEncodeCert(ext.Certificate().Raw))
	kc := kubeconfigFixture(ext.URL, base64.StdEncoding.EncodeToString([]byte(caPEM)), "")

	b := eks.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	require.NoError(t, b.EnableExistingCluster(writeKubeconfig(t, t.TempDir(), kc), ""))
	client := newTestEKSClient(t, eks.NewHandler(b))

	_, err := client.CreateCluster(t.Context(), &ekssdk.CreateClusterInput{
		Name:               aws.String("ext"),
		RoleArn:            aws.String("arn:aws:iam::123456789012:role/eks-role"),
		ResourcesVpcConfig: vpcReq(),
	})
	require.NoError(t, err)

	var endpoint, ca string

	require.Eventually(t, func() bool {
		out, derr := client.DescribeCluster(t.Context(), &ekssdk.DescribeClusterInput{Name: aws.String("ext")})
		require.NoError(t, derr)

		if string(out.Cluster.Status) != "ACTIVE" {
			return false
		}

		endpoint, ca = aws.ToString(out.Cluster.Endpoint), aws.ToString(out.Cluster.CertificateAuthority.Data)

		return true
	}, 30*time.Second, 10*time.Millisecond)

	assert.Equal(t, ext.URL, endpoint)

	get := func() string {
		pemBytes, derr := base64.StdEncoding.DecodeString(ca)
		require.NoError(t, derr)

		pool := x509.NewCertPool()
		require.True(t, pool.AppendCertsFromPEM(pemBytes))

		tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}
		defer tr.CloseIdleConnections()

		req, rerr := http.NewRequestWithContext(t.Context(), http.MethodGet, endpoint+"/version", nil)
		require.NoError(t, rerr)

		resp, rerr := (&http.Client{Transport: tr}).Do(req)
		require.NoError(t, rerr)

		defer func() { _ = resp.Body.Close() }()

		body, rerr := io.ReadAll(resp.Body)
		require.NoError(t, rerr)

		return string(body)
	}

	assert.Contains(t, get(), "v1.31.0")

	_, err = client.DeleteCluster(t.Context(), &ekssdk.DeleteClusterInput{Name: aws.String("ext")})
	require.NoError(t, err)
	assert.Contains(t, get(), "v1.31.0", "DeleteCluster must not touch the external cluster")
}

func TestDockerCluster_VolumeMountAllowlist(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mount   func(t *testing.T, allowed, outside string) string
		name    string
		allow   bool
		wantErr bool
	}{
		{name: "default_off", allow: false, wantErr: true,
			mount: func(_ *testing.T, allowed, _ string) string { return allowed + ":/mnt/x" }},
		{name: "allowed", allow: true,
			mount: func(_ *testing.T, allowed, _ string) string { return allowed + ":/mnt/x" }},
		{name: "outside_prefix", allow: true, wantErr: true,
			mount: func(_ *testing.T, _, outside string) string { return outside + ":/mnt/x" }},
		{name: "missing_path", allow: true, wantErr: true,
			mount: func(_ *testing.T, allowed, _ string) string { return allowed + "/nope:/mnt/x" }},
		{name: "symlink_escape", allow: true, wantErr: true,
			mount: func(t *testing.T, allowed, outside string) string {
				t.Helper()

				link := filepath.Join(allowed, "escape")
				require.NoError(t, os.Symlink(outside, link))

				return link + ":/mnt/x"
			}},
		{name: "prefix_sibling", allow: true, wantErr: true,
			mount: func(t *testing.T, allowed, _ string) string {
				t.Helper()

				sib := allowed + "-sibling"
				require.NoError(t, os.Mkdir(sib, 0o750))
				t.Cleanup(func() { _ = os.Remove(sib) })

				return sib + ":/mnt/x"
			}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			allowed, outside := t.TempDir(), t.TempDir()
			cfg := eks.ClusterEngineConfig{Probe: newGatedClusterProbe().probe}

			if tt.allow {
				cfg.VolumeMountAllow = []string{allowed}
			}

			rt := &fakeClusterRuntime{}
			b := dockerClusterBackend(t, rt, cfg)

			_, err := b.CreateCluster("c1", "1.31", "", nil, nil, map[string]string{
				eks.TagVolumeMount: tt.mount(t, allowed, outside),
			})
			if tt.wantErr {
				require.ErrorIs(t, err, eks.ErrValidation)
				assert.Equal(t, 0, b.ClusterCount())

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestEnableClusters_RejectsBadMountAllow(t *testing.T) {
	t.Parallel()

	for _, allow := range [][]string{{"/"}, {"/a/.."}, {"rel"}, {"//"}} {
		b := eks.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
		err := b.EnableClusters(eks.ClusterEngineConfig{
			Runtime: &fakeClusterRuntime{}, Token: testToken, VolumeMountAllow: allow,
		})
		require.ErrorIs(t, err, eks.ErrInvalidClusterEngineConfig, allow)
	}
}
