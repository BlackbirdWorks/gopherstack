package sagemaker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	xmaps "maps"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	headerCustomAttributes = "X-Amzn-Sagemaker-Custom-Attributes"
	headerInferenceID      = "X-Amzn-Sagemaker-Inference-Id"
	headerTargetModel      = "X-Amzn-Sagemaker-Target-Model"
	defaultContentType     = "application/octet-stream"
	modelDirInContainer    = "/opt/ml/model"
	modeSerial             = "Serial"
	modeDirect             = "Direct"
	errBodySnippet         = 1024
	pingTimeout            = 2 * time.Second
	containerNameSuffix    = 8
	shortIDLen             = 12
)

var (
	errModelPackageContainer = errors.New("model package containers are not supported by the container engine")
	errNoContainers          = errors.New("model defines no containers")
	errContainerExited       = errors.New("container exited")
)

// startError carries the FailureReason an endpoint or job reports.
type startError struct {
	cause  error
	reason string
}

func (e *startError) Error() string { return e.reason }
func (e *startError) Unwrap() error { return e.cause }

func failReason(err error) string {
	if se, ok := errors.AsType[*startError](err); ok {
		return se.reason
	}

	return err.Error()
}

type runningContainer struct {
	exited   chan int
	stopLogs context.CancelFunc
	stopWait context.CancelFunc
	hostname string
	baseURL  string
	id       string
	port     int
}

// modelDeployment is the running container set of one model (one production variant).
type modelDeployment struct {
	e          *engineState
	variant    string
	dir        string
	containers []*runningContainer
	stopped    atomic.Bool
	serial     bool
}

func (d *modelDeployment) stop() {
	if d == nil || !d.stopped.CompareAndSwap(false, true) {
		return
	}

	for _, rc := range d.containers {
		rc.stopLogs()
		rc.stopWait()

		ctx, cancel := cleanupContext()
		_ = d.e.cfg.Runtime.StopAndRemove(ctx, rc.id)

		cancel()
		portalloc.ReleaseEphemeral(rc.port)
	}

	if d.dir != "" {
		_ = os.RemoveAll(d.dir)
	}
}

type modelStart struct {
	env            map[string]string
	region         string
	label          string
	variant        string
	logGroup       string
	logStreamBase  string
	model          Model
	startupTimeout time.Duration
}

func modelContainers(m Model) []ContainerDefinition {
	if len(m.Containers) > 0 {
		return m.Containers
	}

	if m.PrimaryContainer != nil {
		return []ContainerDefinition{*m.PrimaryContainer}
	}

	return nil
}

// startModel launches every container of the model and waits until each answers GET /ping.
func (e *engineState) startModel(ctx context.Context, in modelStart) (*modelDeployment, error) {
	if err := e.unavailable(); err != nil {
		return nil, &startError{reason: err.Error(), cause: err}
	}

	defs := modelContainers(in.model)
	if len(defs) == 0 {
		return nil, &startError{reason: errNoContainers.Error(), cause: errNoContainers}
	}

	dir, err := e.newStageDir("model")
	if err != nil {
		return nil, &startError{reason: err.Error(), cause: err}
	}

	d := &modelDeployment{e: e, variant: in.variant, dir: dir, serial: true}
	if in.model.InferenceExecutionConfig != nil && in.model.InferenceExecutionConfig.Mode == modeDirect {
		d.serial = false
	}

	for i, def := range defs {
		rc, serr := e.startContainer(ctx, in, d, i, def)
		if rc != nil {
			d.containers = append(d.containers, rc)
		}

		if serr != nil {
			d.stop()

			return nil, serr
		}
	}

	return d, nil
}

func (e *engineState) startContainer(
	ctx context.Context, in modelStart, d *modelDeployment, idx int, def ContainerDefinition,
) (*runningContainer, error) {
	if def.Image == "" {
		return nil, &startError{reason: errModelPackageContainer.Error(), cause: errModelPackageContainer}
	}

	modelDir := filepath.Join(d.dir, "c"+strconv.Itoa(idx), "model")
	if err := mkSharedDir(modelDir); err != nil {
		return nil, &startError{reason: err.Error(), cause: err}
	}

	if def.ModelDataURL != "" {
		if err := e.stageModelData(ctx, def.ModelDataURL, modelDir); err != nil {
			reason := fmt.Sprintf("Unable to load model data %s: %v", def.ModelDataURL, err)

			return nil, &startError{reason: reason, cause: err}
		}
	}

	env := envList(def.Environment, in.env)
	name := fmt.Sprintf("gopherstack-sagemaker-%s-%s", sanitizeName(in.label), awsid.NewN("", containerNameSuffix))

	var port int

	id, err := container.CreateAndStartRetry(ctx, e.cfg.Runtime, func() (container.Spec, error) {
		p, perr := portalloc.AcquireEphemeral()
		if perr != nil {
			return container.Spec{}, perr
		}

		port = p

		return container.Spec{
			Image: def.Image, Name: name, Env: env, Cmd: []string{"serve"},
			Mounts: []string{modelDir + ":" + modelDirInContainer},
			Ports:  []string{container.PortSpec(loopbackHost, strconv.Itoa(p), modelContainerPort)},
		}, nil
	}, func() { portalloc.ReleaseEphemeral(port) })
	if err != nil {
		reason := fmt.Sprintf("Unable to start the container for production variant %s (image %s): %v",
			in.variant, def.Image, err)

		return nil, &startError{reason: reason, cause: err}
	}

	rc := &runningContainer{
		hostname: def.ContainerHostname, id: id, port: port, exited: make(chan int, 1),
		baseURL: "http://" + loopbackHost + ":" + strconv.Itoa(port),
	}

	bg := context.WithoutCancel(ctx)
	lt := e.openLog(bg, in.region, in.logGroup, fmt.Sprintf("%s/%s", in.logStreamBase, id[:min(len(id), shortIDLen)]))
	_, rc.stopLogs = followLogs(bg, e.cfg.Runtime, id, lt)
	rc.stopWait = e.watchExit(bg, rc)

	if herr := e.waitHealthy(ctx, rc, in); herr != nil {
		return rc, herr
	}

	return rc, nil
}

func (e *engineState) watchExit(ctx context.Context, rc *runningContainer) context.CancelFunc {
	wctx, cancel := context.WithCancel(ctx)

	if w, ok := e.cfg.Runtime.(container.ExitWaiter); ok {
		go func() {
			if code, err := w.WaitExit(wctx, rc.id); err == nil {
				rc.exited <- code
			}
		}()
	}

	return cancel
}

func (e *engineState) waitHealthy(ctx context.Context, rc *runningContainer, in modelStart) error {
	timeout := in.startupTimeout
	if timeout <= 0 {
		timeout = e.cfg.StartupTimeout
	}

	hctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(e.cfg.PingInterval)
	defer ticker.Stop()

	for {
		if e.ping(hctx, rc) {
			return nil
		}

		select {
		case <-hctx.Done():
			if ctx.Err() != nil {
				return ctx.Err()
			}

			reason := fmt.Sprintf(
				"The primary container for production variant %s did not pass the ping health check. "+
					"Please check CloudWatch logs for this endpoint.",
				in.variant,
			)

			return &startError{reason: reason, cause: hctx.Err()}
		case code := <-rc.exited:
			reason := fmt.Sprintf("The container for production variant %s exited with code %d before passing the "+
				"ping health check. Please check CloudWatch logs for this endpoint.", in.variant, code)

			return &startError{reason: reason, cause: errContainerExited}
		case <-ticker.C:
		}
	}
}

func (e *engineState) ping(ctx context.Context, rc *runningContainer) bool {
	pctx, cancel := context.WithTimeout(ctx, pingTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(pctx, http.MethodGet, rc.baseURL+"/ping", nil)
	if err != nil {
		return false
	}

	resp, err := e.http.Do(req)
	if err != nil {
		return false
	}

	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

	return resp.StatusCode == http.StatusOK
}

// stageModelData downloads ModelDataUrl into dir, unpacking a model.tar.gz archive.
func (e *engineState) stageModelData(ctx context.Context, uri, dir string) error {
	store := e.svc().Store

	data, err := getS3URI(ctx, store, uri)
	if err != nil {
		return err
	}

	if isGzip(data) {
		return extractTarGz(data, dir)
	}

	_, key, _ := parseS3URI(uri)

	return os.WriteFile(filepath.Join(dir, path.Base(key)), data, fileMode)
}

func envList(layers ...map[string]string) []string {
	merged := make(map[string]string)

	for _, m := range layers {
		xmaps.Copy(merged, m)
	}

	out := make([]string, 0, len(merged))
	for k, v := range merged {
		out = append(out, k+"="+v)
	}

	slices.Sort(out)

	return out
}

func sanitizeName(s string) string {
	var sb strings.Builder

	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			sb.WriteRune(r)
		default:
			sb.WriteByte('-')
		}
	}

	return sb.String()
}

// invocation is one request to a model's /invocations.
type invocation struct {
	contentType string
	accept      string
	custom      string
	targetModel string
	inferenceID string
	body        []byte
	timeout     time.Duration
}

type cancelBody struct {
	io.ReadCloser

	cancel context.CancelFunc
}

func (c cancelBody) Close() error {
	err := c.ReadCloser.Close()
	c.cancel()

	return err
}

func (d *modelDeployment) post(ctx context.Context, rc *runningContainer, inv invocation) (*http.Response, error) {
	timeout := d.e.cfg.InvokeTimeout
	if inv.timeout > 0 {
		timeout = inv.timeout
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rc.baseURL+"/invocations", bytes.NewReader(inv.body))
	if err != nil {
		cancel()

		return nil, err //nolint:wrapcheck // request construction cannot fail on valid input
	}

	ct := inv.contentType
	if ct == "" {
		ct = defaultContentType
	}

	req.Header.Set("Content-Type", ct)
	setIf(req.Header, "Accept", inv.accept)
	setIf(req.Header, headerCustomAttributes, inv.custom)
	setIf(req.Header, headerTargetModel, inv.targetModel)
	setIf(req.Header, headerInferenceID, inv.inferenceID)

	resp, err := d.e.http.Do(req)
	if err != nil {
		cancel()

		return nil, fmt.Errorf("invoke container: %w", err)
	}

	resp.Body = cancelBody{ReadCloser: resp.Body, cancel: cancel}

	return resp, nil
}

func setIf(h http.Header, k, v string) {
	if v != "" {
		h.Set(k, v)
	}
}

// invoke routes inv through the model's containers: a serial pipeline feeds each response into the
// next container; direct mode targets one container by hostname. The caller closes the response.
func (d *modelDeployment) invoke(ctx context.Context, inv invocation, target string) (*http.Response, error) {
	if len(d.containers) == 0 || d.stopped.Load() {
		return nil, ErrEndpointNotRunning
	}

	if !d.serial || len(d.containers) == 1 {
		return d.post(ctx, d.pick(target), inv)
	}

	for i, rc := range d.containers {
		last := i == len(d.containers)-1
		step := inv

		if !last {
			step.accept = ""
		}

		resp, err := d.post(ctx, rc, step)
		if err != nil || last {
			return resp, err
		}

		body, rerr := io.ReadAll(io.LimitReader(resp.Body, maxStagedBytes))
		_ = resp.Body.Close()

		if rerr != nil {
			return nil, fmt.Errorf("read container response: %w", rerr)
		}

		if resp.StatusCode/100 != 2 { //nolint:mnd // HTTP status class
			resp.Body = io.NopCloser(bytes.NewReader(body))

			return resp, nil
		}

		inv.body = body
		inv.contentType = resp.Header.Get("Content-Type")
	}

	return nil, ErrEndpointNotRunning
}

func (d *modelDeployment) pick(hostname string) *runningContainer {
	if hostname != "" {
		for _, rc := range d.containers {
			if strings.EqualFold(rc.hostname, hostname) {
				return rc
			}
		}
	}

	return d.containers[0]
}

func readSnippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, errBodySnippet))

	return strings.TrimSpace(string(b))
}
