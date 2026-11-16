package sagemaker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	modeMultiModel       = "MultiModel"
	modelsDirInContainer = "/opt/ml/models"
	multiModelNameLen    = 16
	maxTargetModelLen    = 1024
)

// ErrTargetModelRequired means a multi-model endpoint was invoked without TargetModel.
var ErrTargetModelRequired = errors.New("TargetModel is required to invoke a multi-model endpoint")

// multiModel tracks the models loaded into one multi-model container (mms-container-apis).
type multiModel struct {
	mu     *lockmetrics.RWMutex
	loaded map[string]bool
	prefix string
	dir    string
}

func newMultiModel(prefix, dir string) *multiModel {
	return &multiModel{mu: lockmetrics.New("sagemaker.multimodel"), loaded: map[string]bool{}, prefix: prefix, dir: dir}
}

func multiModelName(target string) string {
	sum := sha256.Sum256([]byte(target))

	return hex.EncodeToString(sum[:])[:multiModelNameLen*2]
}

// invocationPath returns the container path serving inv, loading the target model first on a
// multi-model container.
func (d *modelDeployment) invocationPath(ctx context.Context, rc *runningContainer, inv invocation) (string, error) {
	if rc.mm == nil {
		return "/invocations", nil
	}

	if inv.targetModel == "" {
		return "", ErrTargetModelRequired
	}

	if len(inv.targetModel) > maxTargetModelLen {
		return "", fmt.Errorf("%w: TargetModel exceeds %d characters", ErrTargetModelRequired, maxTargetModelLen)
	}

	name := multiModelName(inv.targetModel)
	if err := d.ensureModelLoaded(ctx, rc, name, inv); err != nil {
		return "", err
	}

	return "/models/" + name + "/invoke", nil
}

func (d *modelDeployment) ensureModelLoaded(
	ctx context.Context,
	rc *runningContainer,
	name string,
	inv invocation,
) error {
	mm := rc.mm

	mm.mu.Lock("ensureModelLoaded")
	defer mm.mu.Unlock()

	if mm.loaded[name] {
		return nil
	}

	modelDir := filepath.Join(mm.dir, name, "model")
	if err := mkSharedDir(modelDir); err != nil {
		return fmt.Errorf("stage model: %w", err)
	}

	if err := d.e.stageModelData(ctx, joinS3(mm.prefix, inv.targetModel), modelDir); err != nil {
		_ = os.RemoveAll(filepath.Join(mm.dir, name))

		return &ModelError{OriginalStatusCode: http.StatusNotFound, OriginalMessage: fmt.Sprintf(
			"Model %s not found under %s: %v", inv.targetModel, mm.prefix, err)}
	}

	if err := d.loadStaged(ctx, rc, name, inv.targetModel); err != nil {
		_ = os.RemoveAll(filepath.Join(mm.dir, name))

		return err
	}

	mm.loaded[name] = true

	return nil
}

func (d *modelDeployment) loadStaged(ctx context.Context, rc *runningContainer, name, target string) error {
	status, err := d.loadModel(ctx, rc, name, target)
	if err != nil {
		return err
	}

	if status == http.StatusInsufficientStorage {
		d.unloadAll(ctx, rc)

		if status, err = d.loadModel(ctx, rc, name, target); err != nil {
			return err
		}
	}

	if status/100 != 2 && status != http.StatusConflict {
		return &ModelError{OriginalStatusCode: status, OriginalMessage: "container failed to load model " + target}
	}

	return nil
}

func (d *modelDeployment) loadModel(ctx context.Context, rc *runningContainer, name, target string) (int, error) {
	body, err := json.Marshal(map[string]string{
		"model_name": name, "url": path.Join(modelsDirInContainer, name, "model"),
	}, jsonx.V1())
	if err != nil {
		return 0, fmt.Errorf("encode load model: %w", err)
	}

	return d.modelsCall(ctx, http.MethodPost, rc.baseURL+"/models", body, target)
}

// unloadAll evicts every loaded model, the platform's reply to a 507 from LoadModel.
func (d *modelDeployment) unloadAll(ctx context.Context, rc *runningContainer) {
	for name := range rc.mm.loaded {
		_, _ = d.modelsCall(ctx, http.MethodDelete, rc.baseURL+"/models/"+name, nil, "")

		_ = os.RemoveAll(filepath.Join(rc.mm.dir, name))

		delete(rc.mm.loaded, name)
	}
}

func (d *modelDeployment) modelsCall(ctx context.Context, method, url string, body []byte, target string) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, d.e.cfg.InvokeTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build models request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	setIf(req.Header, headerTargetModel, target)

	resp, err := d.e.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("models call: %w", err)
	}

	defer resp.Body.Close()

	return resp.StatusCode, nil
}
