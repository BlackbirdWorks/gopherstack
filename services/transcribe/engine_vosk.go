package transcribe

import (
	"archive/zip"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awsid"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// DefaultVoskImage is the pinned Vosk server image; Python 3 with the vosk module is all the engine uses.
const DefaultVoskImage = "alphacep/kaldi-vosk-server@" +
	"sha256:381024347b954835c9c20adea2041377e7b55f50d4c5887eb42101bbf6e750a1"

const (
	voskScriptName   = "recognize.py"
	voskDownloadWait = 15 * time.Minute
	voskMaxFileBytes = 1 << 30
	voskDirMode      = 0o755
	voskFileMode     = 0o644
)

const voskScript = `import sys, json, wave
from vosk import Model, KaldiRecognizer, SetLogLevel
SetLogLevel(-1)
wf = wave.open(sys.argv[1], "rb")
rec = KaldiRecognizer(Model("/model"), wf.getframerate())
rec.SetWords(True)
words = []
while True:
    data = wf.readframes(4000)
    if not data:
        break
    if rec.AcceptWaveform(data):
        words.extend(json.loads(rec.Result()).get("result", []))
words.extend(json.loads(rec.FinalResult()).get("result", []))
print(json.dumps(words))
`

var (
	errVoskChecksum = errors.New("model archive checksum mismatch")
	errVoskExec     = errors.New("recognizer failed")
	errVoskArchive  = errors.New("unsafe model archive entry")
)

// EngineRuntime is the slice of container.Runtime the Vosk engine uses.
type EngineRuntime interface {
	HasImage(ctx context.Context, ref string) (bool, error)
	PullImage(ctx context.Context, ref string) error
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, id string) error
	Exec(ctx context.Context, id string, cmd []string) (container.ExecResult, error)
}

// VoskConfig configures a VoskEngine.
type VoskConfig struct {
	Runtime      EngineRuntime
	HTTP         *http.Client
	Models       map[string]VoskModel
	Image        string
	ModelDir     string
	ModelBaseURL string
}

// VoskEngine runs the offline Vosk recognizer inside a container, one warm container per language model.
type VoskEngine struct {
	mu         *lockmetrics.RWMutex
	cfg        VoskConfig
	containers map[string]string
	workDir    string
}

// NewVoskEngine builds an engine; containers and models are created on first use.
func NewVoskEngine(cfg VoskConfig) *VoskEngine {
	if cfg.Image == "" {
		cfg.Image = DefaultVoskImage
	}

	if cfg.HTTP == nil {
		cfg.HTTP = httpclient.New(voskDownloadWait)
	}

	if cfg.ModelBaseURL == "" {
		cfg.ModelBaseURL = voskModelBaseURL
	}

	return &VoskEngine{mu: lockmetrics.New("transcribe-vosk"), cfg: cfg, containers: map[string]string{}}
}

// SupportsLanguage implements Engine.
func (e *VoskEngine) SupportsLanguage(code string) bool {
	_, ok := e.models()[code]

	return ok
}

// Recognize implements Engine.
func (e *VoskEngine) Recognize(ctx context.Context, language string, sampleRate int, pcm []byte) ([]Word, error) {
	model := e.models()[language]

	cid, work, err := e.ensureContainer(ctx, model)
	if err != nil {
		return nil, err
	}

	name := awsid.New("job-") + ".wav"
	host := filepath.Join(work, name)

	if err = os.WriteFile(host, monoWAV(pcm, sampleRate), voskFileMode); err != nil {
		return nil, fmt.Errorf("write audio: %w", err)
	}

	defer os.Remove(host)

	res, err := e.cfg.Runtime.Exec(ctx, cid, []string{"python3", "/work/" + voskScriptName, "/work/" + name})
	if err != nil {
		return nil, fmt.Errorf("exec recognizer: %w", err)
	}

	if res.ExitCode != 0 {
		return nil, fmt.Errorf("%w: exit %d: %s", errVoskExec, res.ExitCode, strings.TrimSpace(res.Stderr))
	}

	return parseVoskWords(res.Stdout)
}

func (e *VoskEngine) models() map[string]VoskModel {
	if e.cfg.Models != nil {
		return e.cfg.Models
	}

	return voskLanguageModels()
}

func parseVoskWords(stdout string) ([]Word, error) {
	lines := strings.Split(strings.TrimSpace(stdout), "\n")

	var raw []struct {
		Word  string  `json:"word"`
		Start float64 `json:"start"`
		End   float64 `json:"end"`
		Conf  float64 `json:"conf"`
	}

	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &raw, jsonx.V1()); err != nil {
		return nil, fmt.Errorf("%w: unparsable output: %w", errVoskExec, err)
	}

	words := make([]Word, 0, len(raw))
	for _, r := range raw {
		words = append(words, Word{Text: r.Word, Start: r.Start, End: r.End, Confidence: r.Conf})
	}

	return words, nil
}

func (e *VoskEngine) ensureContainer(ctx context.Context, model VoskModel) (string, string, error) {
	e.mu.Lock("ensureContainer")
	defer e.mu.Unlock()

	if id, ok := e.containers[model.Name]; ok {
		return id, e.workDir, nil
	}

	dir, err := e.ensureModel(ctx, model)
	if err != nil {
		return "", "", err
	}

	if e.workDir == "" {
		if e.workDir, err = os.MkdirTemp("", "gopherstack-transcribe-"); err != nil {
			return "", "", fmt.Errorf("work dir: %w", err)
		}

		if err = os.WriteFile(filepath.Join(e.workDir, voskScriptName), []byte(voskScript), voskFileMode); err != nil {
			return "", "", fmt.Errorf("write script: %w", err)
		}
	}

	if have, _ := e.cfg.Runtime.HasImage(ctx, e.cfg.Image); !have {
		if err = e.cfg.Runtime.PullImage(ctx, e.cfg.Image); err != nil {
			return "", "", fmt.Errorf("pull %s: %w", e.cfg.Image, err)
		}
	}

	id, err := e.cfg.Runtime.CreateAndStart(ctx, container.Spec{
		Image:      e.cfg.Image,
		Entrypoint: []string{"sleep", "infinity"},
		Mounts:     []string{dir + ":/model:ro", e.workDir + ":/work"},
	})
	if err != nil {
		return "", "", fmt.Errorf("start recognizer container: %w", err)
	}

	e.containers[model.Name] = id

	return id, e.workDir, nil
}

// Close removes the engine's containers and scratch directory.
func (e *VoskEngine) Close(ctx context.Context) {
	e.mu.Lock("Close")
	defer e.mu.Unlock()

	for name, id := range e.containers {
		_ = e.cfg.Runtime.StopAndRemove(ctx, id)

		delete(e.containers, name)
	}

	if e.workDir != "" {
		_ = os.RemoveAll(e.workDir)
		e.workDir = ""
	}
}

func (e *VoskEngine) modelRoot() (string, error) {
	if e.cfg.ModelDir != "" {
		return e.cfg.ModelDir, nil
	}

	base, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("user cache dir: %w", err)
	}

	return filepath.Join(base, "gopherstack", "transcribe-models"), nil
}

// ensureModel returns the extracted model directory, downloading and verifying the archive on first use.
func (e *VoskEngine) ensureModel(ctx context.Context, model VoskModel) (string, error) {
	root, err := e.modelRoot()
	if err != nil {
		return "", err
	}

	dir := filepath.Join(root, model.Name)
	if st, statErr := os.Stat(dir); statErr == nil && st.IsDir() {
		return dir, nil
	}

	if err = os.MkdirAll(root, voskDirMode); err != nil {
		return "", fmt.Errorf("model cache: %w", err)
	}

	archive, err := e.download(ctx, root, model)
	if err != nil {
		return "", err
	}

	defer os.Remove(archive)

	staging, err := os.MkdirTemp(root, ".extract-")
	if err != nil {
		return "", fmt.Errorf("staging dir: %w", err)
	}

	defer os.RemoveAll(staging)

	if err = unzipSafe(archive, staging); err != nil {
		return "", err
	}

	if err = os.Rename(filepath.Join(staging, model.Name), dir); err != nil {
		return "", fmt.Errorf("install model: %w", err)
	}

	return dir, nil
}

func (e *VoskEngine) download(ctx context.Context, root string, model VoskModel) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.cfg.ModelBaseURL+model.Name+".zip", nil)
	if err != nil {
		return "", fmt.Errorf("model request: %w", err)
	}

	// One-shot download: drop the keep-alive connection so it doesn't outlive the engine.
	defer e.cfg.HTTP.CloseIdleConnections()

	resp, err := e.cfg.HTTP.Do(req)
	if err != nil {
		return "", fmt.Errorf("download %s: %w", model.Name, err)
	}

	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("download %s: %w: %s", model.Name, errVoskExec, resp.Status)
	}

	f, err := os.CreateTemp(root, ".model-*.zip")
	if err != nil {
		return "", fmt.Errorf("model temp file: %w", err)
	}

	defer f.Close()

	h := sha256.New()
	if _, err = io.Copy(io.MultiWriter(f, h), resp.Body); err != nil {
		_ = os.Remove(f.Name())

		return "", fmt.Errorf("download %s: %w", model.Name, err)
	}

	if got := hex.EncodeToString(h.Sum(nil)); got != model.SHA256 {
		_ = os.Remove(f.Name())

		return "", fmt.Errorf("%w: %s sha256 %s", errVoskChecksum, model.Name, got)
	}

	return f.Name(), nil
}

func unzipSafe(archive, dest string) error {
	zr, err := zip.OpenReader(archive)
	if err != nil {
		return fmt.Errorf("open model archive: %w", err)
	}

	defer zr.Close()

	for _, f := range zr.File {
		if !filepath.IsLocal(f.Name) {
			return fmt.Errorf("%w: %s", errVoskArchive, f.Name)
		}

		rel := filepath.Clean(f.Name)
		target := filepath.Join(dest, rel)

		if target != filepath.Clean(dest) && !strings.HasPrefix(target, filepath.Clean(dest)+string(os.PathSeparator)) {
			return fmt.Errorf("%w: %s", errVoskArchive, f.Name)
		}

		if f.FileInfo().IsDir() {
			if err = os.MkdirAll(target, voskDirMode); err != nil {
				return fmt.Errorf("extract: %w", err)
			}

			continue
		}

		if err = extractZipFile(f, target); err != nil {
			return err
		}
	}

	return nil
}

func extractZipFile(f *zip.File, target string) error {
	if err := os.MkdirAll(filepath.Dir(target), voskDirMode); err != nil {
		return fmt.Errorf("extract: %w", err)
	}

	src, err := f.Open()
	if err != nil {
		return fmt.Errorf("extract: %w", err)
	}

	defer src.Close()

	out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, voskFileMode)
	if err != nil {
		return fmt.Errorf("extract: %w", err)
	}

	defer out.Close()

	if _, err = io.Copy(out, io.LimitReader(src, voskMaxFileBytes)); err != nil {
		return fmt.Errorf("extract: %w", err)
	}

	return nil
}

// monoWAV wraps mono 16-bit PCM in a canonical WAV header.
func monoWAV(pcm []byte, rate int) []byte {
	const (
		headerLen = 44
		fmtLen    = 16
		sampleLen = 2
	)

	le := binary.LittleEndian
	buf := make([]byte, headerLen, headerLen+len(pcm))
	copy(buf[0:], "RIFF")
	le.PutUint32(buf[4:], uint32(headerLen-8+len(pcm))) //nolint:gosec // bounded by the media size limit
	copy(buf[8:], "WAVEfmt ")
	le.PutUint32(buf[16:], fmtLen)
	le.PutUint16(buf[20:], wavFormatPCM)
	le.PutUint16(buf[22:], 1)
	le.PutUint32(buf[24:], uint32(rate))           //nolint:gosec // sample rate is a small positive int
	le.PutUint32(buf[28:], uint32(rate*sampleLen)) //nolint:gosec // as above
	le.PutUint16(buf[32:], sampleLen)
	le.PutUint16(buf[34:], pcmBits)
	copy(buf[36:], "data")
	le.PutUint32(buf[40:], uint32(len(pcm))) //nolint:gosec // bounded by the media size limit

	return append(buf, pcm...)
}
