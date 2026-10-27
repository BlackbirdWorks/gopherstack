package terraform_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	mirrorDirEnv       = "GOPHERSTACK_TOFU_MIRROR_DIR"
	mirrorAttempts     = 5
	mirrorBackoffStep  = 3 * time.Second
	mirrorAttemptLimit = 10 * time.Minute
)

var providerPinRe = regexp.MustCompile(`source\s*=\s*"([\w-]+/[\w-]+)"\s*\n\s*version\s*=\s*"([^"]+)"`)

type providerPin struct {
	source     string
	constraint string
}

func (p providerPin) marker() string {
	sum := sha256.Sum256([]byte(p.source + "@" + p.constraint))

	return ".mirrored-" + hex.EncodeToString(sum[:8])
}

func providerMirrorDir() string {
	if d := os.Getenv(mirrorDirEnv); d != "" {
		return d
	}

	return filepath.Join(os.TempDir(), "gopherstack-tofu-provider-mirror")
}

// scanProviderPins finds every required_providers source/version pair in the
// suite's Go sources and embedded fixtures.
func scanProviderPins() []providerPin {
	seen := map[providerPin]struct{}{}

	collect := func(data []byte) {
		for _, m := range providerPinRe.FindAllSubmatch(data, -1) {
			seen[providerPin{source: string(m[1]), constraint: string(m[2])}] = struct{}{}
		}
	}

	if files, err := filepath.Glob("*.go"); err == nil {
		for _, f := range files {
			if data, readErr := os.ReadFile(f); readErr == nil {
				collect(data)
			}
		}
	}

	_ = fs.WalkDir(fixtureFS, "fixtures", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // best-effort scan
		}

		if data, readErr := fixtureFS.ReadFile(path); readErr == nil {
			collect(data)
		}

		return nil
	})

	pins := make([]providerPin, 0, len(seen))
	for p := range seen {
		pins = append(pins, p)
	}

	sort.Slice(pins, func(i, j int) bool {
		return pins[i].source+pins[i].constraint < pins[j].source+pins[j].constraint
	})

	return pins
}

func mirrorOnePin(ctx context.Context, tofuBin, mirrorDir string, pin providerPin) error {
	workDir, err := os.MkdirTemp("", "tofu-mirror-*")
	if err != nil {
		return fmt.Errorf("creating mirror work dir: %w", err)
	}

	defer os.RemoveAll(workDir)

	name := pin.source[strings.Index(pin.source, "/")+1:]
	hcl := fmt.Sprintf(
		"terraform {\n  required_providers {\n    %s = {\n      source  = %q\n      version = %q\n    }\n  }\n}\n",
		strings.ReplaceAll(name, "-", "_"), pin.source, pin.constraint,
	)

	if writeErr := os.WriteFile(filepath.Join(workDir, "main.tf"), []byte(hcl), 0o644); writeErr != nil {
		return fmt.Errorf("writing mirror config: %w", writeErr)
	}

	cmd := exec.CommandContext(ctx, tofuBin, "providers", "mirror", mirrorDir)
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(), "TF_IN_AUTOMATION=1", "TF_CLI_CONFIG_FILE=/dev/null")

	if out, runErr := cmd.CombinedOutput(); runErr != nil {
		return fmt.Errorf("tofu providers mirror %s %s: %w: %s", pin.source, pin.constraint, runErr, out)
	}

	return nil
}

func ensureMirrorPin(tofuBin, mirrorDir string, pin providerPin, logger *slog.Logger) error {
	markerPath := filepath.Join(mirrorDir, pin.marker())
	if _, err := os.Stat(markerPath); err == nil {
		return nil
	}

	var lastErr error

	for attempt := 1; attempt <= mirrorAttempts; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), mirrorAttemptLimit)
		lastErr = mirrorOnePin(ctx, tofuBin, mirrorDir, pin)

		cancel()

		if lastErr == nil {
			return os.WriteFile(markerPath, nil, 0o644)
		}

		logger.Warn("provider mirror attempt failed", "attempt", attempt, "error", lastErr)

		if attempt < mirrorAttempts {
			time.Sleep(time.Duration(attempt) * mirrorBackoffStep)
		}
	}

	return lastErr
}

// setupProviderMirror populates a filesystem mirror for every provider the
// fixtures pin (the only network step) and points every tofu invocation at it
// through TF_CLI_CONFIG_FILE, so per-test `tofu init` never contacts a registry.
func setupProviderMirror(tofuBin string, logger *slog.Logger) error {
	pins := scanProviderPins()
	if len(pins) == 0 {
		return nil
	}

	mirrorDir := providerMirrorDir()
	if err := os.MkdirAll(mirrorDir, 0o755); err != nil {
		return fmt.Errorf("creating mirror dir: %w", err)
	}

	sources := make([]string, 0, len(pins))

	for _, pin := range pins {
		if err := ensureMirrorPin(tofuBin, mirrorDir, pin, logger); err != nil {
			return err
		}

		sources = append(sources, fmt.Sprintf("%q", "registry.opentofu.org/"+pin.source))
	}

	list := strings.Join(sources, ", ")
	cfg := "provider_installation {\n" +
		"  filesystem_mirror {\n" +
		fmt.Sprintf("    path    = %q\n", mirrorDir) +
		fmt.Sprintf("    include = [%s]\n", list) +
		"  }\n" +
		"  direct {\n" +
		fmt.Sprintf("    exclude = [%s]\n", list) +
		"  }\n" +
		"}\n"

	cfgPath := filepath.Join(mirrorDir, "cli.tfrc")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		return fmt.Errorf("writing CLI config: %w", err)
	}

	if err := os.Setenv("TF_CLI_CONFIG_FILE", cfgPath); err != nil {
		return fmt.Errorf("setting TF_CLI_CONFIG_FILE: %w", err)
	}

	logger.Info("provider mirror ready", "dir", mirrorDir, "providers", len(pins))

	return nil
}
