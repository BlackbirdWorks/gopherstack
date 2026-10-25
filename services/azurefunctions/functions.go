package azurefunctions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

const (
	adminFunctionsPath = "/admin/functions"
	maxAdminBytes      = 32 << 20
)

// FunctionInfo describes one function of an app.
type FunctionInfo struct {
	Config     map[string]any
	Name       string
	Language   string
	InvokeURL  string
	IsDisabled bool
}

type adminFunction struct {
	Config     map[string]any `json:"config"`
	Name       string         `json:"name"`
	Language   string         `json:"language"`
	IsDisabled bool           `json:"isDisabled"`
}

// httpTrigger returns the function's HTTP trigger binding.
func httpTrigger(cfg map[string]any) (map[string]any, bool) {
	bindings, _ := cfg["bindings"].([]any)
	for _, raw := range bindings {
		bd, _ := raw.(map[string]any)
		if t, _ := bd["type"].(string); strings.EqualFold(t, "httpTrigger") {
			return bd, true
		}
	}

	return nil, false
}

func (b *InMemoryBackend) invokeURL(name, fn string, cfg map[string]any) string {
	bd, ok := httpTrigger(cfg)
	if !ok {
		return ""
	}

	route := fn
	if r, _ := bd["route"].(string); r != "" {
		route = r
	}

	return "http://" + b.HostName(name) + "/api/" + route
}

func (b *InMemoryBackend) adminFunctions(ctx context.Context, name string) ([]adminFunction, error) {
	port, err := b.ensureStarted(ctx, name)
	if err != nil {
		return nil, err
	}

	b.mu.RLock("adminFunctions")
	master := ""

	if a, ok := b.apps[strs.Fold(name)]; ok {
		master = a.MasterKey
	}
	b.mu.RUnlock()

	u := url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(container.LoopbackHost, strconv.Itoa(port)),
		Path:   adminFunctionsPath,
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("build admin request: %w", err)
	}

	req.Header.Set("X-Functions-Key", master)

	resp, err := (&http.Client{Transport: b.transport}).Do(req)
	if err != nil {
		return nil, fmt.Errorf("list functions on host: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxAdminBytes))
	if err != nil {
		return nil, fmt.Errorf("read host functions: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%w: host answered %d to %s", errStartFailed, resp.StatusCode, adminFunctionsPath)
	}

	var fns []adminFunction
	if err = json.Unmarshal(body, &fns); err != nil {
		return nil, fmt.Errorf("decode host functions: %w", err)
	}

	return fns, nil
}

// Functions lists the app's functions. With a container runtime they come from the running host's admin API,
// which also sees code-first (no function.json) functions; without one, from the deployed package's
// function.json files.
func (b *InMemoryBackend) Functions(ctx context.Context, name string) ([]FunctionInfo, error) {
	b.mu.RLock("Functions")

	a, err := b.appLocked(name)
	if err != nil {
		b.mu.RUnlock()

		return nil, err
	}

	servable, zipData, worker := a.servable(), a.Zip, ""
	if rc, rcErr := chooseRuntime(a.Settings, a.LinuxFx); rcErr == nil {
		worker = rc.worker
	}
	b.mu.RUnlock()

	if !servable {
		return nil, nil
	}

	fns, err := b.adminFunctions(ctx, name)
	if err == nil {
		out := make([]FunctionInfo, 0, len(fns))
		for _, f := range fns {
			out = append(out, FunctionInfo{
				Name: f.Name, Language: f.Language, Config: f.Config, IsDisabled: f.IsDisabled,
				InvokeURL: b.invokeURL(name, f.Name, f.Config),
			})
		}

		return out, nil
	}

	if !errors.Is(err, errNoRuntime) && !errors.Is(err, errNoHost) {
		return nil, err
	}

	defs := scanFunctions(zipData)
	out := make([]FunctionInfo, 0, len(defs))

	for _, d := range defs {
		disabled, _ := d.Config["disabled"].(bool)
		out = append(out, FunctionInfo{
			Name: d.Name, Language: worker, Config: d.Config, IsDisabled: disabled,
			InvokeURL: b.invokeURL(name, d.Name, d.Config),
		})
	}

	return out, nil
}

// Function returns one function of the app.
func (b *InMemoryBackend) Function(ctx context.Context, name, fn string) (FunctionInfo, error) {
	fns, err := b.Functions(ctx, name)
	if err != nil {
		return FunctionInfo{}, err
	}

	for _, f := range fns {
		if strs.Equal(f.Name, fn) {
			return f, nil
		}
	}

	return FunctionInfo{}, ErrFunctionNotFound
}
