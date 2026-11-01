package bedrockruntime

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

// Environment variables selecting the inference backend.
const (
	EnvBackend   = "GOPHERSTACK_BEDROCK_BACKEND"
	EnvOllamaURL = "GOPHERSTACK_BEDROCK_OLLAMA_URL"
	EnvModelMap  = "GOPHERSTACK_BEDROCK_MODEL_MAP"

	BackendMock   = "mock"
	BackendOllama = "ollama"

	defaultOllamaURL = "http://localhost:11434"
)

// ErrInvalidLLMConfig is returned for an unusable backend setting.
var ErrInvalidLLMConfig = errors.New("invalid bedrock inference backend config")

// LLMConfig selects how Converse and InvokeModel produce completions.
type LLMConfig struct {
	// Backend is "mock" (default, deterministic placeholder output) or "ollama".
	Backend string
	// OllamaURL is the Ollama base URL; empty means http://localhost:11434.
	OllamaURL string
	// ModelMap is a comma list of "bedrock-id-glob=ollama-model" rules, first match wins.
	ModelMap string
}

// LLMConfigFromEnv reads the GOPHERSTACK_BEDROCK_* variables.
func LLMConfigFromEnv() LLMConfig {
	return LLMConfig{
		Backend:   os.Getenv(EnvBackend),
		OllamaURL: os.Getenv(EnvOllamaURL),
		ModelMap:  os.Getenv(EnvModelMap),
	}
}

type modelRule struct {
	pattern string
	target  string
}

type ollamaBackend struct {
	client  *http.Client
	baseURL string
	rules   []modelRule
}

func newOllamaBackend(cfg LLMConfig) (*ollamaBackend, error) {
	base := strings.TrimRight(strings.TrimSpace(cfg.OllamaURL), "/")
	if base == "" {
		base = defaultOllamaURL
	}

	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("%w: %s must be an http(s) URL, got %q", ErrInvalidLLMConfig, EnvOllamaURL, base)
	}

	rules, err := parseModelMap(cfg.ModelMap)
	if err != nil {
		return nil, err
	}

	return &ollamaBackend{baseURL: base, rules: rules, client: httpclient.New(0)}, nil
}

func parseModelMap(spec string) ([]modelRule, error) {
	var rules []modelRule

	for entry := range strings.SplitSeq(spec, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}

		pattern, target, ok := strings.Cut(entry, "=")
		pattern, target = strings.TrimSpace(pattern), strings.TrimSpace(target)

		if !ok || pattern == "" || target == "" {
			return nil, fmt.Errorf("%w: %s entry %q must look like pattern=ollama-model",
				ErrInvalidLLMConfig, EnvModelMap, entry)
		}

		if _, err := path.Match(pattern, ""); err != nil {
			return nil, fmt.Errorf("%w: %s pattern %q: %w", ErrInvalidLLMConfig, EnvModelMap, pattern, err)
		}

		rules = append(rules, modelRule{pattern: pattern, target: target})
	}

	return rules, nil
}

// modelCandidates lists the identifiers a rule may match: the id itself, the resource
// part of an ARN, and either without a cross-region inference-profile prefix.
func modelCandidates(modelID string) []string {
	id := modelID
	if strings.HasPrefix(id, "arn:") {
		if i := strings.LastIndex(id, "/"); i >= 0 {
			id = id[i+1:]
		}
	}

	out := []string{modelID}
	if id != modelID {
		out = append(out, id)
	}

	for _, p := range []string{"us-gov.", "us.", "eu.", "apac.", "au.", "ca.", "jp.", "global."} {
		if rest, ok := strings.CutPrefix(id, p); ok {
			out = append(out, rest)

			break
		}
	}

	return out
}

// normalizedModelID is the identifier used to pick a request-body family.
func normalizedModelID(modelID string) string {
	c := modelCandidates(modelID)

	return strings.ToLower(c[len(c)-1])
}

// resolve returns the Ollama model for a Bedrock model id.
func (o *ollamaBackend) resolve(modelID string) (string, bool) {
	cands := modelCandidates(modelID)

	for _, r := range o.rules {
		for _, c := range cands {
			if ok, _ := path.Match(r.pattern, c); ok {
				return r.target, true
			}
		}
	}

	return "", false
}

// SetLLM selects the inference backend; BackendMock (or empty) restores the default.
func (b *InMemoryBackend) SetLLM(cfg LLMConfig) error {
	switch strings.ToLower(strings.TrimSpace(cfg.Backend)) {
	case "", BackendMock:
		b.llm.Store(nil)

		return nil
	case BackendOllama:
		o, err := newOllamaBackend(cfg)
		if err != nil {
			return err
		}

		b.llm.Store(o)

		return nil
	default:
		return fmt.Errorf("%w: %s must be %q or %q, got %q",
			ErrInvalidLLMConfig, EnvBackend, BackendMock, BackendOllama, cfg.Backend)
	}
}
