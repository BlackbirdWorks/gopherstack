package registryauth

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const helperTimeout = 15 * time.Second

var helperName = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

type configEntry struct {
	Auth          string `json:"auth"`
	Username      string `json:"username"`
	Password      string `json:"password"`
	IdentityToken string `json:"identitytoken"`
}

type dockerConfig struct {
	Auths       map[string]configEntry `json:"auths"`
	CredHelpers map[string]string      `json:"credHelpers"`
	CredsStore  string                 `json:"credsStore"`
}

// authConfig mirrors the Docker engine AuthConfig JSON.
type authConfig struct {
	Username      string `json:"username,omitempty"`
	Password      string `json:"password,omitempty"`
	ServerAddress string `json:"serveraddress,omitempty"`
	IdentityToken string `json:"identitytoken,omitempty"`
}

func configPath() string {
	if d := os.Getenv("DOCKER_CONFIG"); d != "" {
		return filepath.Join(d, "config.json")
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}

	return filepath.Join(home, ".docker", "config.json")
}

func loadDockerConfig() *dockerConfig {
	p := configPath()
	if p == "" {
		return nil
	}

	data, err := os.ReadFile(p)
	if err != nil {
		return nil
	}

	var cfg dockerConfig
	if json.Unmarshal(data, &cfg) != nil {
		return nil
	}

	return &cfg
}

func (c *dockerConfig) helperFor(host string) string {
	for k, v := range c.CredHelpers {
		if normalizeHost(k) == host {
			return v
		}
	}

	return c.CredsStore
}

func (c *dockerConfig) staticFor(host string) (authConfig, bool) {
	for k, e := range c.Auths {
		if normalizeHost(k) != host {
			continue
		}

		if ac, ok := e.toAuthConfig(host); ok {
			return ac, true
		}
	}

	return authConfig{}, false
}

func (e configEntry) toAuthConfig(host string) (authConfig, bool) {
	ac := authConfig{ServerAddress: host}

	switch {
	case e.IdentityToken != "":
		ac.IdentityToken = e.IdentityToken
	case e.Username != "":
		ac.Username, ac.Password = e.Username, e.Password
	case e.Auth != "":
		raw, err := base64.StdEncoding.DecodeString(e.Auth)
		if err != nil {
			return authConfig{}, false
		}

		u, p, ok := strings.Cut(string(raw), ":")
		if !ok {
			return authConfig{}, false
		}

		ac.Username, ac.Password = u, p
	default:
		return authConfig{}, false
	}

	return ac, true
}

func serverURL(host string) string {
	if host == hubHost {
		return hubServerURL
	}

	return host
}

type helperReply struct {
	Username string `json:"Username"`
	Secret   string `json:"Secret"`
}

// runHelper executes docker-credential-<name> get; stderr is discarded since
// helpers may echo secrets.
func runHelper(ctx context.Context, name, host string) (authConfig, bool) {
	ctx, cancel := context.WithTimeout(ctx, helperTimeout)
	defer cancel()

	if !helperName.MatchString(name) {
		return authConfig{}, false
	}

	//nolint:gosec // name is validated against helperName and comes from the operator's docker config
	cmd := exec.CommandContext(ctx, "docker-credential-"+name, "get")
	cmd.Stdin = strings.NewReader(serverURL(host))

	var out bytes.Buffer

	cmd.Stdout = &out

	if cmd.Run() != nil {
		return authConfig{}, false
	}

	var rep helperReply
	if json.Unmarshal(out.Bytes(), &rep) != nil || rep.Secret == "" {
		return authConfig{}, false
	}

	ac := authConfig{ServerAddress: host}
	if rep.Username == "<token>" {
		ac.IdentityToken = rep.Secret
	} else {
		ac.Username, ac.Password = rep.Username, rep.Secret
	}

	return ac, true
}

// fromDockerConfig resolves host via config.json: helpers first, then static auths.
func fromDockerConfig(ctx context.Context, host string) (authConfig, bool) {
	cfg := loadDockerConfig()
	if cfg == nil {
		return authConfig{}, false
	}

	if name := cfg.helperFor(host); name != "" {
		if ac, ok := runHelper(ctx, name, host); ok {
			return ac, true
		}
	}

	return cfg.staticFor(host)
}
