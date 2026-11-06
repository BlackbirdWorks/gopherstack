package dmsengine

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
)

// ErrSecret reports a Secrets Manager secret that cannot authenticate an endpoint.
var ErrSecret = errors.New("invalid endpoint secret")

// ApplySecret fills the endpoint's credentials from a Secrets Manager secret string. DMS reads username, password,
// host and port; other members are ignored ("Using secrets to access AWS Database Migration Service endpoints").
func ApplySecret(ep *EndpointConfig, secret string) error {
	var raw struct {
		Username string          `json:"username"`
		Password string          `json:"password"`
		Host     string          `json:"host"`
		Port     json.RawMessage `json:"port"`
	}

	if err := json.Unmarshal([]byte(secret), &raw); err != nil {
		return fmt.Errorf("%w: the secret is not a JSON object: %w", ErrSecret, err)
	}

	if raw.Username == "" || raw.Host == "" {
		return fmt.Errorf("%w: the secret needs username, password, host and port members", ErrSecret)
	}

	port, err := secretPort(raw.Port)
	if err != nil {
		return err
	}

	ep.User, ep.Password, ep.Server, ep.Port = raw.Username, raw.Password, raw.Host, port

	return nil
}

func secretPort(raw json.RawMessage) (int, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("%w: the secret needs a port member", ErrSecret)
	}

	var s json.Number
	if err := json.Unmarshal(raw, &s); err != nil {
		return 0, fmt.Errorf("%w: port: %w", ErrSecret, err)
	}

	n, err := strconv.Atoi(s.String())
	if err != nil {
		return 0, fmt.Errorf("%w: port %q is not a number", ErrSecret, s.String())
	}

	return n, nil
}
