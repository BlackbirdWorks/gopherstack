package ecr

import (
	"context"
	"encoding/base64"
	"strings"
	"time"
)

const (
	dummyUser     = "AWS"
	dummyPassword = "dummy-password"
	tokenTTL      = 12 * time.Hour
)

// getAuthorizationTokenInput is the request body for GetAuthorizationToken.
// RegistryIDs specifies which registries to fetch tokens for; when empty a
// single token for the default registry is returned.
type getAuthorizationTokenInput struct {
	RegistryIDs []string `json:"registryIds,omitempty"`
}

type authorizationDataView struct {
	AuthorizationToken string `json:"authorizationToken"`
	ProxyEndpoint      string `json:"proxyEndpoint,omitempty"`
	ExpiresAt          int64  `json:"expiresAt"`
}

type getAuthorizationTokenOutput struct {
	AuthorizationData []authorizationDataView `json:"authorizationData"`
}

func (h *Handler) handleGetAuthorizationToken(
	_ context.Context,
	in *getAuthorizationTokenInput,
) (*getAuthorizationTokenOutput, error) {
	token := generateAuthToken()
	expiresAt := time.Now().Add(tokenTTL).Unix()

	proxyEndpoint := h.Backend.ProxyEndpoint()
	if proxyEndpoint != "" &&
		!strings.HasPrefix(proxyEndpoint, "https://") &&
		!strings.HasPrefix(proxyEndpoint, "http://") {
		proxyEndpoint = "https://" + proxyEndpoint
	}

	entry := authorizationDataView{
		AuthorizationToken: token,
		ExpiresAt:          expiresAt,
		ProxyEndpoint:      proxyEndpoint,
	}

	// When specific registry IDs are requested, return one token per registry.
	// Since this is a single-account simulator, the token is identical for each.
	if len(in.RegistryIDs) > 0 {
		data := make([]authorizationDataView, 0, len(in.RegistryIDs))
		for range in.RegistryIDs {
			data = append(data, entry)
		}

		return &getAuthorizationTokenOutput{AuthorizationData: data}, nil
	}

	return &getAuthorizationTokenOutput{
		AuthorizationData: []authorizationDataView{entry},
	}, nil
}

// generateAuthToken produces the ECR authorization token in AWS's structure:
// base64(AWS:<password>). The emulator uses a stable dummy password so clients
// (and docker login) get a deterministic credential.
func generateAuthToken() string {
	return base64.StdEncoding.EncodeToString([]byte(dummyUser + ":" + dummyPassword))
}
