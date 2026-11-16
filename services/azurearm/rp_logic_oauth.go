package azurearm

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	logicOAuthDefaultAuthority = "https://login.microsoftonline.com/"
	logicOAuthMaxResponse      = 1 << 20
)

var errLogicTokenRequest = errors.New("token request failed")

type logicOAuthToken struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
}

// fetchOAuthToken performs the client-credentials grant for an ActiveDirectoryOAuth authentication block with the
// Secret credential type and returns the Authorization header value. The request goes through the provider's
// loopback-gated client, so only an authority inside the emulator is reachable unless external hosts are allowed.
func (rs *logicRunState) fetchOAuthToken(auth map[string]any) (string, error) {
	if ct := stringOf(auth["credentialType"]); ct != "" && !strings.EqualFold(ct, "Secret") {
		return "", inputErr("the ActiveDirectoryOAuth credential type '%s' is not supported by this emulator", ct)
	}

	for _, k := range []string{"tenant", "audience", fieldClientID, apimCSecret} {
		if stringOf(auth[k]) == "" {
			return "", inputErr("the ActiveDirectoryOAuth authentication requires '%s'", k)
		}
	}

	authority := orStr(stringOf(auth["authority"]), logicOAuthDefaultAuthority)
	endpoint := strings.TrimRight(authority, "/") + "/" + url.PathEscape(stringOf(auth["tenant"])) + "/oauth2/token"

	form := url.Values{
		"grant_type": {"client_credentials"}, "client_id": {stringOf(auth[fieldClientID])},
		"client_secret": {stringOf(auth[apimCSecret])}, "resource": {stringOf(auth["audience"])},
	}

	ctx, cancel := context.WithTimeout(rs.ctx, logicHTTPTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", fmt.Errorf("%w: %w", errLogicTokenRequest, err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := rs.p.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: %w", errLogicTokenRequest, err)
	}

	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, logicOAuthMaxResponse))
	if err != nil {
		return "", fmt.Errorf("%w: %w", errLogicTokenRequest, err)
	}

	var tok logicOAuthToken
	if resp.StatusCode != http.StatusOK || json.Unmarshal(raw, &tok, jsonx.V1()) != nil || tok.AccessToken == "" {
		return "", fmt.Errorf(
			"%w: the authority answered %d without an access token",
			errLogicTokenRequest,
			resp.StatusCode,
		)
	}

	return orStr(tok.TokenType, "Bearer") + " " + tok.AccessToken, nil
}
