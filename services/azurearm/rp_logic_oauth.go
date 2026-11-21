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
	cert, err := oauthCredential(auth)
	if err != nil {
		return "", err
	}

	authority := orStr(stringOf(auth["authority"]), logicOAuthDefaultAuthority)
	endpoint := strings.TrimRight(authority, "/") + "/" + url.PathEscape(stringOf(auth["tenant"])) + "/oauth2/token"

	form := url.Values{
		"grant_type": {"client_credentials"}, "client_id": {stringOf(auth[fieldClientID])},
		"resource": {stringOf(auth["audience"])},
	}

	if err = rs.setOAuthCredential(form, auth, endpoint, cert); err != nil {
		return "", err
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

// oauthCredential validates the block and reports whether it uses the Certificate credential type.
func oauthCredential(auth map[string]any) (bool, error) {
	ct := stringOf(auth["credentialType"])
	cert := strings.EqualFold(ct, "Certificate")

	if ct != "" && !cert && !strings.EqualFold(ct, "Secret") {
		return false, inputErr("the ActiveDirectoryOAuth credential type '%s' is not supported by this emulator", ct)
	}

	required := []string{"tenant", "audience", fieldClientID, apimCSecret}
	if cert {
		required = []string{"tenant", "audience", fieldClientID, "pfx"}
	}

	for _, k := range required {
		if stringOf(auth[k]) == "" {
			return false, inputErr("the ActiveDirectoryOAuth authentication requires '%s'", k)
		}
	}

	return cert, nil
}

func (rs *logicRunState) setOAuthCredential(form url.Values, auth map[string]any, endpoint string, cert bool) error {
	if !cert {
		form.Set("client_secret", stringOf(auth[apimCSecret]))

		return nil
	}

	pfx, err := loadLogicPFX(auth)
	if err != nil {
		return err
	}

	assertion, err := pfx.clientAssertion(stringOf(auth[fieldClientID]), endpoint, rs.clock().UTC())
	if err != nil {
		return err
	}

	form.Set("client_assertion_type", "urn:ietf:params:oauth:client-assertion-type:jwt-bearer")
	form.Set("client_assertion", assertion)

	return nil
}
