package transfer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	apiGatewayIDPTimeout = 30 * time.Second
	apiGatewayMaxBody    = 1 << 20
)

// APIGatewayClient performs the GET an API_GATEWAY identity provider needs.
// The app resolves URLs that target the emulator's own API Gateway in-process.
type APIGatewayClient interface {
	Get(ctx context.Context, rawURL string, headers map[string]string) (status int, body []byte, err error)
}

// HTTPAPIGatewayClient calls the identity provider URL over HTTP.
type HTTPAPIGatewayClient struct{ client *http.Client }

// NewHTTPAPIGatewayClient returns a client using its own cloned transport.
func NewHTTPAPIGatewayClient() *HTTPAPIGatewayClient {
	return &HTTPAPIGatewayClient{client: httpclient.New(apiGatewayIDPTimeout)}
}

// Get implements APIGatewayClient.
func (c *HTTPAPIGatewayClient) Get(
	ctx context.Context, rawURL string, headers map[string]string,
) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, nil, err
	}

	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, apiGatewayMaxBody))

	return resp.StatusCode, body, err
}

// apiGatewayConfigURL builds {Url}/servers/{serverId}/users/{username}/config per the Transfer docs.
func apiGatewayConfigURL(base, serverID, user, ip string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("%w: invalid identity provider url", errSFTPDenied)
	}

	u = u.JoinPath("servers", serverID, "users", user, "config")
	q := u.Query()
	q.Set("protocol", protocolSFTP)
	q.Set("sourceIp", ip)
	u.RawQuery = q.Encode()

	return u.String(), nil
}

func (s *sftpServer) apiGatewayLogin(
	srv *Server, c ssh.ConnMetadata, key ssh.PublicKey, password string,
) (*sftpSession, error) {
	if s.cfg.APIGateway == nil || srv.IdentityProviderDetails == nil || srv.IdentityProviderDetails.URL == "" {
		return nil, errSFTPDenied
	}

	target, err := apiGatewayConfigURL(srv.IdentityProviderDetails.URL, s.serverID, c.User(), remoteIP(c.RemoteAddr()))
	if err != nil {
		return nil, err
	}

	headers := map[string]string{}
	if password != "" {
		headers["Password"] = password
	}

	status, body, err := s.cfg.APIGateway.Get(s.ctx, target, headers)
	if err != nil {
		logger.Load(s.ctx).Debug("transfer: api gateway identity provider call failed", "server", s.serverID)

		return nil, err
	}

	if status != http.StatusOK {
		return nil, errSFTPDenied
	}

	var resp idpAuthResponse
	if jerr := json.Unmarshal(body, &resp); jerr != nil {
		return nil, errSFTPDenied
	}

	return resp.session(key)
}
