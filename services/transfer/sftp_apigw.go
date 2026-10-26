package transfer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"golang.org/x/crypto/ssh"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	apiGatewayIDPTimeout = 30 * time.Second
	apiGatewayMaxBody    = 1 << 20
	httpDefaultPort      = 80
	httpsDefaultPort     = 443
	maxPort              = 65535
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

// Get implements APIGatewayClient. Only loopback targets are dialled; the request URL is
// rebuilt from constant prefixes plus a numeric port so no caller text selects the host.
func (c *HTTPAPIGatewayClient) Get(
	ctx context.Context, rawURL string, headers map[string]string,
) (int, []byte, error) {
	target, err := loopbackTarget(rawURL)
	if err != nil {
		return 0, nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
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

// loopbackTarget validates rawURL and rebuilds it as http(s)://<loopback>:<port><path>?<query>.
func loopbackTarget(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("%w: invalid identity provider url", errSFTPDenied)
	}

	port, err := urlPort(u)
	if err != nil {
		return "", err
	}

	var origin string

	switch {
	case u.Scheme == "http" && u.Hostname() == "127.0.0.1":
		origin = fmt.Sprintf("http://127.0.0.1:%d", port)
	case u.Scheme == "https" && u.Hostname() == "127.0.0.1":
		origin = fmt.Sprintf("https://127.0.0.1:%d", port)
	case u.Scheme == "http" && u.Hostname() == "localhost":
		origin = fmt.Sprintf("http://localhost:%d", port)
	case u.Scheme == "https" && u.Hostname() == "localhost":
		origin = fmt.Sprintf("https://localhost:%d", port)
	case u.Scheme == "http" && u.Hostname() == "::1":
		origin = fmt.Sprintf("http://[::1]:%d", port)
	case u.Scheme == "https" && u.Hostname() == "::1":
		origin = fmt.Sprintf("https://[::1]:%d", port)
	default:
		return "", fmt.Errorf("%w: identity provider url must target the emulator", errSFTPDenied)
	}

	target := origin + u.EscapedPath()
	if u.RawQuery != "" {
		target += "?" + u.RawQuery
	}

	return target, nil
}

func urlPort(u *url.URL) (int, error) {
	ps := u.Port()
	if ps == "" {
		if u.Scheme == "https" {
			return httpsDefaultPort, nil
		}

		return httpDefaultPort, nil
	}

	port, err := strconv.Atoi(ps)
	if err != nil || port < 1 || port > maxPort {
		return 0, fmt.Errorf("%w: invalid identity provider url", errSFTPDenied)
	}

	return port, nil
}

var executeAPIHost = regexp.MustCompile(`^[a-z0-9]+\.execute-api\.[a-z0-9-]+\.amazonaws\.com(\.cn)?$`)

func apiGatewayHostAllowed(host string) bool {
	switch host {
	case "127.0.0.1", "localhost", "::1":
		return true
	}

	return executeAPIHost.MatchString(host)
}

// apiGatewayConfigURL builds {Url}/servers/{serverId}/users/{username}/config per the Transfer docs.
func apiGatewayConfigURL(base, serverID, user, ip string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", fmt.Errorf("%w: invalid identity provider url", errSFTPDenied)
	}

	if !apiGatewayHostAllowed(u.Hostname()) {
		return "", fmt.Errorf("%w: identity provider url must target API Gateway", errSFTPDenied)
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
