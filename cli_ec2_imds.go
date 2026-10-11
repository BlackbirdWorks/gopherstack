package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
	iambackend "github.com/blackbirdworks/gopherstack/services/iam"
	stsbackend "github.com/blackbirdworks/gopherstack/services/sts"
)

const (
	imdsServicePrincipal    = "ec2.amazonaws.com"
	imdsReadHeaderTimeout   = 10 * time.Second
	imdsListenerShutdownTTL = 2 * time.Second
)

var errIMDSBackendUnavailable = errors.New("iam or sts backend unavailable")

// ec2IMDSPlatform resolves instance profiles through IAM and issues role credentials through STS.
// Both backends are resolved per call, never captured at wiring time.
type ec2IMDSPlatform struct {
	byName map[string]service.Registerable
}

func (p *ec2IMDSPlatform) InstanceProfileRole(profileName string) (ec2backend.IMDSProfile, error) {
	iamH, ok := p.byName["IAM"].(*iambackend.Handler)
	if !ok {
		return ec2backend.IMDSProfile{}, errIMDSBackendUnavailable
	}

	bk, ok := iamH.Backend.(*iambackend.InMemoryBackend)
	if !ok {
		return ec2backend.IMDSProfile{}, errIMDSBackendUnavailable
	}

	prof, err := bk.GetInstanceProfile(profileName)
	if err != nil {
		return ec2backend.IMDSProfile{}, err
	}

	out := ec2backend.IMDSProfile{ARN: prof.Arn, ID: prof.InstanceProfileID}
	if len(prof.Roles) == 0 {
		return out, nil
	}

	role, err := bk.GetRole(prof.Roles[0])
	if err != nil {
		return ec2backend.IMDSProfile{}, err
	}

	out.RoleName, out.RoleARN = role.RoleName, role.Arn

	return out, nil
}

func (p *ec2IMDSPlatform) IssueInstanceCredentials(roleArn, sessionName string) (ec2backend.RoleCredentials, error) {
	stsH, ok := p.byName["STS"].(*stsbackend.Handler)
	if !ok {
		return ec2backend.RoleCredentials{}, errIMDSBackendUnavailable
	}

	stsBk, ok := stsH.Backend.(*stsbackend.InMemoryBackend)
	if !ok {
		return ec2backend.RoleCredentials{}, errIMDSBackendUnavailable
	}

	out, err := stsBk.AssumeRoleForService(imdsServicePrincipal, roleArn, sessionName)
	if err != nil {
		return ec2backend.RoleCredentials{}, err
	}

	c := out.AssumeRoleResult.Credentials

	exp, err := time.Parse(time.RFC3339, c.Expiration)
	if err != nil {
		return ec2backend.RoleCredentials{}, err
	}

	return ec2backend.RoleCredentials{
		AccessKeyID: c.AccessKeyID, SecretAccessKey: c.SecretAccessKey, SessionToken: c.SessionToken, Expires: exp,
	}, nil
}

// ec2IMDSListeners gives every compute-backed instance its own metadata endpoint on an ephemeral
// port; SDKs overwrite any path prefix on the endpoint, so the instance has to be identified by port.
type ec2IMDSListeners struct {
	servers map[string]*http.Server
	host    string
	mu      sync.Mutex
}

func (l *ec2IMDSListeners) Listen(instanceID string, h http.Handler) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if srv, ok := l.servers[instanceID]; ok {
		return l.endpoint(srv.Addr), nil
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", ":0")
	if err != nil {
		return "", err
	}

	srv := &http.Server{Handler: h, Addr: ln.Addr().String(), ReadHeaderTimeout: imdsReadHeaderTimeout}
	l.servers[instanceID] = srv

	go func() { _ = srv.Serve(ln) }()

	return l.endpoint(srv.Addr), nil
}

func (l *ec2IMDSListeners) endpoint(addr string) string {
	_, port, _ := net.SplitHostPort(addr)

	return "http://" + net.JoinHostPort(l.host, port)
}

func (l *ec2IMDSListeners) Close(instanceID string) {
	l.mu.Lock()
	srv, ok := l.servers[instanceID]
	delete(l.servers, instanceID)
	l.mu.Unlock()

	if ok {
		go shutdownIMDSServer(srv)
	}
}

func (l *ec2IMDSListeners) closeAll() {
	l.mu.Lock()
	servers := l.servers
	l.servers = map[string]*http.Server{}
	l.mu.Unlock()

	for _, srv := range servers {
		shutdownIMDSServer(srv)
	}
}

func shutdownIMDSServer(srv *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), imdsListenerShutdownTTL)
	defer cancel()

	_ = srv.Shutdown(ctx)
}

func ec2HandlerOf(byName map[string]service.Registerable) *ec2backend.Handler {
	h, _ := byName["EC2"].(*ec2backend.Handler)

	return h
}

// wireEC2IMDS gives EC2 the IAM/STS bridge behind iam/* metadata paths and, for compute-backed
// instances, a per-instance endpoint injected as AWS_EC2_METADATA_SERVICE_ENDPOINT.
func wireEC2IMDS(ctx context.Context, cli *CLI, byName map[string]service.Registerable) {
	h := ec2HandlerOf(byName)
	if h == nil {
		return
	}

	h.SetIMDSPlatform(&ec2IMDSPlatform{byName: byName})

	if cli == nil {
		return
	}

	l := &ec2IMDSListeners{host: cli.Lambda.DockerHost, servers: map[string]*http.Server{}}
	h.SetIMDSListener(l)

	if ctx != nil {
		context.AfterFunc(ctx, l.closeAll)
	}
}

// registerEC2IMDSEndpoints serves each instance's metadata service under /_gopherstack/ec2/imds/<instance-id>/.
func registerEC2IMDSEndpoints(e *echo.Echo, services []service.Registerable) {
	for _, svc := range services {
		h, ok := svc.(*ec2backend.Handler)
		if !ok {
			continue
		}

		for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut} {
			e.Add(method, "/_gopherstack/ec2/imds/:instance/*", h.ServeIMDS)
		}
	}
}
