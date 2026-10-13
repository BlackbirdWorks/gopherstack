package main

import (
	"context"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/internal/linklocal"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ec2backend "github.com/blackbirdworks/gopherstack/services/ec2"
)

const (
	envLinkLocal      = "LINKLOCAL_PROXY"
	envLinkLocalImage = "LINKLOCAL_IMAGE"
	ec2InstanceLabel  = "gopherstack.ec2.instance"
)

// linkLocalEnabled reports whether the opt-in link-local relay is requested.
func linkLocalEnabled(getenv func(string) string) bool {
	on, err := strconv.ParseBool(strings.TrimSpace(getenv(envLinkLocal)))

	return err == nil && on
}

// wireLinkLocal serves ECS task credentials at 169.254.170.2 and IMDS at 169.254.169.254 inside
// docker-backed task and instance containers, when LINKLOCAL_PROXY is set.
func wireLinkLocal(ctx context.Context, cli *CLI, byName map[string]service.Registerable) {
	if cli == nil || cli.Port == "" || !linkLocalEnabled(os.Getenv) {
		return
	}

	host, port, err := net.SplitHostPort(
		strings.TrimPrefix(lambdaEndpointURL(cli.Lambda.DockerHost, cli.Port), "http://"),
	)
	if err != nil {
		return
	}

	image := os.Getenv(envLinkLocalImage)
	log := logger.Load(ctx)

	if bk := ecsBackendOf(byName); bk != nil {
		if m, mErr := linklocal.NewFromEnv(linklocal.ECSSpec(image, host, port)); mErr != nil {
			log.WarnContext(ctx, "ecs link-local relay disabled", "error", mErr)
		} else {
			bk.SetLinkLocal(m)
		}
	}

	if h := ec2HandlerOf(byName); h != nil {
		if m, mErr := linklocal.NewFromEnv(linklocal.IMDSSpec(image, host, port, ec2InstanceLabel)); mErr != nil {
			log.WarnContext(ctx, "ec2 link-local relay disabled", "error", mErr)
		} else {
			h.SetLinkLocal(m)
		}
	}
}

var _ ec2backend.LinkLocal = (*linklocal.Manager)(nil)
