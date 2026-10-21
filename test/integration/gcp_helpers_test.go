package integration_test

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/google/uuid"
)

// gcpPorts lists every dedicated GCP emulator port exposed by the test container.
var gcpPorts = []string{ //nolint:gochecknoglobals // static port table consumed by TestMain
	"4443", "8080", "8085", "8086", "8087", "8123", "8124", "8125", "8126", "8128", "8129", "8131", "9050",
}

// gcpAddrs maps a container GCP port to its host "localhost:<port>" address.
var gcpAddrs = map[string]string{} //nolint:gochecknoglobals // populated once by TestMain

const gcpProject = "gcp-it-project"

func gcpExposedPorts() []string {
	out := make([]string, 0, len(gcpPorts))
	for _, p := range gcpPorts {
		out = append(out, p+"/tcp")
	}

	return out
}

func gcpWaitStrategies() []wait.Strategy {
	out := make([]wait.Strategy, 0, len(gcpPorts))
	for _, p := range gcpPorts {
		out = append(out, wait.ForListeningPort(p+"/tcp").WithStartupTimeout(60*time.Second))
	}

	return out
}

func resolveGCPAddrs(ctx context.Context, c testcontainers.Container, logger *slog.Logger) {
	for _, p := range gcpPorts {
		ep := resolveMappedEndpoint(ctx, c, logger, p, "http", "GCP emulator port "+p, "GCP")
		if ep != "" {
			gcpAddrs[p] = strings.TrimPrefix(ep, "http://")
		}
	}
}

// gcpAddr returns the host address for a container GCP port, skipping the
// test when the suite is not running against the managed container.
func gcpAddr(t *testing.T, port string) string {
	t.Helper()

	addr := gcpAddrs[port]
	if addr == "" {
		t.Skipf("GCP emulator port %s not available (external endpoint mode)", port)
	}

	return addr
}

// gcpGRPCOpts returns insecure, unauthenticated client options for a gRPC GCP emulator.
func gcpGRPCOpts(t *testing.T, port string) []option.ClientOption {
	t.Helper()

	return []option.ClientOption{
		option.WithEndpoint(gcpAddr(t, port)),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
}

// gcpName returns a unique lowercase resource id with the given prefix.
func gcpName(prefix string) string {
	return prefix + "-" + strings.ReplaceAll(uuid.NewString(), "-", "")[:12]
}
