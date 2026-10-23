package gcpspanner

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	emulatorGRPCPort = "9010"
	readyInterval    = 250 * time.Millisecond
	probeParent      = "projects/gopherstack-probe"
	probeTimeout     = 2 * time.Second
)

// dockerLauncher runs the official Cloud Spanner emulator image as one container.
type dockerLauncher struct {
	lr      *gcpserve.LazyRuntime
	pool    *portalloc.Allocator
	image   string
	startup time.Duration
}

func newDockerLauncher(
	log *slog.Logger,
	pool *portalloc.Allocator,
	image string,
	startup time.Duration,
) *dockerLauncher {
	return &dockerLauncher{
		lr:      gcpserve.NewLazyRuntime(log, "gcpspanner.runtime"),
		pool:    pool,
		image:   image,
		startup: startup,
	}
}

// Close releases the runtime client.
func (l *dockerLauncher) Close() { _ = l.lr.Close() }

func (l *dockerLauncher) launch(ctx context.Context) (*running, error) {
	rt, err := l.lr.Get()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRuntimeUnavailable, err)
	}

	ctx, cancel := context.WithTimeout(ctx, l.startup)
	defer cancel()

	var port int

	prepare := func() (container.Spec, error) {
		p, perr := portalloc.AcquireFrom(l.pool, "gcpspanner")
		if perr != nil {
			return container.Spec{}, fmt.Errorf("host port: %w", perr)
		}

		port = p

		return container.Spec{
			Name:  "gopherstack-gcpspanner-" + strconv.Itoa(p) + "-" + strconv.Itoa(os.Getpid()),
			Image: l.image,
			Ports: []string{container.PortSpec(container.LoopbackHost, strconv.Itoa(p), emulatorGRPCPort)},
		}, nil
	}

	release := func() { portalloc.ReleaseTo(l.pool, port) }

	id, err := container.CreateAndStartRetry(ctx, rt, prepare, release)
	if err != nil {
		return nil, fmt.Errorf("start %s: %w", l.image, err)
	}

	addr := container.LoopbackHost + ":" + strconv.Itoa(port)
	stop := func(sctx context.Context) {
		_ = rt.StopAndRemove(sctx, id)

		release()
	}

	if err = waitEmulator(ctx, addr); err != nil {
		stop(context.WithoutCancel(ctx))

		return nil, err
	}

	return &running{addr: addr, stop: stop}, nil
}

// waitEmulator blocks until the emulator answers an admin call.
func waitEmulator(ctx context.Context, addr string) error {
	cc, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("dial emulator: %w", err)
	}

	defer func() { _ = cc.Close() }()

	client := instancepb.NewInstanceAdminClient(cc)
	tick := time.NewTicker(readyInterval)

	defer tick.Stop()

	for {
		cctx, cancel := context.WithTimeout(ctx, probeTimeout)
		_, err = client.ListInstances(cctx, &instancepb.ListInstancesRequest{Parent: probeParent})

		cancel()

		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("emulator did not answer: %w", ctx.Err())
		case <-tick.C:
		}
	}
}
