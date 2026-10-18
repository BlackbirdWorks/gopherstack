package ssm

import (
	"context"
	"fmt"
	"net"
	"strconv"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
)

type dockerPortDialer struct{ cli *client.Client }

// NewDockerPortDialer returns a PortDialer that reaches containers by their Docker network IP.
// Remote hosts are dialed from the gopherstack process, not from inside the container.
func NewDockerPortDialer() (PortDialer, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return &dockerPortDialer{cli: cli}, nil
}

func (d *dockerPortDialer) DialTarget(ctx context.Context, containerID, host string, port int) (net.Conn, error) {
	if host == "" {
		ip, err := d.cli.ContainerIP(ctx, containerID)
		if err != nil {
			return nil, err
		}

		host = ip
	}

	var dialer net.Dialer

	conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("dial %s:%d: %w", host, port, err)
	}

	return conn, nil
}
