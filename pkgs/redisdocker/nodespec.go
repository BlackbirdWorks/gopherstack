package redisdocker

import (
	"path"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

const (
	tlsDir     = "tls"
	certFile   = "tls.crt"
	keyFile    = "tls.key"
	caFile     = "ca.crt"
	seedPrefix = "seed-"
	seedSuffix = ".rdb"
)

// entryScript copies the shard's seed RDB into the data directory, then starts the server with the admin account
// and replication credentials taken from the environment so they stay out of the argument list.
const entryScript = `if [ -n "$GS_SEED" ] && [ -f "$GS_SEED" ]; then cp "$GS_SEED" /data/dump.rdb; fi
exec "$@" --masteruser ` + AdminUser + ` --masterauth "$GS_ADMIN_PW" --user ` + AdminUser +
	` on ">$GS_ADMIN_PW" '~*' '&*' '+@all'`

func seedName(shard int) string { return seedPrefix + strconv.Itoa(shard) + seedSuffix }

func (g *Group) containerSpec(n *node) container.Spec {
	cfg := g.engine.cfg
	image, binary := ImageFor(g.spec.Engine, g.spec.Version)
	env := []string{"GS_ADMIN_PW=" + g.admin}

	if n.primary() && len(g.spec.Shards[n.shard].Seed) > 0 {
		env = append(env, "GS_SEED="+path.Join(mountDir, seedName(n.shard)))
	}

	port := strconv.Itoa(n.port)

	return container.Spec{
		Name:       "gopherstack-" + cfg.Label + "-" + randomHex(containerNameBytes),
		Image:      image,
		Env:        env,
		Entrypoint: []string{"sh", "-c", entryScript, "gs-redis"},
		Cmd:        append([]string{binary}, g.serverArgs(n)...),
		Mounts:     []string{g.dir + ":" + mountDir + ":ro"},
		Ports:      []string{container.PortSpec(container.BindHostFor(cfg.Host), port, port)},
	}
}

// serverArgs are the server flags for node n. The server listens on its published port number inside the
// container too, so the port a cluster gossips equals the one clients use.
func (g *Group) serverArgs(n *node) []string {
	port := strconv.Itoa(n.port)
	args := []string{"--dir", "/data", "--save", "", "--appendonly", "no", "--protected-mode", "no"}

	if g.spec.TLS {
		args = append(args, "--port", "0", "--tls-port", port,
			"--tls-cert-file", path.Join(mountDir, tlsDir, certFile),
			"--tls-key-file", path.Join(mountDir, tlsDir, keyFile),
			"--tls-ca-cert-file", path.Join(mountDir, tlsDir, caFile),
			"--tls-auth-clients", "no", "--tls-replication", "yes")
	} else {
		args = append(args, "--port", port)
	}

	if !g.spec.ClusterMode {
		return args
	}

	args = append(args, "--cluster-enabled", "yes", "--cluster-config-file", "/data/nodes.conf",
		"--cluster-port", strconv.Itoa(busPort), "--cluster-node-timeout", "5000",
		"--cluster-announce-hostname", g.engine.cfg.Host, "--cluster-preferred-endpoint-type", "hostname")

	if g.spec.TLS {
		args = append(args, "--tls-cluster", "yes")
	}

	return args
}
