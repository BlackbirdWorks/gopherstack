package gcp_test

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"runtime"
	"strconv"
	"testing"

	goredis "github.com/redis/go-redis/v9"
)

const redisFixture = `
resource "google_redis_instance" "basic" {
  name           = "tf-redis-basic"
  display_name   = "terraform basic"
  tier           = "BASIC"
  memory_size_gb = 1
  region         = "us-central1"
  redis_version  = "REDIS_7_0"

  labels = {
    env = "test"
  }

  redis_configs = {
    maxmemory-policy = "allkeys-lru"
  }
}

resource "google_redis_instance" "secure" {
  name                    = "tf-redis-secure"
  tier                    = "STANDARD_HA"
  memory_size_gb          = 2
  region                  = "us-central1"
  location_id             = "us-central1-a"
  alternative_location_id = "us-central1-b"
  redis_version           = "REDIS_7_0"
  auth_enabled            = true
  transit_encryption_mode = "SERVER_AUTHENTICATION"
  read_replicas_mode      = "READ_REPLICAS_ENABLED"
  replica_count           = 2

  maintenance_policy {
    weekly_maintenance_window {
      day = "TUESDAY"
      start_time {
        hours   = 3
        minutes = 0
        seconds = 0
        nanos   = 0
      }
    }
  }
}

output "basic_host" {
  value = google_redis_instance.basic.host
}

output "basic_port" {
  value = google_redis_instance.basic.port
}

output "secure_host" {
  value = google_redis_instance.secure.host
}

output "secure_port" {
  value = google_redis_instance.secure.port
}

output "secure_auth" {
  value     = google_redis_instance.secure.auth_string
  sensitive = true
}

output "secure_ca" {
  value = google_redis_instance.secure.server_ca_certs[0].cert
}
`

// redisClient dials an instance's endpoint, which listens inside the container, via the container's bridge IP.
func redisClient(t *testing.T, port, auth string, pool *x509.CertPool) *goredis.Client {
	t.Helper()

	ip, err := sharedContainer.ContainerIP(t.Context())
	if err != nil {
		t.Fatalf("container IP: %v", err)
	}

	n, err := strconv.Atoi(port)
	if err != nil {
		t.Fatalf("port %q: %v", port, err)
	}

	opts := &goredis.Options{Addr: net.JoinHostPort(ip, strconv.Itoa(n)), Password: auth}
	if pool != nil {
		opts.TLSConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12, ServerName: "localhost"}
	}

	c := goredis.NewClient(opts)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func TestTerraformGCP_Redis(t *testing.T) {
	t.Parallel()

	r := newRunner(t, redisFixture)

	r.lifecycleThen(func() {
		if runtime.GOOS != "linux" {
			t.Log("skipping the Redis connection check: the container bridge IP is only routable on Linux")

			return
		}

		basic := redisClient(t, r.output("basic_port"), "", nil)
		if err := basic.Set(t.Context(), "k", "v", 0).Err(); err != nil {
			t.Fatalf("basic SET: %v", err)
		}

		if v, err := basic.Get(t.Context(), "k").Result(); err != nil || v != "v" {
			t.Fatalf("basic GET = %q, %v", v, err)
		}

		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(r.output("secure_ca"))) {
			t.Fatal("secure_ca is not a PEM certificate")
		}

		if err := redisClient(t, r.output("secure_port"), "", pool).Ping(t.Context()).Err(); err == nil {
			t.Fatal("secure instance accepted an unauthenticated PING")
		}

		secure := redisClient(t, r.output("secure_port"), r.output("secure_auth"), pool)
		if err := secure.Ping(t.Context()).Err(); err != nil {
			t.Fatalf("secure PING with the auth string over TLS: %v", err)
		}
	})
}
