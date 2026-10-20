# DNS Setup

Gopherstack includes an optional embedded DNS server. When enabled, services that create network-addressable resources (RDS, Redshift, ElastiCache, OpenSearch) automatically register synthetic hostnames that resolve to a configurable IP address.

## Enabling the DNS server

```bash
gopherstack --dns-addr :10053
```

Or with an environment variable:

```bash
DNS_ADDR=:10053 gopherstack
```

The server binds to the specified address (UDP + TCP). An empty `--dns-addr` (the default) disables the DNS server entirely.

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--dns-addr` | `DNS_ADDR` | `` (disabled) | Address for the embedded DNS server (e.g. `:10053`) |
| `--dns-resolve-ip` | `DNS_RESOLVE_IP` | `127.0.0.1` | IP address synthetic hostnames resolve to |

## Hostname formats

Each service uses a predictable hostname format:

| Service | Format | Example |
|---------|--------|---------|
| RDS | `<identifier>.gopherstack.internal` | `my-db.gopherstack.internal` |
| Redshift | `<identifier>.gopherstack.internal` | `my-cluster.gopherstack.internal` |
| ElastiCache | `<cluster-id>.gopherstack.internal` | `my-redis.gopherstack.internal` |
| OpenSearch | `<domain>.gopherstack.internal` | `my-search.gopherstack.internal` |

All hostnames resolve to the `DNS_RESOLVE_IP` (default `127.0.0.1`).

## Forwarding `.gopherstack.internal` to Gopherstack

To make hostnames resolve in your local shell, configure your OS resolver to forward the `.gopherstack.internal` zone to `127.0.0.1:10053`.

### dnsmasq (Linux / macOS with Homebrew)

Add to `/etc/dnsmasq.conf` (or `/usr/local/etc/dnsmasq.conf` on macOS):

```
server=/gopherstack.internal/127.0.0.1#10053
```

Restart dnsmasq:

```bash
sudo systemctl restart dnsmasq    # Linux
brew services restart dnsmasq     # macOS
```

### systemd-resolved (Ubuntu / Debian)

Create `/etc/systemd/resolved.conf.d/gopherstack.conf`:

```ini
[Resolve]
DNS=127.0.0.53
Domains=~gopherstack.internal
```

Add a `resolved` drop-in for the stub zone:

```bash
sudo mkdir -p /etc/systemd/resolved.conf.d
sudo tee /etc/systemd/resolved.conf.d/gopherstack.conf > /dev/null << 'EOF'
[Resolve]
DNS=127.0.0.1:10053
Domains=~gopherstack.internal
EOF
sudo systemctl restart systemd-resolved
```

### macOS — `/etc/resolver/`

```bash
sudo mkdir -p /etc/resolver
sudo tee /etc/resolver/gopherstack.internal > /dev/null << 'EOF'
nameserver 127.0.0.1
port 10053
EOF
```

Flush the DNS cache:

```bash
sudo dscacheutil -flushcache; sudo killall -HUP mDNSResponder
```

### Test resolution

```bash
dig @127.0.0.1 -p 10053 my-db.gopherstack.internal A
# Should return 127.0.0.1

# After configuring OS resolver:
nslookup my-db.gopherstack.internal
# Should return 127.0.0.1
```

## Usage example with RDS

```bash
# Start Gopherstack with DNS
gopherstack --dns-addr :10053

# Create a DB instance
aws --endpoint-url http://localhost:8000 rds create-db-instance \
    --db-instance-identifier my-db \
    --db-instance-class db.t3.micro \
    --engine postgres \
    --master-username admin \
    --master-user-password password \
    --allocated-storage 20

# The endpoint hostname is now registered in DNS
# With OS resolver configured:
psql -h my-db.gopherstack.internal -U admin
```

> **Note:** RDS and Redshift in Gopherstack are metadata-only stubs; no real database process is started at the resolved address. To test actual database connectivity, run a real PostgreSQL/MySQL container separately and point your DNS entry at its address using `--dns-resolve-ip`.

## Transparent endpoint injection

Unmodified SDKs and CLIs that target real `*.amazonaws.com` endpoints (no
`endpoint_url`) can reach Gopherstack, like LocalStack's DNS + transparent
injection.

```bash
sudo gopherstack --transparent-dns --transparent-ca-dir ~/.gopherstack/ca
```

| Flag | Default | Meaning |
|---|---|---|
| `--transparent-dns` | off | Resolve `*.amazonaws.com`, `*.amazonaws.com.cn`, `*.api.aws` to `--dns-resolve-ip`; forward every other name upstream. Listens on `--dns-addr` (default `:53`). |
| `--transparent-https-addr` | `:443` | HTTPS listener for those names; certificates are issued per SNI from the CA. |
| `--transparent-http-addr` | off | Optional plain-HTTP listener (e.g. `:80`) so `http://s3.amazonaws.com` URLs work without TLS; a bind failure logs a warning and startup continues. |
| `--transparent-dns-skip` | none | Names/zones that still resolve upstream (e.g. `sts.amazonaws.com` for real credentials). |
| `--transparent-dns-upstream` | `/etc/resolv.conf` | Upstream resolvers (`host:port`). |
| `--transparent-ca-dir` | ephemeral | Persist the CA so you trust it once. |
| `--transparent-container-dns` | `--dns-resolve-ip` if not loopback | DNS IP given to spawned containers. |

Point the client at the DNS server (`resolvectl dns <iface> 127.0.0.1`, or
`/etc/resolv.conf`), then trust the CA, downloadable from
`http://localhost:<port>/_gopherstack/ca.pem` (or `<ca-dir>/ca.pem`):

```bash
export AWS_CA_BUNDLE=$PWD/ca.pem          # AWS CLI / SDKs
export REQUESTS_CA_BUNDLE=$PWD/ca.pem     # Python requests
export NODE_EXTRA_CA_CERTS=$PWD/ca.pem    # Node.js
keytool -importcert -noprompt -alias gopherstack -file ca.pem \
  -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit   # Java
```

Requests route by wire shape (target header, path, form action) exactly as with
`endpoint_url`; S3 virtual-hosted names such as `bucket.s3.us-west-2.amazonaws.com`
are recognised without extra configuration. The DNS answers every name under the
AWS zones and the per-SNI certificates carry a one-label wildcard on the parent
zone, so these S3 hostnames also work, dual-stack (`s3.dualstack.<region>`) and
FIPS (`s3-fips.<region>`, `s3-accesspoint-fips`, `s3-control-fips`) variants included:

| Hostname | Dispatch |
|----------|----------|
| `<name>-<account>.s3-accesspoint.<region>.amazonaws.com` | Access point resolved via S3 Control, request served as path-style on its bucket (`NoSuchAccessPoint` 404 if unknown). |
| `<bucket>.s3-website[-.]<region>.amazonaws.com` | GET served by the website handler (`/_gopherstack/website/<bucket>/...`) when website serving is registered. |
| `<account>.s3-control.<region>.amazonaws.com` | S3 Control (`/v20180820/...`), account taken from the host when `x-amz-account-id` is absent. |

The host-to-path mapping runs in front of the router on the transparent
listeners only (`pkgs/transparent` `S3HostRouter`).

Spawned engine containers (Lambda, CodeBuild, ECS, RDS, ...) get `--dns`
pointing at the embedded DNS, the CA mounted at `/etc/gopherstack/ca.pem`, and
`AWS_CA_BUNDLE`, `REQUESTS_CA_BUNDLE`, `NODE_EXTRA_CA_CERTS` and
`CURL_CA_BUNDLE` set. Containers cannot use a loopback DNS: set
`--dns-resolve-ip` (or `--transparent-container-dns`) to a host IP reachable from
the container network, and make the DNS server listen on port 53.
