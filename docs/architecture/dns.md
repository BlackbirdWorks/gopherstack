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
