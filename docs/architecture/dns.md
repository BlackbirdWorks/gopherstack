# DNS

An optional embedded DNS server (UDP and TCP, built on `miekg/dns`) gives AWS-style hostnames for emulated resources, and can optionally take over the real AWS endpoint names.

## Enable

```bash
gopherstack --dns-addr :10053     # or DNS_ADDR=:10053
```

An empty `--dns-addr` (default) disables the server.

| Flag | Env var | Default | Meaning |
|------|---------|---------|---------|
| `--dns-addr` | `DNS_ADDR` | empty (disabled) | Listen address |
| `--dns-resolve-ip` | `DNS_RESOLVE_IP` | `127.0.0.1` | IP that registered hostnames resolve to |

## Registered hostnames

Resources that have an endpoint register an AWS-style name, which resolves to `--dns-resolve-ip`. Names are removed when the resource is deleted.

| Service | Hostname |
|---------|----------|
| RDS | `<id>.<account>.<region>.rds.amazonaws.com` |
| Redshift | `<id>.<account>.<region>.redshift.amazonaws.com` |
| ElastiCache | `<id>.<suffix>.<region>.cache.amazonaws.com` (serverless: `<name>.serverless.<suffix>.<region>.cache.amazonaws.com`) |
| Elasticsearch | `search-<name>-<account>.<region>.es.amazonaws.com` |

OpenSearch, Lambda function URLs, EC2, Route 53 records, Cloud Map and Route 53 Resolver also register names. The exact name is on the resource (for example the `Endpoint` returned by `DescribeDBInstances`).

> [!NOTE]
> With the default `stub` engines no database process runs behind the name. Use the `docker` engine (see the Engine modes guide) for a real server, or point `--dns-resolve-ip` at another address.

## Resolving from your shell

Forward the zone to the embedded server. Replace `rds.amazonaws.com` with the zone of the service you use.

dnsmasq:

```
server=/rds.amazonaws.com/127.0.0.1#10053
```

systemd-resolved (`/etc/systemd/resolved.conf.d/gopherstack.conf`, then `sudo systemctl restart systemd-resolved`):

```ini
[Resolve]
DNS=127.0.0.1:10053
Domains=~rds.amazonaws.com
```

macOS (`/etc/resolver/rds.amazonaws.com`):

```
nameserver 127.0.0.1
port 10053
```

Test:

```bash
dig @127.0.0.1 -p 10053 my-db.000000000000.us-east-1.rds.amazonaws.com A
```

## Transparent endpoint injection

Lets unmodified SDKs and CLIs that target real `*.amazonaws.com` endpoints (no `endpoint_url`) reach Gopherstack.

```bash
sudo gopherstack --transparent-dns --transparent-ca-dir ~/.gopherstack/ca
```

| Flag | Env var | Default | Meaning |
|---|---|---|---|
| `--transparent-dns` | `TRANSPARENT_DNS` | off | Resolve `*.amazonaws.com`, `*.amazonaws.com.cn`, `*.api.aws` and `*.on.aws` (Lambda function URLs) to `--dns-resolve-ip`. Other names are forwarded upstream. Listens on `--dns-addr`, default `:53` |
| `--transparent-https-addr` | `TRANSPARENT_HTTPS_ADDR` | `:443` | HTTPS listener for those names. Certificates are issued per SNI from the CA |
| `--transparent-http-addr` | `TRANSPARENT_HTTP_ADDR` | off | Optional plain-HTTP listener (for example `:80`) so `http://s3.amazonaws.com` works |
| `--transparent-dns-skip` | `TRANSPARENT_DNS_SKIP` | none | Names or zones that still resolve upstream (for example `sts.amazonaws.com` for real credentials) |
| `--transparent-dns-skip-patterns` | `TRANSPARENT_DNS_SKIP_PATTERNS` | none | Regexes (whole name) that resolve upstream |
| `--transparent-dns-upstream` | `TRANSPARENT_DNS_UPSTREAM` | `/etc/resolv.conf` | Upstream resolvers, `host:port` |
| `--transparent-ca-dir` | `TRANSPARENT_CA_DIR` | ephemeral | Persist the CA so you trust it once |
| `--transparent-container-dns` | `TRANSPARENT_CONTAINER_DNS` | `--dns-resolve-ip`, if not loopback | DNS IP given to spawned containers |
| `--transparent-container-ca-path` | `TRANSPARENT_CONTAINER_CA_PATH` | | CA PEM path visible to the Docker daemon. Required when Gopherstack runs in Docker |
| `--transparent-disable-injection` | `TRANSPARENT_DISABLE_ENDPOINT_INJECTION` | off | Force injection off even with `--transparent-dns` |

Point the client at the DNS server (`resolvectl dns <iface> 127.0.0.1`, or `/etc/resolv.conf`), then trust the CA. Download it from `http://localhost:<port>/_gopherstack/ca.pem`, or read `<ca-dir>/ca.pem`.

```bash
export AWS_CA_BUNDLE=$PWD/ca.pem          # AWS CLI, SDKs
export REQUESTS_CA_BUNDLE=$PWD/ca.pem     # Python requests
export NODE_EXTRA_CA_CERTS=$PWD/ca.pem    # Node.js
keytool -importcert -noprompt -alias gopherstack -file ca.pem \
  -keystore $JAVA_HOME/lib/security/cacerts -storepass changeit   # Java
```

Requests route by wire shape (target header, path, form action), as with `endpoint_url`. S3 virtual-hosted names such as `bucket.s3.us-west-2.amazonaws.com` work without extra configuration, including dual-stack (`s3.dualstack.<region>`) and FIPS (`s3-fips.<region>`, `s3-accesspoint-fips`, `s3-control-fips`) variants. Special S3 hostnames are mapped on the transparent listeners only (`pkgs/transparent` `S3HostRouter`):

| Hostname | Dispatch |
|----------|----------|
| `<name>-<account>.s3-accesspoint.<region>.amazonaws.com` | Access point resolved via S3 Control and served path-style on its bucket. Unknown: `NoSuchAccessPoint` 404 |
| `<bucket>.s3-website[-.]<region>.amazonaws.com` | GET served by the website handler (`/_gopherstack/website/<bucket>/...`) when website serving is registered |
| `<account>.s3-control.<region>.amazonaws.com` | S3 Control (`/v20180820/...`). Account comes from the host if `x-amz-account-id` is absent |

Spawned engine containers (Lambda, CodeBuild, ECS, RDS, ...) get `--dns` pointing at the embedded DNS, the CA at `/etc/gopherstack/ca.pem`, and `AWS_CA_BUNDLE`, `REQUESTS_CA_BUNDLE`, `NODE_EXTRA_CA_CERTS` and `CURL_CA_BUNDLE` set.

> [!IMPORTANT]
> Containers cannot use a loopback DNS. Set `--dns-resolve-ip` (or `--transparent-container-dns`) to a host IP reachable from the container network, and make the DNS server listen on port 53.
