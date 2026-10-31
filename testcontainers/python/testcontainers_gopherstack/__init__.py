"""testcontainers module that runs the Gopherstack multi-cloud emulator."""

from __future__ import annotations

import time
import urllib.parse
import urllib.request

from testcontainers.core.container import DockerContainer

DEFAULT_IMAGE = "ghcr.io/blackbirdworks/gopherstack:latest"
HEALTH_PATH = "/_gopherstack/health"
API_PORT = 8000
DEFAULT_REGION = "us-east-1"

AZURE_ACCOUNT_NAME = "devstoreaccount1"
AZURE_ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

AZURE_PORTS = {
    "blob": 10000,
    "queue": 10001,
    "table": 10002,
    "servicebus": 10003,
    "keyvault": 10004,
    "eventhubs": 10005,
    "arm": 10006,
    "eventgrid": 10007,
    "appconfig": 10008,
    "file": 10009,
    "monitor": 10013,
    "graph": 10014,
    "acr": 10011,
    "storagevhost": 10010,
    "cosmosdb": 8081,
}

OCI_PORTS = {"objectstorage": 8150, "vault": 8151, "streaming": 8152, "identity": 8153, "functions": 8154, "core": 8155, "queue": 8156, "notifications": 8157, "containerengine": 8158, "mysql": 8159, "logging": 8160, "monitoring": 8161, "nosql": 8162, "apigateway": 8163, "events": 8164, "loadbalancer": 8165, "artifacts": 8166, "containerinstances": 8167}

GCP_PORTS = {
    "storage": 4443,
    "firestore": 8080,
    "pubsub": 8085,
    "secretmanager": 8086,
    "kms": 8087,
    "cloudtasks": 8123,
    "scheduler": 8124,
    "iam": 8125,
    "run": 8126,
    "functions": 8128,
    "sql": 8130,
    "logging": 8131,
    "monitoring": 8132,
    "redis": 8133,
    "artifactregistry": 8134,
    "spanner": 8135,
    "cloudbuild": 8136,
    "bigtable": 8137,
    "eventarc": 8138,
    "workflows": 8139,
    "dns": 8140,
    "dnsserver": 8141,
    "compute": 8142,
    "datastore": 8143,
    "container": 8144,
    "identitytoolkit": 8145,
    "apigateway": 8146,
    "batch": 8147,
    "resourcemanager": 8148,
    "bigquery": 9050,
}

DATA_DIR = "/data"
DOCKER_SOCK = "/var/run/docker.sock"


class GopherstackContainer(DockerContainer):
    """Gopherstack emulator container (AWS, Azure and GCP APIs)."""

    def __init__(self, image: str = DEFAULT_IMAGE, startup_timeout: float = 120.0, **kwargs):
        super().__init__(image, **kwargs)
        self._startup_timeout = startup_timeout
        self._services: list[str] = []
        for port in [API_PORT, *AZURE_PORTS.values(), *GCP_PORTS.values(), *OCI_PORTS.values()]:
            self.with_exposed_ports(port)

    def with_services(self, *names: str) -> GopherstackContainer:
        """Restrict the emulator to the named services (GOPHERSTACK_SERVICES)."""
        self._services.extend(names)
        self.with_env("GOPHERSTACK_SERVICES", ",".join(self._services))
        return self

    def with_persistence(self, host_dir: str) -> GopherstackContainer:
        """Enable snapshot persistence, bind-mounting host_dir at /data."""
        self.with_env("PERSIST", "true")
        self.with_env("GOPHERSTACK_DATA_DIR", DATA_DIR)
        self.with_volume_mapping(host_dir, DATA_DIR, "rw")
        return self

    def with_docker_socket(self) -> GopherstackContainer:
        """Mount the host Docker socket (Lambda/RDS/ECS docker engines).

        This gives the emulator control of the host Docker daemon; use only in
        trusted test environments.
        """
        self.with_volume_mapping(DOCKER_SOCK, DOCKER_SOCK, "rw")
        return self

    def start(self) -> GopherstackContainer:
        super().start()
        self._wait_healthy()
        return self

    def _wait_healthy(self) -> None:
        url = self.get_endpoint() + HEALTH_PATH
        if urllib.parse.urlparse(url).scheme not in ("http", "https"):
            raise ValueError(f"unexpected health URL scheme: {url}")
        deadline = time.monotonic() + self._startup_timeout
        last: Exception | None = None
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=2) as resp:  # noqa: S310  # nosec B310
                    if resp.status == 200:
                        return
            except Exception as exc:  # noqa: BLE001
                last = exc
            time.sleep(0.2)
        raise TimeoutError(f"gopherstack not healthy at {url}: {last}")

    def _host_port(self, port: int) -> str:
        return f"{self.get_container_host_ip()}:{self.get_exposed_port(port)}"

    def get_endpoint(self) -> str:
        """AWS API base URL, e.g. http://localhost:32768."""
        return "http://" + self._host_port(API_PORT)

    def get_aws_client(self, service: str, **kwargs):
        """boto3 client wired to the container with static test credentials."""
        import boto3  # local import: boto3 is an optional dependency

        return boto3.client(
            service,
            endpoint_url=self.get_endpoint(),
            region_name=DEFAULT_REGION,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            **kwargs,
        )

    def get_azure_endpoints(self) -> dict[str, str]:
        """Base URL of every Azure listener, keyed by short service name."""
        return {name: "http://" + self._host_port(p) for name, p in AZURE_PORTS.items()}

    def get_azure_connection_string(self) -> str:
        """Azurite-style Azure Storage connection string (devstoreaccount1)."""
        eps = self.get_azure_endpoints()
        acct = "/" + AZURE_ACCOUNT_NAME
        return (
            f"DefaultEndpointsProtocol=http;AccountName={AZURE_ACCOUNT_NAME};"
            f"AccountKey={AZURE_ACCOUNT_KEY};BlobEndpoint={eps['blob']}{acct};"
            f"QueueEndpoint={eps['queue']}{acct};TableEndpoint={eps['table']}{acct};"
            f"FileEndpoint={eps['file']}{acct};"
        )

    def get_oci_endpoint(self, service: str) -> str:
        """http URL of an OCI service (objectstorage, vault, streaming, identity, functions, core, queue, notifications), usable as the OCI SDK client host."""
        try:
            port = OCI_PORTS[service.lower()]
        except KeyError:
            raise ValueError(f"unknown oci service {service!r}") from None
        return "http://" + self._host_port(port)

    def get_gcp_endpoint(self, service: str) -> str:
        """host:port of a GCP service (storage is the STORAGE_EMULATOR_HOST value)."""
        try:
            port = GCP_PORTS[service.lower()]
        except KeyError:
            raise ValueError(f"unknown gcp service {service!r}") from None
        return self._host_port(port)
