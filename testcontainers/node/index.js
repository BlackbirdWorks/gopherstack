import { GenericContainer, Wait } from "testcontainers";

export const DEFAULT_IMAGE = "ghcr.io/blackbirdworks/gopherstack:latest";
export const HEALTH_PATH = "/_gopherstack/health";
export const API_PORT = 8000;
export const DEFAULT_REGION = "us-east-1";
export const AZURE_ACCOUNT_NAME = "devstoreaccount1";
export const AZURE_ACCOUNT_KEY =
  "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

export const AZURE_PORTS = {
  blob: 10000, queue: 10001, table: 10002, servicebus: 10003, keyvault: 10004,
  eventhubs: 10005, arm: 10006, eventgrid: 10007, appconfig: 10008, file: 10009, monitor: 10013, graph: 10014, acr: 10011, storagevhost: 10010, cosmosdb: 8081,
};

export const OCI_PORTS = { objectstorage: 8150, vault: 8151, streaming: 8152, identity: 8153, functions: 8154, core: 8155, queue: 8156, notifications: 8157, containerengine: 8158, mysql: 8159, logging: 8160, monitoring: 8161, nosql: 8162, apigateway: 8163, events: 8164, loadbalancer: 8165, artifacts: 8166 };

export const GCP_PORTS = {
  storage: 4443, firestore: 8080, pubsub: 8085, secretmanager: 8086, kms: 8087,
  cloudtasks: 8123, scheduler: 8124, iam: 8125, run: 8126, functions: 8128,
  sql: 8130, logging: 8131, monitoring: 8132, redis: 8133, artifactregistry: 8134, spanner: 8135, cloudbuild: 8136, bigtable: 8137, eventarc: 8138, workflows: 8139, dns: 8140, dnsserver: 8141, compute: 8142, datastore: 8143, container: 8144, identitytoolkit: 8145, apigateway: 8146, batch: 8147, resourcemanager: 8148, bigquery: 9050,
};

const DATA_DIR = "/data";
const DOCKER_SOCK = "/var/run/docker.sock";

/** Builder for a Gopherstack container. */
export class GopherstackContainer {
  #image;
  #services = [];
  #env = {};
  #binds = [];

  constructor(image = DEFAULT_IMAGE) {
    this.#image = image;
  }

  withServices(...names) {
    this.#services.push(...names);
    return this;
  }

  withPersistence(hostDir) {
    this.#env.PERSIST = "true";
    this.#env.GOPHERSTACK_DATA_DIR = DATA_DIR;
    this.#binds.push({ source: hostDir, target: DATA_DIR, mode: "rw" });
    return this;
  }

  /** Mounts the host Docker socket; grants control of the host daemon. */
  withDockerSocket() {
    this.#binds.push({ source: DOCKER_SOCK, target: DOCKER_SOCK, mode: "rw" });
    return this;
  }

  withEnv(key, value) {
    this.#env[key] = value;
    return this;
  }

  async start() {
    const env = { ...this.#env };
    if (this.#services.length > 0) env.GOPHERSTACK_SERVICES = this.#services.join(",");
    const ports = [API_PORT, ...Object.values(AZURE_PORTS), ...Object.values(GCP_PORTS), ...Object.values(OCI_PORTS)];
    const started = await new GenericContainer(this.#image)
      .withExposedPorts(...ports)
      .withEnvironment(env)
      .withBindMounts(this.#binds)
      .withWaitStrategy(Wait.forHttp(HEALTH_PATH, API_PORT))
      .withStartupTimeout(120_000)
      .start();
    return new StartedGopherstackContainer(started);
  }
}

export class StartedGopherstackContainer {
  constructor(container) {
    this.container = container;
  }

  #hostPort(port) {
    return `${this.container.getHost()}:${this.container.getMappedPort(port)}`;
  }

  /** AWS API base URL, e.g. http://localhost:32768. */
  getEndpoint() {
    return `http://${this.#hostPort(API_PORT)}`;
  }

  /** Client config for AWS SDK v3 clients: `new S3Client(c.getAwsClientConfig())`. */
  getAwsClientConfig() {
    return {
      endpoint: this.getEndpoint(),
      region: DEFAULT_REGION,
      credentials: { accessKeyId: "test", secretAccessKey: "test" },
      forcePathStyle: true,
    };
  }

  getAzureEndpoints() {
    return Object.fromEntries(
      Object.entries(AZURE_PORTS).map(([name, p]) => [name, `http://${this.#hostPort(p)}`]),
    );
  }

  getAzureConnectionString() {
    const eps = this.getAzureEndpoints();
    const acct = `/${AZURE_ACCOUNT_NAME}`;
    return (
      `DefaultEndpointsProtocol=http;AccountName=${AZURE_ACCOUNT_NAME};AccountKey=${AZURE_ACCOUNT_KEY};` +
      `BlobEndpoint=${eps.blob}${acct};QueueEndpoint=${eps.queue}${acct};TableEndpoint=${eps.table}${acct};FileEndpoint=${eps.file}${acct};`
    );
  }

  /** http URL of an OCI service (objectstorage, vault, streaming, identity, functions, core, queue, notifications), usable as the OCI SDK client host. */
  getOciEndpoint(service) {
    const port = OCI_PORTS[String(service).toLowerCase()];
    if (port === undefined) throw new Error(`unknown oci service "${service}"`);
    return `http://${this.#hostPort(port)}`;
  }

  /** host:port of a GCP service; storage is the STORAGE_EMULATOR_HOST value. */
  getGcpEndpoint(service) {
    const port = GCP_PORTS[String(service).toLowerCase()];
    if (port === undefined) throw new Error(`unknown gcp service "${service}"`);
    return this.#hostPort(port);
  }

  stop() {
    return this.container.stop();
  }
}
