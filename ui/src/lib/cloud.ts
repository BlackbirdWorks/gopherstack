import type { DashboardCategory, DashboardRoute } from "./nav";

export type Cloud = "aws" | "azure" | "gcp" | "oci";

export const CLOUDS: readonly { id: Cloud; label: string; icon?: string }[] = [
  { id: "aws", label: "AWS" },
  { id: "azure", label: "Azure", icon: "azure" },
  { id: "gcp", label: "GCP", icon: "gcp" },
  { id: "oci", label: "Oracle Cloud", icon: "oci" },
];

export const DEFAULT_CLOUD: Cloud = "aws";
export const CLOUD_PARAM = "cloud";
export const CLOUD_STORAGE_KEY = "gopherstack_cloud";

export type AzureService = {
  id: string;
  label: string;
  description: string;
  /** Registerable Name() reported by /_gopherstack/health. */
  healthName: string;
  /** Label the server reserves the fixed port under (/dashboard/api/system/state). */
  portLabel: string;
  defaultPort: number;
};

export const AZURE_SERVICES: readonly AzureService[] = [
  {
    id: "blob",
    label: "Blob Storage",
    description: "Azure Blob Storage",
    healthName: "AzureBlob",
    portLabel: "azureblob",
    defaultPort: 10000,
  },
  {
    id: "queue",
    label: "Queue Storage",
    description: "Azure Queue Storage",
    healthName: "AzureQueue",
    portLabel: "azurequeue",
    defaultPort: 10001,
  },
  {
    id: "table",
    label: "Table Storage",
    description: "Azure Table Storage",
    healthName: "AzureTable",
    portLabel: "azuretable",
    defaultPort: 10002,
  },
  {
    id: "servicebus",
    label: "Service Bus",
    description: "Azure Service Bus",
    healthName: "AzureServiceBus",
    portLabel: "azureservicebus",
    defaultPort: 10003,
  },
  {
    id: "keyvault",
    label: "Key Vault",
    description: "Azure Key Vault",
    healthName: "AzureKeyVault",
    portLabel: "azurekeyvault",
    defaultPort: 10004,
  },
  {
    id: "eventhubs",
    label: "Event Hubs",
    description: "Azure Event Hubs",
    healthName: "AzureEventHubs",
    portLabel: "azureeventhubs",
    defaultPort: 10005,
  },
  {
    id: "eventgrid",
    label: "Event Grid",
    description: "Azure Event Grid",
    healthName: "AzureEventGrid",
    portLabel: "azureeventgrid",
    defaultPort: 10007,
  },
  {
    id: "appconfig",
    label: "App Configuration",
    description: "Azure App Configuration",
    healthName: "AzureAppConfig",
    portLabel: "azureappconfig",
    defaultPort: 10008,
  },
  {
    id: "arm",
    label: "Resource Manager",
    description: "Azure Resource Manager (ARM)",
    healthName: "AzureARM",
    portLabel: "azurearm",
    defaultPort: 10006,
  },
  {
    id: "cosmosdb",
    label: "Cosmos DB",
    description: "Azure Cosmos DB",
    healthName: "CosmosDB",
    portLabel: "cosmosdb",
    defaultPort: 8081,
  },
  {
    id: "functions",
    label: "Functions",
    description: "Azure Functions",
    healthName: "AzureFunctions",
    portLabel: "azurefunctions",
    defaultPort: 10012,
  },
  {
    id: "file",
    label: "Files",
    description: "Azure Files",
    healthName: "AzureFile",
    portLabel: "azurefile",
    defaultPort: 10009,
  },
  {
    id: "monitor",
    label: "Monitor Logs",
    description: "Azure Monitor Logs",
    healthName: "AzureMonitor",
    portLabel: "azuremonitor",
    defaultPort: 10013,
  },
  {
    id: "aci",
    label: "Container Instances",
    description: "Azure Container Instances (served by Resource Manager)",
    healthName: "AzureARM",
    portLabel: "azurearm",
    defaultPort: 10006,
  },
  {
    id: "appservice",
    label: "App Service",
    description: "Azure App Service web apps and slots (served by Resource Manager)",
    healthName: "AzureARM",
    portLabel: "azurearm",
    defaultPort: 10006,
  },
  {
    id: "containerapps",
    label: "Container Apps",
    description:
      "Azure Container Apps environments, apps, revisions and jobs (served by Resource Manager)",
    healthName: "AzureARM",
    portLabel: "azurearm",
    defaultPort: 10006,
  },
  {
    id: "vm",
    label: "Virtual Machines",
    description: "Azure virtual machines, disks and networking (served by Resource Manager)",
    healthName: "AzureARM",
    portLabel: "azurearm",
    defaultPort: 10006,
  },
  {
    id: "aks",
    label: "Kubernetes Service",
    description: "Azure Kubernetes Service clusters and agent pools (served by Resource Manager)",
    healthName: "AzureARM",
    portLabel: "azurearm",
    defaultPort: 10006,
  },
  {
    id: "flexdb",
    label: "Flexible Server",
    description:
      "Azure Database for PostgreSQL and MySQL flexible servers (served by Resource Manager)",
    healthName: "AzureARM",
    portLabel: "azurearm",
    defaultPort: 10006,
  },
  {
    id: "apim",
    label: "API Management",
    description:
      "Azure API Management services, APIs, products and subscriptions (served by Resource Manager)",
    healthName: "AzureARM",
    portLabel: "azurearm",
    defaultPort: 10006,
  },
];

export type GcpService = {
  id: string;
  label: string;
  description: string;
  /** Registerable Name() reported by /_gopherstack/health. */
  healthName: string;
  /** Label the server reserves the fixed port under (/dashboard/api/system/state). */
  portLabel: string;
  defaultPort: number;
};

export const GCP_SERVICES: readonly GcpService[] = [
  {
    id: "storage",
    label: "Cloud Storage",
    description: "Google Cloud Storage",
    healthName: "GCPStorage",
    portLabel: "gcpstorage",
    defaultPort: 4443,
  },
  {
    id: "pubsub",
    label: "Pub/Sub",
    description: "Google Cloud Pub/Sub",
    healthName: "GCPPubSub",
    portLabel: "gcppubsub",
    defaultPort: 8085,
  },
  {
    id: "secretmanager",
    label: "Secret Manager",
    description: "Google Secret Manager",
    healthName: "GCPSecretManager",
    portLabel: "gcpsecretmanager",
    defaultPort: 8086,
  },
  {
    id: "kms",
    label: "Cloud KMS",
    description: "Google Cloud KMS",
    healthName: "GCPKMS",
    portLabel: "gcpkms",
    defaultPort: 8087,
  },
  {
    id: "cloudtasks",
    label: "Cloud Tasks",
    description: "Google Cloud Tasks",
    healthName: "GCPCloudTasks",
    portLabel: "gcpcloudtasks",
    defaultPort: 8123,
  },
  {
    id: "scheduler",
    label: "Cloud Scheduler",
    description: "Google Cloud Scheduler",
    healthName: "GCPScheduler",
    portLabel: "gcpscheduler",
    defaultPort: 8124,
  },
  {
    id: "run",
    label: "Cloud Run",
    description: "Google Cloud Run",
    healthName: "GCPRun",
    portLabel: "gcprun",
    defaultPort: 8126,
  },
  {
    id: "functions",
    label: "Cloud Functions",
    description: "Google Cloud Functions",
    healthName: "GCPFunctions",
    portLabel: "gcpfunctions",
    defaultPort: 8128,
  },
  {
    id: "bigquery",
    label: "BigQuery",
    description: "Google BigQuery",
    healthName: "GCPBigQuery",
    portLabel: "gcpbigquery",
    defaultPort: 9050,
  },
  {
    id: "sql",
    label: "Cloud SQL",
    description: "Google Cloud SQL",
    healthName: "GCPSQL",
    portLabel: "gcpsql",
    defaultPort: 8130,
  },
  {
    id: "logging",
    label: "Cloud Logging",
    description: "Google Cloud Logging",
    healthName: "GCPLogging",
    portLabel: "gcplogging",
    defaultPort: 8131,
  },
  {
    id: "iam",
    label: "Cloud IAM",
    description: "Google Cloud IAM",
    healthName: "GCPIAM",
    portLabel: "gcpiam",
    defaultPort: 8125,
  },
  {
    id: "firestore",
    label: "Firestore",
    description: "Google Cloud Firestore",
    healthName: "GCPFirestore",
    portLabel: "gcpfirestore",
    defaultPort: 8080,
  },
  {
    id: "bigtable",
    label: "Bigtable",
    description: "Google Cloud Bigtable",
    healthName: "GCPBigtable",
    portLabel: "gcpbigtable",
    defaultPort: 8137,
  },
  {
    id: "eventarc",
    label: "Eventarc",
    description: "Google Cloud Eventarc",
    healthName: "GCPEventarc",
    portLabel: "gcpeventarc",
    defaultPort: 8138,
  },
  {
    id: "spanner",
    label: "Cloud Spanner",
    description: "Google Cloud Spanner",
    healthName: "GCPSpanner",
    portLabel: "gcpspanner",
    defaultPort: 8135,
  },
  {
    id: "artifactregistry",
    label: "Artifact Registry",
    description: "Google Artifact Registry",
    healthName: "GCPArtifactRegistry",
    portLabel: "gcpartifactregistry",
    defaultPort: 8134,
  },
  {
    id: "cloudbuild",
    label: "Cloud Build",
    description: "Google Cloud Build",
    healthName: "GCPCloudBuild",
    portLabel: "gcpcloudbuild",
    defaultPort: 8136,
  },
  {
    id: "redis",
    label: "Memorystore Redis",
    description: "Google Memorystore for Redis",
    healthName: "GCPRedis",
    portLabel: "gcpredis",
    defaultPort: 8133,
  },
  {
    id: "monitoring",
    label: "Cloud Monitoring",
    description: "Google Cloud Monitoring",
    healthName: "GCPMonitoring",
    portLabel: "gcpmonitoring",
    defaultPort: 8132,
  },
  {
    id: "workflows",
    label: "Workflows",
    description: "Google Cloud Workflows",
    healthName: "GCPWorkflows",
    portLabel: "gcpworkflows",
    defaultPort: 8139,
  },
  {
    id: "dns",
    label: "Cloud DNS",
    description: "Google Cloud DNS",
    healthName: "GCPDNS",
    portLabel: "gcpdns",
    defaultPort: 8140,
  },
  {
    id: "compute",
    label: "Compute Engine",
    description: "Google Compute Engine",
    healthName: "GCPCompute",
    portLabel: "gcpcompute",
    defaultPort: 8142,
  },
  {
    id: "datastore",
    label: "Datastore",
    description: "Google Cloud Datastore",
    healthName: "GCPDatastore",
    portLabel: "gcpdatastore",
    defaultPort: 8143,
  },
  {
    id: "container",
    label: "Kubernetes Engine",
    description: "Google Kubernetes Engine clusters and node pools",
    healthName: "GCPContainer",
    portLabel: "gcpcontainer",
    defaultPort: 8144,
  },
  {
    id: "identitytoolkit",
    label: "Identity Platform",
    description: "Google Cloud Identity Platform users, tenants and sign-in providers",
    healthName: "GCPIdentityToolkit",
    portLabel: "gcpidentitytoolkit",
    defaultPort: 8145,
  },
  {
    id: "apigateway",
    label: "API Gateway",
    description: "Google Cloud API Gateway APIs, configs and gateways",
    healthName: "GCPAPIGateway",
    portLabel: "gcpapigateway",
    defaultPort: 8146,
  },
  {
    id: "batch",
    label: "Cloud Batch",
    description: "Google Cloud Batch jobs, task groups and tasks",
    healthName: "GCPBatch",
    portLabel: "gcpbatch",
    defaultPort: 8147,
  },
  {
    id: "resourcemanager",
    label: "Resource Manager",
    description: "Google Cloud organization, folders, projects, IAM policies and Service Usage",
    healthName: "GCPResourceManager",
    portLabel: "gcpresourcemanager",
    defaultPort: 8148,
  },
];

export type OciService = {
  id: string;
  label: string;
  description: string;
  /** Registerable Name() reported by /_gopherstack/health. */
  healthName: string;
  /** Label the server reserves the fixed port under (/dashboard/api/system/state). */
  portLabel: string;
  defaultPort: number;
};

export const OCI_SERVICES: readonly OciService[] = [
  {
    id: "objectstorage",
    label: "Object Storage",
    description: "Oracle Cloud Object Storage",
    healthName: "OCIObjectStorage",
    portLabel: "ociobjectstorage",
    defaultPort: 8150,
  },
  {
    id: "vault",
    label: "Vault",
    description: "Oracle Cloud Vault: keys and secrets",
    healthName: "OCIVault",
    portLabel: "ocivault",
    defaultPort: 8151,
  },
  {
    id: "streaming",
    label: "Streaming",
    description: "Oracle Cloud Streaming",
    healthName: "OCIStreaming",
    portLabel: "ocistreaming",
    defaultPort: 8152,
  },
  {
    id: "identity",
    label: "Identity",
    description: "Oracle Cloud Identity and Access Management",
    healthName: "OCIIdentity",
    portLabel: "ociidentity",
    defaultPort: 8153,
  },
  {
    id: "functions",
    label: "Functions",
    description: "Oracle Cloud Functions",
    healthName: "OCIFunctions",
    portLabel: "ocifunctions",
    defaultPort: 8154,
  },
  {
    id: "core",
    label: "Core Services",
    description: "Oracle Cloud networking, compute and block storage",
    healthName: "OCICore",
    portLabel: "ocicore",
    defaultPort: 8155,
  },
  {
    id: "queue",
    label: "Queue",
    description: "Oracle Cloud Queue",
    healthName: "OCIQueue",
    portLabel: "ociqueue",
    defaultPort: 8156,
  },
  {
    id: "notifications",
    label: "Notifications",
    description: "Oracle Cloud Notifications topics and subscriptions",
    healthName: "OCINotifications",
    portLabel: "ocinotifications",
    defaultPort: 8157,
  },
  {
    id: "containerengine",
    label: "Container Engine",
    description: "Oracle Container Engine for Kubernetes clusters and node pools",
    healthName: "OCIContainerEngine",
    portLabel: "ocicontainerengine",
    defaultPort: 8158,
  },
  {
    id: "mysql",
    label: "MySQL HeatWave",
    description: "Oracle Cloud MySQL HeatWave DB systems, backups and configurations",
    healthName: "OCIMySQL",
    portLabel: "ocimysql",
    defaultPort: 8159,
  },
  {
    id: "logging",
    label: "Logging",
    description: "Oracle Cloud Logging log groups, logs and search",
    healthName: "OCILogging",
    portLabel: "ocilogging",
    defaultPort: 8160,
  },
  {
    id: "monitoring",
    label: "Monitoring",
    description: "Oracle Cloud Monitoring metrics, queries and alarms",
    healthName: "OCIMonitoring",
    portLabel: "ocimonitoring",
    defaultPort: 8161,
  },
  {
    id: "nosql",
    label: "NoSQL Database",
    description: "Oracle NoSQL Database Cloud tables, indexes, rows and SQL queries",
    healthName: "OCINoSQL",
    portLabel: "ocinosql",
    defaultPort: 8162,
  },
  {
    id: "apigateway",
    label: "API Gateway",
    description: "Oracle Cloud API Gateway gateways, deployments and certificates",
    healthName: "OCIAPIGateway",
    portLabel: "ociapigateway",
    defaultPort: 8163,
  },
  {
    id: "events",
    label: "Events",
    description: "Oracle Cloud Events rules, actions and the delivery log",
    healthName: "OCIEvents",
    portLabel: "ocievents",
    defaultPort: 8164,
  },
  {
    id: "loadbalancer",
    label: "Load Balancer",
    description: "Oracle Cloud Load Balancer listeners, backend sets and backend health",
    healthName: "OCILoadBalancer",
    portLabel: "ociloadbalancer",
    defaultPort: 8165,
  },
  {
    id: "artifacts",
    label: "Artifacts",
    description: "Oracle Cloud container registry repositories, images and generic artifacts",
    healthName: "OCIArtifacts",
    portLabel: "ociartifacts",
    defaultPort: 8166,
  },
  {
    id: "containerinstances",
    label: "Container Instances",
    description: "Oracle Cloud Container Instances, their containers, logs and work requests",
    healthName: "OCIContainerInstances",
    portLabel: "ocicontainerinstances",
    defaultPort: 8167,
  },
];

export function parseCloud(value: string | null | undefined): Cloud | null {
  const v = value?.trim().toLowerCase();
  return CLOUDS.find((c) => c.id === v)?.id ?? null;
}

/** The cloud a dashboard path belongs to (/dashboard/azure/..., /dashboard/gcp/..., /dashboard/oci/...), else null. */
export function cloudFromPath(pathname: string): Cloud | null {
  const m = /^\/dashboard\/(azure|gcp|oci)(\/|$)/.exec(pathname);
  return m ? parseCloud(m[1]) : null;
}

/** URL param wins over the stored choice; AWS is the default. */
export function resolveCloud(urlValue: string | null, storedValue: string | null): Cloud {
  return parseCloud(urlValue) ?? parseCloud(storedValue) ?? DEFAULT_CLOUD;
}

export function readStoredCloud(storage?: Pick<Storage, "getItem">): string | null {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    return s?.getItem(CLOUD_STORAGE_KEY) ?? null;
  } catch {
    return null;
  }
}

export function writeStoredCloud(cloud: Cloud, storage?: Pick<Storage, "setItem">): void {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    s?.setItem(CLOUD_STORAGE_KEY, cloud);
  } catch {
    // storage unavailable (private mode / blocked): selection lives for this session only
  }
}

/** Azure services the backend actually serves, in catalog order. */
export function availableAzureServices(healthNames: readonly string[]): AzureService[] {
  const served = new Set(healthNames.map((n) => n.toLowerCase()));
  return AZURE_SERVICES.filter((s) => served.has(s.healthName.toLowerCase()));
}

export function azureServiceById(id: string): AzureService | undefined {
  return AZURE_SERVICES.find((s) => s.id === id);
}

/** GCP services the backend actually serves, in catalog order. */
export function availableGcpServices(healthNames: readonly string[]): GcpService[] {
  const served = new Set(healthNames.map((n) => n.toLowerCase()));
  return GCP_SERVICES.filter((s) => served.has(s.healthName.toLowerCase()));
}

export function gcpServiceById(id: string): GcpService | undefined {
  return GCP_SERVICES.find((s) => s.id === id);
}

/** OCI services the backend actually serves, in catalog order. */
export function availableOciServices(healthNames: readonly string[]): OciService[] {
  const served = new Set(healthNames.map((n) => n.toLowerCase()));
  return OCI_SERVICES.filter((s) => served.has(s.healthName.toLowerCase()));
}

export function ociServiceById(id: string): OciService | undefined {
  return OCI_SERVICES.find((s) => s.id === id);
}

export function ociRoute(service: OciService): DashboardRoute {
  return {
    id: `oci-${service.id}`,
    href: `/dashboard/oci/${service.id}`,
    label: service.label,
    icon: `oci-${service.id}`,
  };
}

export function gcpRoute(service: GcpService): DashboardRoute {
  return {
    id: `gcp-${service.id}`,
    href: `/dashboard/gcp/${service.id}`,
    label: service.label,
    icon: `gcp-${service.id}`,
  };
}

export function azureRoute(service: AzureService): DashboardRoute {
  return {
    id: `azure-${service.id}`,
    href: `/dashboard/azure/${service.id}`,
    label: service.label,
    icon: `azure-${service.id}`,
  };
}

/** Sidebar categories for the selected cloud. AWS passes through unchanged. */
export function categoriesForCloud(
  cloud: Cloud,
  awsCategories: DashboardCategory[],
  azureServices: readonly AzureService[],
  gcpServices: readonly GcpService[] = [],
  ociServices: readonly OciService[] = [],
): DashboardCategory[] {
  if (cloud === "aws") return awsCategories;
  if (cloud === "oci") {
    if (ociServices.length === 0) return [];
    return [
      {
        id: "oci-services",
        label: "Oracle Cloud Services",
        routes: ociServices.map((svc) => ociRoute(svc)),
      },
    ];
  }
  if (cloud === "gcp") {
    if (gcpServices.length === 0) return [];
    return [
      {
        id: "gcp-services",
        label: "GCP Services",
        routes: gcpServices.map((svc) => gcpRoute(svc)),
      },
    ];
  }
  if (azureServices.length === 0) return [];
  return [
    {
      id: "azure-services",
      label: "Azure Services",
      routes: azureServices.map((svc) => azureRoute(svc)),
    },
  ];
}

/** Path + search for `url` with the cloud param set (removed for the default cloud). */
export function withCloud(url: URL, cloud: Cloud): string {
  const next = new URL(url);
  if (cloud === DEFAULT_CLOUD) next.searchParams.delete(CLOUD_PARAM);
  else next.searchParams.set(CLOUD_PARAM, cloud);
  return `${next.pathname}${next.search}${next.hash}`;
}
