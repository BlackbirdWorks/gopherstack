import type { DashboardCategory, DashboardRoute } from "./nav";

export type Cloud = "aws" | "azure" | "gcp";

export const CLOUDS: readonly { id: Cloud; label: string }[] = [
  { id: "aws", label: "AWS" },
  { id: "azure", label: "Azure" },
  { id: "gcp", label: "GCP" },
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
];

export function parseCloud(value: string | null | undefined): Cloud | null {
  const v = value?.trim().toLowerCase();
  return CLOUDS.find((c) => c.id === v)?.id ?? null;
}

/** The cloud a dashboard path belongs to (/dashboard/azure/..., /dashboard/gcp/...), else null. */
export function cloudFromPath(pathname: string): Cloud | null {
  const m = /^\/dashboard\/(azure|gcp)(\/|$)/.exec(pathname);
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
): DashboardCategory[] {
  if (cloud === "aws") return awsCategories;
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
