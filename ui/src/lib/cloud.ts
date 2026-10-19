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
    id: "firestore",
    label: "Firestore",
    description: "Google Cloud Firestore",
    healthName: "GCPFirestore",
    portLabel: "gcpfirestore",
    defaultPort: 8080,
  },
];

export function parseCloud(value: string | null | undefined): Cloud | null {
  const v = value?.trim().toLowerCase();
  return CLOUDS.find((c) => c.id === v)?.id ?? null;
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
