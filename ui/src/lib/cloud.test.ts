import { describe, expect, it } from "vitest";

import {
  AZURE_SERVICES,
  cloudFromPath,
  CLOUD_STORAGE_KEY,
  availableAzureServices,
  availableGcpServices,
  availableOciServices,
  azureServiceById,
  gcpServiceById,
  ociServiceById,
  categoriesForCloud,
  CLOUDS,
  GCP_SERVICES,
  OCI_SERVICES,
  ociRoute,
  parseCloud,
  readStoredCloud,
  resolveCloud,
  withCloud,
  writeStoredCloud,
} from "./cloud";
import { sidebarCategories } from "./nav";

describe("parseCloud", () => {
  it.each([
    ["aws", "aws"],
    ["Azure", "azure"],
    [" GCP ", "gcp"],
    ["oci", "oci"],
    [" Oci ", "oci"],
    ["oracle", null],
    ["", null],
    [null, null],
    [undefined, null],
  ])("parses %j", (input, want) => {
    expect(parseCloud(input)).toBe(want);
  });
});

describe("resolveCloud", () => {
  it.each([
    [null, null, "aws"],
    ["azure", null, "azure"],
    [null, "gcp", "gcp"],
    ["azure", "gcp", "azure"],
    ["bogus", "gcp", "gcp"],
    ["bogus", "nope", "aws"],
  ])("url=%j stored=%j", (url, stored, want) => {
    expect(resolveCloud(url, stored)).toBe(want);
  });
});

describe("storage", () => {
  it("round-trips through a storage object", () => {
    const store = new Map<string, string>();
    const storage = {
      getItem: (k: string) => store.get(k) ?? null,
      setItem: (k: string, v: string) => void store.set(k, v),
    };
    writeStoredCloud("azure", storage);
    expect(store.get(CLOUD_STORAGE_KEY)).toBe("azure");
    expect(readStoredCloud(storage)).toBe("azure");
  });

  it("swallows storage errors", () => {
    const throwing = {
      getItem: () => {
        throw new Error("blocked");
      },
      setItem: () => {
        throw new Error("blocked");
      },
    };
    expect(readStoredCloud(throwing)).toBeNull();
    expect(() => writeStoredCloud("gcp", throwing)).not.toThrow();
  });
});

describe("availableAzureServices", () => {
  it("lists only services the backend reports, case-insensitively", () => {
    const got = availableAzureServices([
      "S3",
      "azureblob",
      "CosmosDB",
      "AzureQueue",
      "AzureMonitor",
    ]);
    expect(got.map((s) => s.id)).toEqual(["blob", "queue", "cosmosdb", "monitor"]);
  });

  it("is empty when no Azure service is served", () => {
    expect(availableAzureServices(["S3", "DynamoDB"])).toEqual([]);
  });

  it("has unique ids and ports", () => {
    expect(new Set(AZURE_SERVICES.map((s) => s.id)).size).toBe(AZURE_SERVICES.length);
    const labels = new Map(AZURE_SERVICES.map((s) => [s.portLabel, s.defaultPort]));
    expect(new Set(labels.values()).size).toBe(labels.size);
  });
});

describe("categoriesForCloud", () => {
  const azure = availableAzureServices(["AzureBlob", "AzureTable"]);

  it("passes AWS categories through unchanged", () => {
    expect(categoriesForCloud("aws", sidebarCategories, azure)).toBe(sidebarCategories);
  });

  it("builds an Azure category linking to per-service routes", () => {
    const cats = categoriesForCloud("azure", sidebarCategories, azure);
    expect(cats).toHaveLength(1);
    expect(cats[0]?.routes.map((r) => r.href)).toEqual([
      "/dashboard/azure/blob",
      "/dashboard/azure/table",
    ]);
  });

  it("is empty for Azure with nothing served and for GCP", () => {
    expect(categoriesForCloud("azure", sidebarCategories, [])).toEqual([]);
    expect(categoriesForCloud("gcp", sidebarCategories, azure)).toEqual([]);
  });

  it("lists served GCP services under the GCP cloud", () => {
    const cats = categoriesForCloud(
      "gcp",
      sidebarCategories,
      azure,
      availableGcpServices(["GCPStorage"]),
    );
    expect(cats[0]?.routes.map((r) => r.href)).toEqual(["/dashboard/gcp/storage"]);
    expect(availableGcpServices(["S3"])).toEqual([]);
  });

  it("lists Pub/Sub, Secret Manager and Firestore only when served", () => {
    const served = availableGcpServices(["GCPFirestore", "GCPSecretManager", "GCPPubSub"]);
    expect(served.map((s) => s.id)).toEqual(["pubsub", "secretmanager", "firestore"]);
    expect(served.map((s) => s.portLabel)).toEqual([
      "gcppubsub",
      "gcpsecretmanager",
      "gcpfirestore",
    ]);
  });
});

describe("withCloud", () => {
  it("sets the param, preserving path and other params", () => {
    const url = new URL("http://x/dashboard/s3?tab=a#h");
    expect(withCloud(url, "azure")).toBe("/dashboard/s3?tab=a&cloud=azure#h");
  });

  it("removes the param for the default cloud", () => {
    const url = new URL("http://x/dashboard?cloud=gcp");
    expect(withCloud(url, "aws")).toBe("/dashboard");
  });
});

describe("cloudFromPath", () => {
  it.each([
    ["/dashboard/azure/blob", "azure"],
    ["/dashboard/gcp", "gcp"],
    ["/dashboard/gcp/sql", "gcp"],
    ["/dashboard/oci", "oci"],
    ["/dashboard/oci/vault", "oci"],
    ["/dashboard/ocifoo", null],
    ["/dashboard/s3", null],
    ["/dashboard/gcpfoo", null],
    ["/dashboard/", null],
  ])("%s", (path, want) => expect(cloudFromPath(path)).toBe(want));
});

describe("Oracle Cloud", () => {
  it("is registered in the switcher with a label and icon", () => {
    expect(CLOUDS.find((c) => c.id === "oci")).toEqual({
      id: "oci",
      label: "Oracle Cloud",
      icon: "oci",
    });
  });

  it("lists only served services, case-insensitively", () => {
    const got = availableOciServices([
      "S3",
      "ocivault",
      "OCIObjectStorage",
      "OCIIdentity",
      "OCIFunctions",
    ]);
    expect(got.map((s) => s.id)).toEqual(["objectstorage", "vault", "identity", "functions"]);
    expect(availableOciServices(["S3"])).toEqual([]);
  });

  it("has unique ids and ports", () => {
    expect(new Set(OCI_SERVICES.map((s) => s.id)).size).toBe(OCI_SERVICES.length);
    expect(new Set(OCI_SERVICES.map((s) => s.defaultPort)).size).toBe(OCI_SERVICES.length);
  });

  it("builds an Oracle Cloud category of per-service routes", () => {
    const cats = categoriesForCloud(
      "oci",
      sidebarCategories,
      [],
      [],
      availableOciServices(["OCIStreaming", "OCIVault"]),
    );
    expect(cats).toHaveLength(1);
    expect(cats[0]?.routes.map((r) => r.href)).toEqual([
      "/dashboard/oci/vault",
      "/dashboard/oci/streaming",
    ]);
    expect(categoriesForCloud("oci", sidebarCategories, [], [], [])).toEqual([]);
  });

  it("derives route ids and icons from the service id", () => {
    const [first] = OCI_SERVICES;
    expect(ociRoute(first)).toEqual({
      id: "oci-objectstorage",
      href: "/dashboard/oci/objectstorage",
      label: "Object Storage",
      icon: "oci-objectstorage",
    });
  });
});

describe("new GCP console pages", () => {
  it("registers Workflows and Cloud DNS", () => {
    expect(availableGcpServices(["GCPWorkflows", "GCPDNS"]).map((s) => s.id)).toEqual([
      "workflows",
      "dns",
    ]);
    expect(new Set(GCP_SERVICES.map((s) => s.defaultPort)).size).toBe(GCP_SERVICES.length);
  });

  it("registers Compute Engine and Container Instances", () => {
    expect(availableGcpServices(["GCPCompute"]).map((s) => s.id)).toEqual(["compute"]);
    expect(availableAzureServices(["AzureARM"]).map((s) => s.id)).toEqual([
      "arm",
      "aci",
      "appservice",
      "containerapps",
      "vm",
      "aks",
      "flexdb",
      "apim",
    ]);
    expect(azureServiceById("aci")?.portLabel).toBe("azurearm");
  });

  it("registers Azure Files", () => {
    expect(availableAzureServices(["AzureFile"]).map((s) => s.id)).toEqual(["file"]);
  });

  it("registers Datastore, Core Services and App Service", () => {
    expect(availableGcpServices(["GCPDatastore"]).map((s) => s.id)).toEqual(["datastore"]);
    expect(availableOciServices(["OCICore"]).map((s) => s.id)).toEqual(["core"]);
    expect(azureServiceById("appservice")?.portLabel).toBe("azurearm");
    expect(gcpServiceById("datastore")?.portLabel).toBe("gcpdatastore");
    expect(ociServiceById("core")?.portLabel).toBe("ocicore");
  });

  it("registers Kubernetes Engine, Container Apps, Virtual Machines, Queue and Notifications", () => {
    expect(availableGcpServices(["GCPContainer"]).map((s) => s.id)).toEqual(["container"]);
    expect(availableOciServices(["OCIQueue", "OCINotifications"]).map((s) => s.id)).toEqual([
      "queue",
      "notifications",
    ]);
    expect(availableOciServices(["OCIContainerEngine"]).map((s) => s.id)).toEqual([
      "containerengine",
    ]);
    expect(azureServiceById("aks")?.portLabel).toBe("azurearm");
    expect(ociServiceById("containerengine")?.portLabel).toBe("ocicontainerengine");
    expect(azureServiceById("containerapps")?.portLabel).toBe("azurearm");
    expect(azureServiceById("vm")?.portLabel).toBe("azurearm");
    expect(gcpServiceById("container")?.portLabel).toBe("gcpcontainer");
    expect(ociServiceById("queue")?.portLabel).toBe("ociqueue");
    expect(ociServiceById("notifications")?.portLabel).toBe("ocinotifications");
  });

  it("registers MySQL HeatWave, Logging and Monitoring", () => {
    expect(
      availableOciServices(["OCIMySQL", "OCILogging", "OCIMonitoring"]).map((s) => s.id),
    ).toEqual(["mysql", "logging", "monitoring"]);
    expect(ociServiceById("mysql")?.portLabel).toBe("ocimysql");
    expect(ociServiceById("logging")?.defaultPort).toBe(8160);
    expect(ociServiceById("monitoring")?.healthName).toBe("OCIMonitoring");
  });

  it("registers NoSQL Database and API Gateway", () => {
    expect(availableOciServices(["OCINoSQL", "OCIAPIGateway"]).map((s) => s.id)).toEqual([
      "nosql",
      "apigateway",
    ]);
    expect(ociServiceById("nosql")).toMatchObject({ portLabel: "ocinosql", defaultPort: 8162 });
    expect(ociServiceById("apigateway")).toMatchObject({
      portLabel: "ociapigateway",
      defaultPort: 8163,
      healthName: "OCIAPIGateway",
    });
  });

  it("registers Events", () => {
    expect(availableOciServices(["OCIEvents"]).map((s) => s.id)).toEqual(["events"]);
    expect(ociServiceById("events")).toMatchObject({
      portLabel: "ocievents",
      defaultPort: 8164,
      healthName: "OCIEvents",
    });
  });

  it("registers Load Balancer", () => {
    expect(availableOciServices(["OCILoadBalancer"]).map((s) => s.id)).toEqual(["loadbalancer"]);
    expect(ociServiceById("loadbalancer")).toMatchObject({
      portLabel: "ociloadbalancer",
      defaultPort: 8165,
      healthName: "OCILoadBalancer",
    });
  });

  it("registers Artifacts", () => {
    expect(availableOciServices(["OCIArtifacts"]).map((s) => s.id)).toEqual(["artifacts"]);
    expect(ociServiceById("artifacts")).toMatchObject({
      portLabel: "ociartifacts",
      defaultPort: 8166,
      healthName: "OCIArtifacts",
    });
  });

  it("registers Container Instances", () => {
    expect(availableOciServices(["OCIContainerInstances"]).map((s) => s.id)).toEqual([
      "containerinstances",
    ]);
    expect(ociServiceById("containerinstances")).toMatchObject({
      portLabel: "ocicontainerinstances",
      defaultPort: 8167,
      healthName: "OCIContainerInstances",
    });
  });

  it("registers Identity Platform and Flexible Server", () => {
    expect(gcpServiceById("identitytoolkit")).toMatchObject({
      portLabel: "gcpidentitytoolkit",
      defaultPort: 8145,
      healthName: "GCPIdentityToolkit",
    });
    expect(azureServiceById("flexdb")?.portLabel).toBe("azurearm");
  });

  it("registers API Gateway and API Management", () => {
    expect(gcpServiceById("apigateway")).toMatchObject({
      portLabel: "gcpapigateway",
      defaultPort: 8146,
      healthName: "GCPAPIGateway",
    });
    expect(azureServiceById("apim")?.portLabel).toBe("azurearm");
  });

  it("registers Cloud Batch", () => {
    expect(gcpServiceById("batch")).toMatchObject({
      portLabel: "gcpbatch",
      defaultPort: 8147,
      healthName: "GCPBatch",
    });
  });

  it("registers Resource Manager", () => {
    expect(gcpServiceById("resourcemanager")).toMatchObject({
      portLabel: "gcpresourcemanager",
      defaultPort: 8148,
      healthName: "GCPResourceManager",
    });
  });
});
