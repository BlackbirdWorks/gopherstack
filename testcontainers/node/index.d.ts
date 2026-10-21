import type { StartedTestContainer } from "testcontainers";

export declare const DEFAULT_IMAGE: string;
export declare const AZURE_PORTS: Record<string, number>;
export declare const GCP_PORTS: Record<string, number>;

export declare class GopherstackContainer {
  constructor(image?: string);
  withServices(...names: string[]): this;
  withPersistence(hostDir: string): this;
  withDockerSocket(): this;
  withEnv(key: string, value: string): this;
  start(): Promise<StartedGopherstackContainer>;
}

export declare class StartedGopherstackContainer {
  readonly container: StartedTestContainer;
  getEndpoint(): string;
  getAwsClientConfig(): {
    endpoint: string;
    region: string;
    credentials: { accessKeyId: string; secretAccessKey: string };
    forcePathStyle: boolean;
  };
  getAzureEndpoints(): Record<string, string>;
  getAzureConnectionString(): string;
  getGcpEndpoint(service: string): string;
  stop(): Promise<unknown>;
}
