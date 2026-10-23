# Gopherstack testcontainers module (Node)

```js
import { GopherstackContainer } from "@blackbirdworks/testcontainers-gopherstack";

const gs = await new GopherstackContainer().withServices("s3", "sqs").start();
const s3 = new S3Client(gs.getAwsClientConfig());
gs.getEndpoint();                // http://host:port
gs.getAzureConnectionString();
gs.getGcpEndpoint("storage");    // host:port
await gs.stop();
```

Also `withPersistence(dir)`, `withDockerSocket()` (see the
[socket warning](../README.md#docker-socket-warning)), `withEnv(k, v)`.

```bash
npm install && GOPHERSTACK_TEST_IMAGE=gopherstack:tc-local npm test
```
