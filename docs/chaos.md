# Chaos Testing

Gopherstack includes a built-in Chaos Testing API that allows you to inject faults and simulate network degradation (latency and jitter) against your AWS clients. 

The chaos middleware is **always enabled** and evaluates every incoming AWS SDK request, but it does nothing by default. You can configure faults dynamically at runtime using the Chaos API mounted at `/_gopherstack/chaos`.

## Network Effects

You can simulate network latency and jitter across all requests. This is useful for testing client timeouts and retry behavior.

### `POST /_gopherstack/chaos/effects`

Update the network effects configuration.

**Request Body:**
```json
{
  "latency": 100,
  "jitter": 50,
  "latencyRange": {
    "min": 50,
    "max": 200
  }
}
```

- `latency`: Base latency added to every request (in milliseconds).
- `jitter`: Random additional latency between `0` and `jitter` (in milliseconds).
- `latencyRange`: Random additional latency between `min` and `max` (in milliseconds).

All fields are optional.

### `POST /_gopherstack/chaos/effects/reset`

Resets all network effects back to zero.

## Fault Injection

You can inject specific HTTP errors (like 503 Service Unavailable or 400 ThrottlingException) for particular services, operations, or regions.

### `POST /_gopherstack/chaos/faults`

Replaces the entire fault configuration.

**Request Body:**
```json
[
  {
    "service": "dynamodb",
    "operation": "PutItem",
    "region": "us-east-1",
    "probability": 0.5,
    "error": {
      "code": "ProvisionedThroughputExceededException",
      "statusCode": 400
    }
  },
  {
    "service": "s3",
    "probability": 1.0,
    "error": {
      "code": "ServiceUnavailable",
      "statusCode": 503
    }
  }
]
```

- **Matching Fields**: `service`, `operation`, `region`. If omitted, the rule matches any request.
- `probability`: Float between `0.0` and `1.0`. A value of `0` is treated as `1.0` (always fire).
- `error`: Custom HTTP error. Defaults to 503 `ServiceUnavailable` if not provided.

### Other Fault Endpoints

- `GET /_gopherstack/chaos/faults`: Returns current fault rules.
- `PATCH /_gopherstack/chaos/faults`: Appends rules to the existing configuration.
- `DELETE /_gopherstack/chaos/faults`: Removes matching rules.
- `POST /_gopherstack/chaos/faults/clear`: Clears all fault rules.
- `DELETE /_gopherstack/chaos/faults/by-index`: Removes a fault rule by index.

## Activity Log

The chaos middleware records fault injection events up to a maximum of 100 recent entries.

- `GET /_gopherstack/chaos/activity`: Returns recent fault injection events in reverse-chronological order.

## State Query

- `GET /_gopherstack/chaos/query`: Returns the combined state of faults, effects, and recent activity.

## AWS Fault Injection Service (FIS)

Gopherstack's Chaos testing integrates natively with the mock AWS Fault Injection Service (FIS). When you run FIS experiments with `aws:fis:inject-api-*` actions, Gopherstack automatically translates them into chaos rules during the experiment and cleans them up afterward.

## Discoverable Targets

- `GET /_gopherstack/chaos/targets`: Returns auto-discovered injectable targets (services, operations, regions) available for fault injection.
