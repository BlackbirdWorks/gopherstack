# stepfunctions-order-workflow

A Step Functions state machine validates an order, takes payment, then fulfils or cancels it. Each task is a Node.js Lambda.

```
ValidateOrder -> ProcessPayment -> PaymentChoice --SUCCESS--> FulfillOrder
                                                 \--default--> CancelOrder
```

## What it shows

- OpenTofu (`main.tf`) creates four `nodejs20.x` Lambdas (`validate-order`, `process-payment`, `fulfill-order`, `cancel-order`, all from `src/index.js`) and the state machine `OrderWorkflow` from `state-machine.asl.json`.
- `ValidateOrder` retries on `States.TaskFailed` (2 attempts, 2 s interval, backoff 2.0) and throws if `amount` is missing or not positive.
- `ProcessPayment` returns `paymentStatus` `SUCCESS` about 80% of the time and `FAILED` otherwise. The `Choice` state routes on it.
- `demo.sh` starts an execution with `{"orderId": "ORD-<n>", "amount": 150}`, waits 5 s, then prints the execution history and the result.

## Prerequisites

- Docker, OpenTofu (`tofu`), AWS CLI v2.

> [!WARNING]
> The compose file mounts `/var/run/docker.sock` and runs Gopherstack as root so it can start the Lambda containers.

> [!NOTE]
> `main.tf` and `demo.sh` use `http://localhost:8000`, so Gopherstack must listen there.

## Run

```bash
docker compose up -d --build
docker compose ps        # wait until healthy
./demo.sh
```

On Docker Desktop, start the stack with `LAMBDA_DOCKER_HOST=host.docker.internal docker compose up -d --build`. The default is `172.17.0.1`, the Linux Docker bridge gateway.

## Expected output

An execution history table, then the result. The path depends on the random payment outcome. A failed payment ends like this:

```
|  ChoiceStateEntered|  PaymentChoice  |  None           |  None                                                             |
|  ChoiceStateExited |  None           |  PaymentChoice  |  None                                                             |
|  TaskStateEntered  |  CancelOrder    |  None           |  None                                                             |
...
|  ExecutionSucceeded|  None           |  None           |  None                                                             |

=== Execution Result ===
{
    "Status": "SUCCEEDED",
    "Output": "{\"amount\":150,\"message\":\"Your order was canceled due to payment failure.\",\"orderId\":\"ORD-29140\",\"paymentStatus\":\"FAILED\",\"status\":\"CANCELED\"}"
}
```

A successful payment goes through `FulfillOrder` and ends with `"status":"FULFILLED"` and `"message":"Your order is on the way!"`. The execution status is `SUCCEEDED` in both cases.

## Files

| File | Purpose |
|------|---------|
| `main.tf` | Roles, Lambdas, state machine |
| `state-machine.asl.json` | State machine definition |
| `src/index.js` | The four handlers |
| `demo.sh` | Deploy, start an execution, print history and result |

## Clean up

```bash
tofu destroy -auto-approve
docker compose down -v
```
