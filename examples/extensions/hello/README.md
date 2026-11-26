# hello extension

A minimal extension. It serves `/hello` and logs startup, shutdown and S3 request hooks.

## Run

From the repo root:

```bash
gopherstack --extensions-config examples/extensions/hello/extensions.yaml
curl localhost:8000/hello
curl localhost:8000/_gopherstack/extensions
```

`extensions.yaml` starts the extension with `go run ./examples/extensions/hello`, so run from the repo root with Go installed.

See [`docs/extensions.md`](../../../docs/extensions.md).
