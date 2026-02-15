# Gopherstack — Claude Code Instructions

These rules apply to all interactions in this repository.

---

## Workspace

- All code must pass `make lint`
- Errors should be sentinel errors
- Logging must be via `slog`
- Avoid `break` statements; any `break` can be replaced with a separate function and an early return
- Avoid anonymous structs
- Break common functionality into packages under `/pkgs`
- Write idiomatic Go

---

## Testing

- Tests must always be table-driven tests — no exceptions
- Tests must always run in parallel unless an environment variable is involved
- Tests must cover at least 85% of the logic
- `make test` runs all unit tests
- `make integration-test` runs all integration tests
- Use `t.Context()` in tests
- Never use `t.Fatal` or `t.Error` — only use `require` and `assert` from testify
- To run a single integration test: `go test -v -race -tags=integration -run TestName ./test/integration/...`

---

## Go Style Guide

Follow idiomatic Go practices based on [Effective Go](https://go.dev/doc/effective_go), [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments), and [Google's Go Style Guide](https://google.github.io/styleguide/go/).

### General

- Write simple, clear, idiomatic Go — favor clarity over cleverness
- Keep the happy path left-aligned; return early to reduce nesting
- Prefer `if condition { return }` over else chains
- Make the zero value useful
- Document exported types, functions, methods, and packages
- Prefer standard library solutions over custom implementations
- No emoji in code or comments

### Naming

- Package names: lowercase, single-word, singular, no underscores or hyphens
- Each `.go` file must have exactly one `package` declaration — never duplicate it
- Variables/functions: `mixedCaps` or `MixedCaps`, not underscores
- Exported names start with a capital letter; unexported start lowercase
- Interfaces: `-er` suffix when possible (e.g., `Reader`, `Writer`)
- Constants: `MixedCaps` (exported), `mixedCaps` (unexported); group related constants in `const` blocks

### Error Handling

- Check errors immediately after the call
- Wrap errors with context: `fmt.Errorf("context: %w", err)`
- Use sentinel errors (`errors.New`) for static errors; `fmt.Errorf` for dynamic
- Use `errors.Is` / `errors.As` for error checking
- Error messages: lowercase, no trailing punctuation
- Don't log and return the same error — choose one

### Concurrency

- Use channels to communicate; use mutexes to protect state
- Always know how a goroutine exits; avoid goroutine leaks
- Use `sync.RWMutex` when there are many readers
- WaitGroup by Go version:
  - `go >= 1.25`: use `wg.Go(fn)`
  - `go < 1.25`: use classic `Add` / `Done` pattern

### HTTP

- `go >= 1.22`: use enhanced `net/http` `ServeMux` with method+path patterns
- `go < 1.22`: handle methods/paths manually or use a justified third-party router
- HTTP client structs hold configuration only — never per-request state
- Construct a fresh `*http.Request` per method invocation
- Always `defer resp.Body.Close()`

### Types

- Use `any` instead of `interface{}` (Go 1.18+)
- Accept interfaces, return concrete types
- Keep interfaces small (1–3 methods ideal)
- Define interfaces close to where they are used

### Performance

- Preallocate slices when size is known
- Minimize allocations in hot paths; consider `sync.Pool`
- Buffer `io.Reader` streams once with `io.ReadAll`, then recreate with `bytes.NewReader`
