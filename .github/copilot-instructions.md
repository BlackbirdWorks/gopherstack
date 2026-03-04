# Session Management

- Commit and push after completing each task.
- Your max session is 1 hour — when approaching the limit, create `checkpoint.md` at the repo root (what is done, what remains, any blockers) and push. Remove `checkpoint.md` when the full issue is complete.
- Run `make test` after each task before committing. Resolve all lint issues via `make lint-fix`.
- Min test coverage is 85%.
- Add integration tests in `test/integration/` as you go for everything you implement.
- Run `make build` before pushing.

# Go Development Instructions

Follow idiomatic Go practices and community standards when writing Go code. These instructions are based on [Effective Go](https://go.dev/doc/effective_go), [Go Code Review Comments](https://go.dev/wiki/CodeReviewComments), and [Google's Go Style Guide](https://google.github.io/styleguide/go/).

## General Instructions

- Write simple, clear, and idiomatic Go code
- Favor clarity and simplicity over cleverness
- Follow the principle of least surprise
- Keep the happy path left-aligned (minimize indentation)
- Return early to reduce nesting
- Prefer early return over if-else chains; use `if condition { return }` pattern to avoid else blocks
- Make the zero value useful
- Write self-documenting code with clear, descriptive names
- Document exported types, functions, methods, and packages
- Use Go modules for dependency management
- Leverage the Go standard library instead of reinventing the wheel (e.g., use `strings.Builder` for string concatenation, `filepath.Join` for path construction)
- Prefer standard library solutions over custom implementations when functionality exists
- Write comments in English by default; translate only upon user request
- Avoid using emoji in code and comments
- Avoid `break` statements; any loop break should be factored into a separate function with a fast return
- Logging MUST always be performed via `log/slog` and MUST always be pulled from the context (e.g. `logger.Load(ctx)`)
- Avoid `nolint` directives; removing rules is forbidden unless no other fix is available

## Naming Conventions

### Packages

- Use lowercase, single-word package names
- Avoid underscores, hyphens, or mixedCaps
- Choose names that describe what the package provides, not what it contains
- Avoid generic names like `util`, `common`, or `base`
- Package names should be singular, not plural

#### Package Declaration Rules (CRITICAL):
- **NEVER duplicate `package` declarations** - each Go file must have exactly ONE `package` line
- When editing an existing `.go` file:
  - **PRESERVE** the existing `package` declaration - do not add another one
  - If you need to replace the entire file content, start with the existing package name
- When creating a new `.go` file:
  - **BEFORE writing any code**, check what package name other `.go` files in the same directory use
  - Use the SAME package name as existing files in that directory
  - If it's a new directory, use the directory name as the package name
  - Write **exactly one** `package <name>` line at the very top of the file
- When using file creation or replacement tools:
  - **ALWAYS verify** the target file doesn't already have a `package` declaration before adding one
  - If replacing file content, include only ONE `package` declaration in the new content
  - **NEVER** create files with multiple `package` lines or duplicate declarations

### Variables and Functions

- Use mixedCaps or MixedCaps (camelCase) rather than underscores
- Keep names short but descriptive
- Use single-letter variables only for very short scopes (like loop indices)
- Exported names start with a capital letter
- Unexported names start with a lowercase letter
- Avoid stuttering (e.g., avoid `http.HTTPServer`, prefer `http.Server`)

### Interfaces

- Name interfaces with -er suffix when possible (e.g., `Reader`, `Writer`, `Formatter`)
- Single-method interfaces should be named after the method (e.g., `Read` → `Reader`)
- Keep interfaces small and focused

### Constants

- Use MixedCaps for exported constants
- Use mixedCaps for unexported constants
- Group related constants using `const` blocks
- Consider using typed constants for better type safety

## Code Style and Formatting

### Formatting

- Always use `gofmt` to format code
- Use `goimports` to manage imports automatically
- Keep line length reasonable (no hard limit, but consider readability)
- Add blank lines to separate logical groups of code

### Comments

- Strive for self-documenting code; prefer clear variable names, function names, and code structure over comments
- Write comments only when necessary to explain complex logic, business rules, or non-obvious behavior
- Write comments in complete sentences in English by default
- Translate comments to other languages only upon specific user request
- Start sentences with the name of the thing being described
- Package comments should start with "Package [name]"
- Use line comments (`//`) for most comments
- Use block comments (`/* */`) sparingly, mainly for package documentation
- Document why, not what, unless the what is complex
- Avoid emoji in comments and code

### Error Handling

- Check errors immediately after the function call
- Don't ignore errors using `_` unless you have a good reason (document why)
- Wrap errors with context using `fmt.Errorf` with `%w` verb
- Create custom error types when you need to check for specific errors
- Place error returns as the last return value
- Name error variables `err`
- Keep error messages lowercase and don't end with punctuation
- Prefer sentinel errors for common error conditions

## Architecture and Project Structure

### Package Organization

- Follow standard Go project layout conventions
- Keep `main` packages in `cmd/` directory
- Put reusable packages in `pkg/` or `internal/`
- Use `internal/` for packages that shouldn't be imported by external projects
- Group related functionality into packages
- Break common functionality into packages under `/pkgs`
- Avoid circular dependencies

### Dependency Management

- Use Go modules (`go.mod` and `go.sum`)
- Keep dependencies minimal
- Regularly update dependencies for security patches
- Use `go mod tidy` to clean up unused dependencies
- Vendor dependencies only when necessary

### Service Operations

- All service operations MUST have a metric recorded
- All service operations MUST have extensive unit tests
- All service operations MUST have integration tests using the Go AWS SDK v2

## Type Safety and Language Features

### Type Definitions

- Define types to add meaning and type safety
- Use struct tags for JSON, XML, database mappings
- Prefer explicit type conversions
- Use type assertions carefully and check the second return value
- Avoid the use of anonymous structs
- Prefer generics over unconstrained types; when an unconstrained type is truly needed, use the predeclared alias `any` instead of `interface{}` (Go 1.18+)

### Pointers vs Values

- Use pointer receivers for large structs or when you need to modify the receiver
- Use value receivers for small structs and when immutability is desired
- Use pointer parameters when you need to modify the argument or for large structs
- Use value parameters for small structs and when you want to prevent modification
- Be consistent within a type's method set
- Consider the zero value when choosing pointer vs value receivers

### Interfaces and Composition

- Accept interfaces, return concrete types
- Keep interfaces small (1-3 methods is ideal)
- Use embedding for composition
- Define interfaces close to where they're used, not where they're implemented
- Don't export interfaces unless necessary

## Concurrency

### Goroutines

- Be cautious about creating goroutines in libraries; prefer letting the caller control concurrency
- If you must create goroutines in libraries, provide clear documentation and cleanup mechanisms
- Always know how a goroutine will exit
- Use `sync.WaitGroup` or channels to wait for goroutines
- Avoid goroutine leaks by ensuring cleanup

### Channels

- Use channels to communicate between goroutines
- Don't communicate by sharing memory; share memory by communicating
- Close channels from the sender side, not the receiver
- Use buffered channels when you know the capacity
- Use `select` for non-blocking operations

### Synchronization

- Use `sync.Mutex` for protecting shared state
- Keep critical sections small
- Use `sync.RWMutex` when you have many readers
- Choose between channels and mutexes based on the use case: use channels for communication, mutexes for protecting state
- Use `sync.Once` for one-time initialization
- WaitGroup usage by Go version:
	- If `go >= 1.25` in `go.mod`, use the new `WaitGroup.Go` method ([documentation](https://pkg.go.dev/sync#WaitGroup)):
		```go
		var wg sync.WaitGroup
		wg.Go(task1)
		wg.Go(task2)
		wg.Wait()
		```
	- If `go < 1.25`, use the classic `Add`/`Done` pattern

## Error Handling Patterns

### Creating Errors

- Use `errors.New` for simple static errors
- Use `fmt.Errorf` for dynamic errors
- Create custom error types for domain-specific errors
- Export error variables for sentinel errors
- Use `errors.Is` and `errors.As` for error checking

### Error Propagation

- Add context when propagating errors up the stack
- Don't log and return errors (choose one)
- Handle errors at the appropriate level
- Consider using structured errors for better debugging

## API Design

### HTTP Handlers

- Use `http.HandlerFunc` for simple handlers
- Implement `http.Handler` for handlers that need state
- Use middleware for cross-cutting concerns
- Set appropriate status codes and headers
- Handle errors gracefully and return appropriate error responses
- Router usage by Go version:
	- If `go >= 1.22`, prefer the enhanced `net/http` `ServeMux` with pattern-based routing and method matching
	- If `go < 1.22`, use the classic `ServeMux` and handle methods/paths manually (or use a third-party router when justified)

### JSON APIs

- Use struct tags to control JSON marshaling
- Validate input data
- Use pointers for optional fields
- Consider using `json.RawMessage` for delayed parsing
- Handle JSON errors appropriately

### HTTP Clients

- Keep the client struct focused on configuration and dependencies only (e.g., base URL, `*http.Client`, auth, default headers). It must not store per-request state
- Do not store or cache `*http.Request` inside the client struct, and do not persist request-specific state across calls; instead, construct a fresh request per method invocation
- Methods should accept `context.Context` and input parameters, assemble the `*http.Request` locally (or via a short-lived builder/helper created per call), then call `c.httpClient.Do(req)`
- If request-building logic is reused, factor it into unexported helper functions or a per-call builder type; never keep `http.Request` (URL params, body, headers) as fields on the long-lived client
- Ensure the underlying `*http.Client` is configured (timeouts, transport) and is safe for concurrent use; avoid mutating `Transport` after first use
- Always set headers on the request instance you're sending, and close response bodies (`defer resp.Body.Close()`), handling errors appropriately

## Performance Optimization

### Memory Management

- Minimize allocations in hot paths
- Reuse objects when possible (consider `sync.Pool`)
- Use value receivers for small structs
- Preallocate slices when size is known
- Avoid unnecessary string conversions

### I/O: Readers and Buffers

- Most `io.Reader` streams are consumable once; reading advances state. Do not assume a reader can be re-read without special handling
- If you must read data multiple times, buffer it once and recreate readers on demand:
	- Use `io.ReadAll` (or a limited read) to obtain `[]byte`, then create fresh readers via `bytes.NewReader(buf)` or `bytes.NewBuffer(buf)` for each reuse
	- For strings, use `strings.NewReader(s)`; you can `Seek(0, io.SeekStart)` on `*bytes.Reader` to rewind
- For HTTP requests, do not reuse a consumed `req.Body`. Instead:
	- Keep the original payload as `[]byte` and set `req.Body = io.NopCloser(bytes.NewReader(buf))` before each send
	- Prefer configuring `req.GetBody` so the transport can recreate the body for redirects/retries: `req.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(buf)), nil }`
- To duplicate a stream while reading, use `io.TeeReader` (copy to a buffer while passing through) or write to multiple sinks with `io.MultiWriter`
- Reusing buffered readers: call `(*bufio.Reader).Reset(r)` to attach to a new underlying reader; do not expect it to "rewind" unless the source supports seeking
- For large payloads, avoid unbounded buffering; consider streaming approaches

## Testing

- Tests MUST be table tests. No matter what
- Tests should always be parallel unless an environment variable is involved
- Tests should always cover at least 85% of the logic
- `make test` to run all the unit tests
- `make integration-test` to run all the integration tests
- Use `t.Context()` in tests
- NEVER use `t.Fatal` or `t.Error`; only use `require` and `assert` from `testify`

### Table-Driven Tests

All test cases must use the table-driven test pattern with struct field assertions. Do NOT write tests that just call a generic `run()` function — that is not a table test.

A proper table test has:
- A `[]struct` with named fields for **args** (inputs), **want** (expected output or struct fields), and **wantErr** (error assertion).
- An optional `setup func()` for per-case state initialization.
- A `for _, tt := range tests` loop with `t.Run(tt.name, ...)`.
- Direct field assertions using `assert.Equal`, `assert.Contains`, etc. — not a single pass/fail helper.
- `require` for preconditions (early exit on failure), `assert` for outcome checks.
- `t.Parallel()` at both the top-level function and inside each `t.Run`.

Example:

```go
func TestParseConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*MockStore)
		name    string
		input   string
		want    Config
		wantErr bool
	}{
		{
			name:  "valid",
			input: `{"timeout": 30}`,
			want:  Config{Timeout: 30},
		},
		{
			name:  "with_setup",
			input: `{"timeout": 60}`,
			setup: func(s *MockStore) { s.Set("default_timeout", 60) },
			want:  Config{Timeout: 60},
		},
		{
			name:    "invalid_json",
			input:   `not-json`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := NewMockStore()
			if tt.setup != nil {
				tt.setup(store)
			}

			got, err := ParseConfig(tt.input, store)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want.Timeout, got.Timeout)
		})
	}
}
```

## Service UI Guidelines

When developing a UI for a service, ensure:
- The UI uses service functions via the AWS SDK
- The service is accessible via a tab at the top
- The service's operations are documented in the Docs section
- There is an E2E test using the UI fully for that service (covering all interactions)
- The service has metrics displayed on the Metrics tab
- All services look good in both light and dark modes
- All services define demo data for visual inspections

## Build and Development Commands

Always use Makefile targets instead of running tools directly:
- `make build` — build the project
- `make lint` — run linting (installs deps automatically)
- `make lint-fix` — auto-fix lint issues including fieldalignment
- `make test` — run unit tests
- `make total-coverage` — run all tests with coverage
- `make integration-test` — run integration tests

Do NOT run `golangci-lint`, `fieldalignment`, `go test`, or `go build` directly. The Makefile targets handle tool installation, flags, and configuration automatically.

- Always ensure code follows the rules in this file
- You are to NEVER change the `golangci-lint` configuration
