package lambda

import (
	"archive/zip"
	"bytes"
	"os"
	"strconv"
)

const (
	accountDefaultCreateFunctionRequestSize = 70167211
	accountDefaultMaxEnvironmentBytes       = 4096
)

// limits holds the effective account quotas; zero Settings fields resolve to AWS defaults.
type limits struct {
	concurrentExecutions      int
	minUnreservedConcurrency  int
	totalCodeSize             int64
	codeSizeZipped            int64
	codeSizeUnzipped          int64
	createFunctionRequestSize int64
	maxEnvironmentBytes       int
}

func (s Settings) limits() limits {
	return limits{
		concurrentExecutions:      orDefault(s.ConcurrentExecutions, accountDefaultConcurrentExecutions),
		minUnreservedConcurrency:  orDefault(s.MinimumUnreservedConcurrency, minUnreservedConcurrency),
		totalCodeSize:             orDefault(s.TotalCodeSize, accountDefaultTotalCodeSize),
		codeSizeZipped:            orDefault(s.CodeSizeZipped, accountDefaultCodeSizeZipped),
		codeSizeUnzipped:          orDefault(s.CodeSizeUnzipped, accountDefaultCodeSizeUnzipped),
		createFunctionRequestSize: orDefault(s.CreateFunctionRequestSize, accountDefaultCreateFunctionRequestSize),
		maxEnvironmentBytes:       orDefault(s.MaxEnvironmentBytes, accountDefaultMaxEnvironmentBytes),
	}
}

func orDefault[T int | int64](v, def T) T {
	if v > 0 {
		return v
	}

	return def
}

func (h *Handler) limits() limits {
	if bk, ok := h.Backend.(*InMemoryBackend); ok {
		return bk.settings.limits()
	}

	return Settings{}.limits()
}

func (b *InMemoryBackend) limits() limits { return b.settings.limits() }

// unzippedSize sums the uncompressed entry sizes; ok is false when data is not a readable zip.
func unzippedSize(data []byte) (int64, bool) {
	r, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return 0, false
	}

	var total int64

	for _, f := range r.File {
		total += int64(f.UncompressedSize64) //nolint:gosec // bounded by zip central directory
	}

	return total, true
}

func (s Settings) withLimitEnv() Settings {
	for _, e := range []struct {
		env string
		dst *int
	}{
		{"LAMBDA_ACCOUNT_CONCURRENT_EXECUTIONS", &s.ConcurrentExecutions},
		{"LAMBDA_ACCOUNT_MIN_UNRESERVED_CONCURRENCY", &s.MinimumUnreservedConcurrency},
		{"LAMBDA_MAX_ENVIRONMENT_BYTES", &s.MaxEnvironmentBytes},
	} {
		if v, err := strconv.Atoi(os.Getenv(e.env)); err == nil && v > 0 {
			*e.dst = v
		}
	}

	for _, e := range []struct {
		env string
		dst *int64
	}{
		{"LAMBDA_ACCOUNT_TOTAL_CODE_SIZE", &s.TotalCodeSize},
		{"LAMBDA_CODE_SIZE_ZIPPED", &s.CodeSizeZipped},
		{"LAMBDA_CODE_SIZE_UNZIPPED", &s.CodeSizeUnzipped},
		{"LAMBDA_CREATE_FUNCTION_REQUEST_SIZE", &s.CreateFunctionRequestSize},
	} {
		if v, err := strconv.ParseInt(os.Getenv(e.env), 10, 64); err == nil && v > 0 {
			*e.dst = v
		}
	}

	if b, err := strconv.ParseBool(os.Getenv("LAMBDA_STRICT_RUNTIME_VALIDATION")); err == nil {
		s.StrictRuntimeValidation = b
	}

	return s
}
