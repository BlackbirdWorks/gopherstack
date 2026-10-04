package lambda_test

import (
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// TestGetFunctionConcurrentWithTagResource proves GetFunction/ListFunctions
// must not hand back the live pointer TagResource/UntagResource mutate.
func TestGetFunctionConcurrentWithTagResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reader func(bk *lambda.InMemoryBackend, fnName string)
		name   string
	}{
		{
			name: "GetFunction races tag writer",
			reader: func(bk *lambda.InMemoryBackend, fnName string) {
				fn, err := bk.GetFunction(fnName)
				if err != nil {
					return
				}

				_ = fn.Description
				_ = len(fn.Tags)
			},
		},
		{
			name: "ListFunctions races tag writer",
			reader: func(bk *lambda.InMemoryBackend, _ string) {
				p := bk.ListFunctions("", 0)
				for _, fn := range p.Data {
					_ = fn.Description
					_ = len(fn.Tags)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			fnName := "race-fn"
			createFunctionForTest(t, h, fnName)

			const iterations = 300

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					tt.reader(bk, fnName)
				}
			}()

			go func() {
				defer wg.Done()

				for i := range iterations {
					if i%2 == 0 {
						_ = bk.TagResource(fnName, map[string]string{"k": "v"})
					} else {
						_ = bk.UntagResource(fnName, []string{"k"})
					}
				}
			}()

			wg.Wait()
		})
	}
}

// TestEventSourceMappingConcurrentWithUpdate proves Get/ListEventSourceMappings
// must not hand back the live pointer UpdateEventSourceMapping and sweepESMs mutate.
func TestEventSourceMappingConcurrentWithUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reader func(bk *lambda.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "GetEventSourceMapping races update",
			reader: func(bk *lambda.InMemoryBackend, id string) {
				m, err := bk.GetEventSourceMapping(id)
				if err != nil {
					return
				}

				_ = m.State
				_ = m.BatchSize
				_ = m.LastProcessingResult
			},
		},
		{
			name: "ListEventSourceMappings races update",
			reader: func(bk *lambda.InMemoryBackend, _ string) {
				p := bk.ListEventSourceMappings("", "", "", 0)
				for _, m := range p.Data {
					_ = m.State
					_ = m.BatchSize
					_ = m.LastProcessingResult
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, bk := newInMemoryHandler(t)

			require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "esm-race-fn"}))

			created, err := bk.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/race-stream",
				FunctionName:   "esm-race-fn",
				Enabled:        true,
			})
			require.NoError(t, err)

			const iterations = 300

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					tt.reader(bk, created.UUID)
				}
			}()

			go func() {
				defer wg.Done()

				for i := range iterations {
					enabled := i%2 == 0
					batchSize := int32(10 + i%50)

					_, _ = bk.UpdateEventSourceMapping(created.UUID, &lambda.UpdateEventSourceMappingInput{
						Enabled:   &enabled,
						BatchSize: &batchSize,
					})
				}
			}()

			wg.Wait()
		})
	}
}

// TestCodeSigningConfigConcurrentWithUpdate proves Get/ListCodeSigningConfigs
// must not hand back the live pointer UpdateCodeSigningConfig mutates.
func TestCodeSigningConfigConcurrentWithUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reader func(bk *lambda.InMemoryBackend, arn string)
		name   string
	}{
		{
			name: "GetCodeSigningConfig races update",
			reader: func(bk *lambda.InMemoryBackend, arn string) {
				cfg, err := bk.GetCodeSigningConfig(arn)
				if err != nil {
					return
				}

				_ = cfg.Description
			},
		},
		{
			name: "ListCodeSigningConfigs races update",
			reader: func(bk *lambda.InMemoryBackend, _ string) {
				p := bk.ListCodeSigningConfigs("", 0)
				for _, cfg := range p.Data {
					_ = cfg.Description
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, bk := newInMemoryHandler(t)

			created, err := bk.CreateCodeSigningConfig(&lambda.CreateCodeSigningConfigInput{})
			require.NoError(t, err)

			const iterations = 300

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					tt.reader(bk, created.CodeSigningConfigArn)
				}
			}()

			go func() {
				defer wg.Done()

				for i := range iterations {
					desc := "desc-" + strconv.Itoa(i)

					_, _ = bk.UpdateCodeSigningConfig(
						created.CodeSigningConfigArn,
						&lambda.UpdateCodeSigningConfigInput{
							Description: &desc,
						},
					)
				}
			}()

			wg.Wait()
		})
	}
}

// TestAliasConcurrentWithUpdate proves GetAlias/ListAliases must not hand
// back the live pointer UpdateAlias mutates.
func TestAliasConcurrentWithUpdate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		reader func(bk *lambda.InMemoryBackend, fnName, aliasName string)
		name   string
	}{
		{
			name: "GetAlias races update",
			reader: func(bk *lambda.InMemoryBackend, fnName, aliasName string) {
				alias, err := bk.GetAlias(fnName, aliasName)
				if err != nil {
					return
				}

				_ = alias.Description
				_ = alias.RevisionID
			},
		},
		{
			name: "ListAliases races update",
			reader: func(bk *lambda.InMemoryBackend, fnName, _ string) {
				p, err := bk.ListAliases(fnName, "", "", 0)
				if err != nil {
					return
				}

				for _, alias := range p.Data {
					_ = alias.Description
					_ = alias.RevisionID
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			fnName := "alias-race-fn"
			createFunctionForTest(t, h, fnName)

			_, err := bk.CreateAlias(fnName, &lambda.CreateAliasInput{
				Name:            "race-alias",
				FunctionVersion: "$LATEST",
			})
			require.NoError(t, err)

			const iterations = 300

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()

				for range iterations {
					tt.reader(bk, fnName, "race-alias")
				}
			}()

			go func() {
				defer wg.Done()

				for i := range iterations {
					desc := "desc-" + strconv.Itoa(i)

					_, _ = bk.UpdateAlias(fnName, "race-alias", &lambda.UpdateAliasInput{
						Description: &desc,
					})
				}
			}()

			wg.Wait()
		})
	}
}
