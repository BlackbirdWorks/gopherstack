package lambda_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

func TestLambda_CreateFunction_DefaultsArchitectures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		inputArchs []string
		wantArchs  []string
	}{
		{
			name:       "empty_architectures_defaults_to_x86_64",
			inputArchs: nil,
			wantArchs:  []string{"x86_64"},
		},
		{
			name:       "arm64_preserved",
			inputArchs: []string{"arm64"},
			wantArchs:  []string{"arm64"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName:  "test-fn-" + tt.name,
				Architectures: tt.inputArchs,
			}))

			got, err := backend.GetFunction("test-fn-" + tt.name)
			require.NoError(t, err)
			assert.Equal(t, tt.wantArchs, got.Architectures)
		})
	}
}

func TestLambda_CreateFunction_DefaultsEphemeralStorage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    *lambda.EphemeralStorageConfig
		name     string
		wantSize int32
	}{
		{
			name:     "nil_ephemeral_storage_defaults_to_512",
			input:    nil,
			wantSize: 512,
		},
		{
			name:     "custom_size_preserved",
			input:    &lambda.EphemeralStorageConfig{Size: 1024},
			wantSize: 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName:     "ephemeral-fn-" + tt.name,
				EphemeralStorage: tt.input,
			}))

			got, err := backend.GetFunction("ephemeral-fn-" + tt.name)
			require.NoError(t, err)
			require.NotNil(t, got.EphemeralStorage)
			assert.Equal(t, tt.wantSize, got.EphemeralStorage.Size)
		})
	}
}

func TestLambda_CreateFunction_MemorySizeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		memorySize int
		wantErr    bool
	}{
		{
			name:       "zero_allowed",
			memorySize: 0,
			wantErr:    false,
		},
		{
			name:       "128_valid",
			memorySize: 128,
			wantErr:    false,
		},
		{
			name:       "10240_valid",
			memorySize: 10240,
			wantErr:    false,
		},
		{
			name:       "256_valid",
			memorySize: 256,
			wantErr:    false,
		},
		{
			name:       "127_invalid_below_min",
			memorySize: 127,
			wantErr:    true,
		},
		{
			name:       "10241_invalid_above_max",
			memorySize: 10241,
			wantErr:    true,
		},
		{
			name:       "129_invalid_not_divisible_by_64",
			memorySize: 129,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			err := backend.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName: "mem-fn-" + tt.name,
				MemorySize:   tt.memorySize,
			})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLambda_CreateESM_SetsLastProcessingResult(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		eventSourceARN string
		wantResult     string
	}{
		{
			name:           "new_esm_has_no_records_processed",
			eventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			wantResult:     "No records processed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName: "esm-fn-" + tt.name,
			}))

			esm, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN: tt.eventSourceARN,
				FunctionName:   "esm-fn-" + tt.name,
				Enabled:        true,
			})

			require.NoError(t, err)
			require.NotNil(t, esm)
			assert.Equal(t, tt.wantResult, esm.LastProcessingResult)
		})
	}
}

func TestLambda_FunctionURL_InvokeMode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		inputInvMode   string
		wantInvokeMode string
	}{
		{
			name:           "empty_invoke_mode_defaults_to_buffered",
			inputInvMode:   "",
			wantInvokeMode: "BUFFERED",
		},
		{
			name:           "response_stream_preserved",
			inputInvMode:   "RESPONSE_STREAM",
			wantInvokeMode: "RESPONSE_STREAM",
		},
		{
			name:           "buffered_explicit",
			inputInvMode:   "BUFFERED",
			wantInvokeMode: "BUFFERED",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			fnName := "url-fn-" + tt.name
			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName: fnName,
			}))

			cfg, err := backend.CreateFunctionURLConfig(t.Context(), fnName, "NONE", nil, tt.inputInvMode)
			require.NoError(t, err)
			require.NotNil(t, cfg)
			assert.Equal(t, tt.wantInvokeMode, cfg.InvokeMode)
		})
	}
}

func TestLambda_Alias_RoutingConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		routingConfig *lambda.AliasRoutingConfig
		wantWeights   map[string]float64
		name          string
		aliasName     string
	}{
		{
			name:      "create_alias_with_routing_config",
			aliasName: "live",
			routingConfig: &lambda.AliasRoutingConfig{
				AdditionalVersionWeights: map[string]float64{
					"2": 0.1,
				},
			},
			wantWeights: map[string]float64{"2": 0.1},
		},
		{
			name:          "create_alias_without_routing_config",
			aliasName:     "staging",
			routingConfig: nil,
			wantWeights:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			fnName := "routing-fn-" + tt.name
			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{
				FunctionName: fnName,
			}))

			alias, err := backend.CreateAlias(fnName, &lambda.CreateAliasInput{
				Name:            tt.aliasName,
				FunctionVersion: "$LATEST",
				RoutingConfig:   tt.routingConfig,
			})
			require.NoError(t, err)
			require.NotNil(t, alias)

			got, err := backend.GetAlias(fnName, tt.aliasName)
			require.NoError(t, err)

			if tt.wantWeights == nil {
				assert.Nil(t, got.RoutingConfig)
			} else {
				require.NotNil(t, got.RoutingConfig)
				assert.Equal(t, tt.wantWeights, got.RoutingConfig.AdditionalVersionWeights)
			}
		})
	}
}
