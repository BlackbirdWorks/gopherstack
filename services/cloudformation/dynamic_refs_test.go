package cloudformation_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudformation"
	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
	"github.com/blackbirdworks/gopherstack/services/ssm"
)

var (
	errStubParamNotFound  = errors.New("parameter not found")
	errStubSecretNotFound = errors.New("secret not found")
)

// ---- stub DynamicRefResolver ------------------------------------------------

// stubResolver is a test-double that returns predefined values or errors.
type stubResolver struct {
	params  map[string]string
	secrets map[string]string
	ssmErr  error
	smErr   error
}

func (s *stubResolver) ResolveSSMParameter(name string) (string, error) {
	if s.ssmErr != nil {
		return "", s.ssmErr
	}

	if v, ok := s.params[name]; ok {
		return v, nil
	}

	return "", fmt.Errorf("%w: %s", errStubParamNotFound, name)
}

func (s *stubResolver) ResolveSSMSecureParameter(name string) (string, error) {
	return s.ResolveSSMParameter(name)
}

func (s *stubResolver) ResolveSecret(secretID, jsonKey string) (string, error) {
	if s.smErr != nil {
		return "", s.smErr
	}

	v, ok := s.secrets[secretID]
	if !ok {
		return "", fmt.Errorf("%w: %s", errStubSecretNotFound, secretID)
	}

	if jsonKey == "" {
		return v, nil
	}

	// For the stub, just return the raw value; JSON extraction is tested via real resolver.
	return v, nil
}

// ---- ResolveDynamicRefsInTemplate unit tests --------------------------------

func TestResolveDynamicRefsInTemplate_SSM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func() *cloudformation.Template
		name     string
		resolver cloudformation.DynamicRefResolver
		wantProp string
		wantErr  bool
	}{
		{
			name: "ssm_resolved",
			setup: func() *cloudformation.Template {
				tmpl, _ := cloudformation.ParseTemplate(`{
					"Resources": {
						"MyQueue": {
							"Type": "AWS::SQS::Queue",
							"Properties": {
								"QueueName": "{{resolve:ssm:/app/queue-name}}"
							}
						}
					}
				}`)

				return tmpl
			},
			resolver: &stubResolver{params: map[string]string{"/app/queue-name": "my-queue"}},
			wantProp: "my-queue",
		},
		{
			name: "ssm_with_version_ignored",
			setup: func() *cloudformation.Template {
				tmpl, _ := cloudformation.ParseTemplate(`{
					"Resources": {
						"R": {
							"Type": "AWS::SQS::Queue",
							"Properties": {
								"QueueName": "{{resolve:ssm:/app/q:3}}"
							}
						}
					}
				}`)

				return tmpl
			},
			resolver: &stubResolver{params: map[string]string{"/app/q": "versioned-queue"}},
			wantProp: "versioned-queue",
		},
		{
			name: "ssm_not_found",
			setup: func() *cloudformation.Template {
				tmpl, _ := cloudformation.ParseTemplate(`{
					"Resources": {
						"R": {
							"Type": "AWS::SQS::Queue",
							"Properties": {
								"QueueName": "{{resolve:ssm:/missing}}"
							}
						}
					}
				}`)

				return tmpl
			},
			resolver: &stubResolver{params: map[string]string{}},
			wantErr:  true,
		},
		{
			name: "nil_resolver_noop",
			setup: func() *cloudformation.Template {
				tmpl, _ := cloudformation.ParseTemplate(`{
					"Resources": {
						"R": {
							"Type": "AWS::SQS::Queue",
							"Properties": {
								"QueueName": "{{resolve:ssm:/param}}"
							}
						}
					}
				}`)

				return tmpl
			},
			resolver: nil,
			wantProp: "{{resolve:ssm:/param}}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpl := tt.setup()
			err := cloudformation.ResolveDynamicRefsInTemplate(tmpl, tt.resolver)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, cloudformation.ErrDynamicRefFailed)

				return
			}

			require.NoError(t, err)

			if tt.wantProp != "" {
				for _, res := range tmpl.Resources {
					assert.Equal(t, tt.wantProp, res.Properties["QueueName"])
				}
			}
		})
	}
}

func TestResolveDynamicRefsInTemplate_SecretsManager(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func() *cloudformation.Template
		resolver cloudformation.DynamicRefResolver
		name     string
		wantProp string
		wantErr  bool
	}{
		{
			name: "secretsmanager_full_secret",
			setup: func() *cloudformation.Template {
				tmpl, _ := cloudformation.ParseTemplate(`{
					"Resources": {
						"R": {
							"Type": "AWS::S3::Bucket",
							"Properties": {
								"BucketName": "{{resolve:secretsmanager:my-secret}}"
							}
						}
					}
				}`)

				return tmpl
			},
			resolver: &stubResolver{secrets: map[string]string{"my-secret": "secret-value"}},
			wantProp: "secret-value",
		},
		{
			name: "secretsmanager_not_found",
			setup: func() *cloudformation.Template {
				tmpl, _ := cloudformation.ParseTemplate(`{
					"Resources": {
						"R": {
							"Type": "AWS::S3::Bucket",
							"Properties": {
								"BucketName": "{{resolve:secretsmanager:missing-secret}}"
							}
						}
					}
				}`)

				return tmpl
			},
			resolver: &stubResolver{secrets: map[string]string{}},
			wantErr:  true,
		},
		{
			name: "unsupported_service",
			setup: func() *cloudformation.Template {
				tmpl, _ := cloudformation.ParseTemplate(`{
					"Resources": {
						"R": {
							"Type": "AWS::S3::Bucket",
							"Properties": {
								"BucketName": "{{resolve:dynamodb:TableName}}"
							}
						}
					}
				}`)

				return tmpl
			},
			resolver: &stubResolver{},
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tmpl := tt.setup()
			err := cloudformation.ResolveDynamicRefsInTemplate(tmpl, tt.resolver)

			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, cloudformation.ErrDynamicRefFailed)

				return
			}

			require.NoError(t, err)

			if tt.wantProp != "" {
				for _, res := range tmpl.Resources {
					assert.Equal(t, tt.wantProp, res.Properties["BucketName"])
				}
			}
		})
	}
}

// ---- CreateStack with dynamic refs (integration with real backends) ---------

func newBackendWithSSMAndSM(t *testing.T) (
	*cloudformation.InMemoryBackend,
	*ssm.InMemoryBackend,
	*secretsmanager.InMemoryBackend,
) {
	t.Helper()

	ssmBackend := ssm.NewInMemoryBackend()
	smBackend := secretsmanager.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	ssmHandler := ssm.NewHandler(ssmBackend)
	smHandler := secretsmanager.NewHandler(smBackend)

	backends := &cloudformation.ServiceBackends{
		SSM:            ssmHandler,
		SecretsManager: smHandler,
		AccountID:      "000000000000",
		Region:         "us-east-1",
	}

	creator := cloudformation.NewResourceCreator(backends)
	cfnBackend := cloudformation.NewInMemoryBackendWithConfig("000000000000", "us-east-1", creator)

	return cfnBackend, ssmBackend, smBackend
}

func TestBackend_CreateStack_DynamicRefs_SSM(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupSSM   func(b *ssm.InMemoryBackend)
		name       string
		template   string
		wantStatus string
		wantReason string
	}{
		{
			name: "ssm_ref_resolved",
			setupSSM: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(&ssm.PutParameterInput{
					Name:  "/app/queue-name",
					Type:  "String",
					Value: "my-resolved-queue",
				})
			},
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Q": {
						"Type": "AWS::SQS::Queue",
						"Properties": {
							"QueueName": "{{resolve:ssm:/app/queue-name}}"
						}
					}
				}
			}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name: "ssm_secure_ref_resolved",
			setupSSM: func(b *ssm.InMemoryBackend) {
				_, _ = b.PutParameter(&ssm.PutParameterInput{
					Name:  "/app/secret-param",
					Type:  ssm.SecureStringType,
					Value: "super-secret-value",
				})
			},
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Q": {
						"Type": "AWS::SQS::Queue",
						"Properties": {
							"QueueName": "{{resolve:ssm-secure:/app/secret-param}}"
						}
					}
				}
			}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:     "ssm_ref_missing",
			setupSSM: func(_ *ssm.InMemoryBackend) {},
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Q": {
						"Type": "AWS::SQS::Queue",
						"Properties": {
							"QueueName": "{{resolve:ssm:/missing/param}}"
						}
					}
				}
			}`,
			wantStatus: "CREATE_FAILED",
			wantReason: "dynamic reference resolution failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfnBackend, ssmBackend, _ := newBackendWithSSMAndSM(t)
			tt.setupSSM(ssmBackend)

			stack, err := cfnBackend.CreateStack(t.Context(), "test-stack", tt.template, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, stack.StackStatus)

			if tt.wantReason != "" {
				assert.Contains(t, stack.StackStatusReason, tt.wantReason)
			}
		})
	}
}

func TestBackend_CreateStack_DynamicRefs_SecretsManager(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupSM    func(b *secretsmanager.InMemoryBackend)
		name       string
		template   string
		wantStatus string
		wantReason string
	}{
		{
			name: "secretsmanager_ref_resolved",
			setupSM: func(b *secretsmanager.InMemoryBackend) {
				_, _ = b.CreateSecret(&secretsmanager.CreateSecretInput{
					Name:         "my-db-secret",
					SecretString: "db-password-value",
				})
			},
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Q": {
						"Type": "AWS::SQS::Queue",
						"Properties": {
							"QueueName": "{{resolve:secretsmanager:my-db-secret}}"
						}
					}
				}
			}`,
			wantStatus: "CREATE_COMPLETE",
		},
		{
			name:    "secretsmanager_missing",
			setupSM: func(_ *secretsmanager.InMemoryBackend) {},
			template: `{
				"AWSTemplateFormatVersion": "2010-09-09",
				"Resources": {
					"Q": {
						"Type": "AWS::SQS::Queue",
						"Properties": {
							"QueueName": "{{resolve:secretsmanager:nonexistent}}"
						}
					}
				}
			}`,
			wantStatus: "CREATE_FAILED",
			wantReason: "dynamic reference resolution failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfnBackend, _, smBackend := newBackendWithSSMAndSM(t)
			tt.setupSM(smBackend)

			stack, err := cfnBackend.CreateStack(t.Context(), "test-stack", tt.template, nil, nil)
			require.NoError(t, err)
			assert.Equal(t, tt.wantStatus, stack.StackStatus)

			if tt.wantReason != "" {
				assert.Contains(t, stack.StackStatusReason, tt.wantReason)
			}
		})
	}
}

func TestBackend_CreateStack_DynamicRefs_StackEvents(t *testing.T) {
	t.Parallel()

	cfnBackend, _, _ := newBackendWithSSMAndSM(t)

	template := `{
		"AWSTemplateFormatVersion": "2010-09-09",
		"Resources": {
			"Q": {
				"Type": "AWS::SQS::Queue",
				"Properties": {
					"QueueName": "{{resolve:ssm:/missing/param}}"
				}
			}
		}
	}`

	stack, err := cfnBackend.CreateStack(t.Context(), "failing-stack", template, nil, nil)
	require.NoError(t, err)
	assert.Equal(t, "CREATE_FAILED", stack.StackStatus)

	events, err := cfnBackend.DescribeStackEvents(stack.StackName)
	require.NoError(t, err)
	require.NotEmpty(t, events)

	// At least one event should record CREATE_FAILED with the dynamic ref reason.
	var found bool

	for _, ev := range events {
		if ev.ResourceStatus == "CREATE_FAILED" && ev.ResourceStatusReason != "" {
			found = true

			break
		}
	}

	assert.True(t, found, "expected a CREATE_FAILED event with a reason")
}

func TestNewDynamicRefResolver_NilBackends(t *testing.T) {
	t.Parallel()

	resolver := cloudformation.NewDynamicRefResolver(nil)
	assert.Nil(t, resolver)
}

func TestNewDynamicRefResolver_NoSSMOrSM(t *testing.T) {
	t.Parallel()

	backends := &cloudformation.ServiceBackends{}
	resolver := cloudformation.NewDynamicRefResolver(backends)
	require.NotNil(t, resolver)

	_, err := resolver.ResolveSSMParameter("/some/param")
	require.Error(t, err)
	require.ErrorIs(t, err, cloudformation.ErrDynamicRefFailed)

	_, err = resolver.ResolveSecret("some-secret", "")
	require.Error(t, err)
	require.ErrorIs(t, err, cloudformation.ErrDynamicRefFailed)
}

func TestNewDynamicRefResolver_RealSSM(t *testing.T) {
	t.Parallel()

	ssmBackend := ssm.NewInMemoryBackend()
	_, _ = ssmBackend.PutParameter(&ssm.PutParameterInput{
		Name:  "/test/param",
		Type:  "String",
		Value: "hello",
	})

	_, _ = ssmBackend.PutParameter(&ssm.PutParameterInput{
		Name:  "/test/secure",
		Type:  ssm.SecureStringType,
		Value: "secret-val",
	})

	backends := &cloudformation.ServiceBackends{
		SSM: ssm.NewHandler(ssmBackend),
	}

	resolver := cloudformation.NewDynamicRefResolver(backends)
	require.NotNil(t, resolver)

	tests := []struct {
		name    string
		call    func() (string, error)
		want    string
		wantErr bool
	}{
		{
			name: "plain_param",
			call: func() (string, error) { return resolver.ResolveSSMParameter("/test/param") },
			want: "hello",
		},
		{
			name: "secure_param",
			call: func() (string, error) { return resolver.ResolveSSMSecureParameter("/test/secure") },
			want: "secret-val",
		},
		{
			name:    "missing_param",
			call:    func() (string, error) { return resolver.ResolveSSMParameter("/not/there") },
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := tt.call()

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNewDynamicRefResolver_RealSecretsManager(t *testing.T) {
	t.Parallel()

	smBackend := secretsmanager.NewInMemoryBackendWithConfig("us-east-1", "000000000000")
	_, _ = smBackend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "my-secret",
		SecretString: "top-secret",
	})

	_, _ = smBackend.CreateSecret(&secretsmanager.CreateSecretInput{
		Name:         "json-secret",
		SecretString: `{"password":"p@ss","user":"admin"}`,
	})

	backends := &cloudformation.ServiceBackends{
		SecretsManager: secretsmanager.NewHandler(smBackend),
	}

	resolver := cloudformation.NewDynamicRefResolver(backends)
	require.NotNil(t, resolver)

	tests := []struct {
		name     string
		secretID string
		jsonKey  string
		want     string
		wantErr  bool
	}{
		{
			name:     "full_secret",
			secretID: "my-secret",
			want:     "top-secret",
		},
		{
			name:     "json_key_extraction",
			secretID: "json-secret",
			jsonKey:  "password",
			want:     "p@ss",
		},
		{
			name:     "json_key_missing",
			secretID: "json-secret",
			jsonKey:  "nonexistent-key",
			wantErr:  true,
		},
		{
			name:     "secret_not_found",
			secretID: "no-such-secret",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := resolver.ResolveSecret(tt.secretID, tt.jsonKey)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
