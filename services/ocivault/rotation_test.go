package ocivault_test

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/oracle/oci-go-sdk/v65/secrets"
	"github.com/oracle/oci-go-sdk/v65/vault"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errTargetUnreachable = errors.New("target unreachable")

const rotFn = "ocid1.fnfunc.oc1..rotator"

type stepLog struct {
	steps []string
	mu    sync.Mutex
}

func (l *stepLog) add(s string) {
	l.mu.Lock()
	l.steps = append(l.steps, s)
	l.mu.Unlock()
}

func (l *stepLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	return append([]string(nil), l.steps...)
}

type rotationPayload struct {
	SecretID string `json:"secretId"`
	Step     string `json:"step"`
	Current  int64  `json:"currentVersionNumber"`
}

func fnTarget() vault.TargetSystemDetails {
	return vault.FunctionTargetSystemDetails{FunctionId: new(rotFn)}
}

func passGen() vault.PassphraseGenerationContext {
	return vault.PassphraseGenerationContext{
		GenerationTemplate: vault.PassphraseGenerationContextGenerationTemplateSecretsDefaultPassword,
		PassphraseLength:   new(20),
	}
}

func (se *secretEnv) rotating(t *testing.T, name string, cfg vault.RotationConfig, auto bool) vault.Secret {
	t.Helper()

	return se.create(t, name, "v1", func(d *vault.CreateSecretDetails) {
		d.RotationConfig = &cfg
		d.SecretContent = content("v1")

		if auto {
			g := passGen()
			d.SecretGenerationContext = g
			d.EnableAutoGeneration = new(true)
			d.SecretContent = nil
		}
	})
}

func (se *secretEnv) get(t *testing.T, id *string) vault.Secret {
	t.Helper()

	r, err := se.e.vsecret.GetSecret(t.Context(), vault.GetSecretRequest{SecretId: id})
	require.NoError(t, err)

	return r.Secret
}

func TestRotateSecretStages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		target    vault.TargetSystemDetails
		name      string
		wantSteps []string
		fnCreates bool
	}{
		{
			name: "function target, service creates pending", target: fnTarget(),
			wantSteps: []string{
				"VERIFY_CONNECTION", "CREATE_PENDING_VERSION", "UPDATE_TARGET_SYSTEM", "PROMOTE_PENDING_VERSION",
			},
		},
		{
			name: "function target, function creates pending", target: fnTarget(), fnCreates: true,
			wantSteps: []string{
				"VERIFY_CONNECTION", "CREATE_PENDING_VERSION", "UPDATE_TARGET_SYSTEM", "PROMOTE_PENDING_VERSION",
			},
		},
		{name: "adb target", target: vault.AdbTargetSystemDetails{AdbId: new("ocid1.autonomousdatabase.oc1..adb")}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := newSecretEnv(t)
			log := &stepLog{}

			var s vault.Secret

			se.e.backend.SetFunctionInvoker(func(ctx context.Context, fnID string, body []byte) error {
				var p rotationPayload
				require.NoError(t, json.Unmarshal(body, &p))
				assert.Equal(t, rotFn, fnID)
				assert.Equal(t, *s.Id, p.SecretID)
				log.add(p.Step)

				if tt.fnCreates && p.Step == "CREATE_PENDING_VERSION" {
					_, err := se.e.vsecret.UpdateSecret(ctx, vault.UpdateSecretRequest{
						SecretId: s.Id,
						SecretContent: vault.Base64SecretContentDetails{
							Content: new(b64([]byte("from-fn"))), Stage: vault.SecretContentDetailsStagePending,
						},
					})
					require.NoError(t, err)
				}

				return nil
			})

			s = se.rotating(t, "rot", vault.RotationConfig{
				TargetSystemDetails: tt.target, RotationInterval: new("P30D"),
			}, true)
			assert.EqualValues(t, 1, *s.CurrentVersionNumber)
			assert.Equal(t, vault.SecretRotationStatusNotEnabled, s.RotationStatus)
			assert.True(t, *s.IsAutoGenerationEnabled)

			_, err := se.e.vsecret.RotateSecret(t.Context(), vault.RotateSecretRequest{SecretId: s.Id})
			require.NoError(t, err)

			got := se.get(t, s.Id)
			assert.EqualValues(t, 2, *got.CurrentVersionNumber)
			require.NotNil(t, got.LastRotationTime)
			assert.Equal(t, vault.SecretRotationStatusNotEnabled, got.RotationStatus)

			assert.Equal(t, tt.wantSteps, log.get())

			b, err := se.bundle(t, s.Id, nil)
			require.NoError(t, err)

			if tt.fnCreates {
				assert.Equal(t, "from-fn", bundleText(t, b))
			} else {
				assert.NotEqual(t, "v1", bundleText(t, b))
			}

			prev, err := se.bundle(
				t,
				s.Id,
				func(r *secrets.GetSecretBundleRequest) { r.Stage = secrets.GetSecretBundleStagePrevious },
			)
			require.NoError(t, err)
			assert.EqualValues(t, 1, *prev.VersionNumber)
		})
	}
}

func TestRotationFailureKeepsCurrent(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	se.e.backend.SetFunctionInvoker(func(_ context.Context, _ string, body []byte) error {
		var p rotationPayload
		_ = json.Unmarshal(body, &p)

		if p.Step == "UPDATE_TARGET_SYSTEM" {
			return errTargetUnreachable
		}

		return nil
	})

	s := se.rotating(
		t,
		"rot",
		vault.RotationConfig{TargetSystemDetails: fnTarget(), RotationInterval: new("P30D")},
		true,
	)

	_, err := se.e.vsecret.RotateSecret(t.Context(), vault.RotateSecretRequest{SecretId: s.Id})
	require.NoError(t, err)

	got := se.get(t, s.Id)
	assert.EqualValues(t, 1, *got.CurrentVersionNumber, "a failed rotation must not promote")
	assert.Nil(t, got.LastRotationTime)

	vs, err := se.e.vsecret.ListSecretVersions(t.Context(), vault.ListSecretVersionsRequest{SecretId: s.Id})
	require.NoError(t, err)
	require.Len(t, vs.Items, 2)
	assert.Contains(t, vs.Items[1].Stages, vault.SecretVersionSummaryStagesPending)
}

func TestScheduledRotation(t *testing.T) {
	t.Parallel()

	se := newSecretEnv(t)
	log := &stepLog{}
	se.e.backend.SetFunctionInvoker(func(_ context.Context, _ string, body []byte) error {
		var p rotationPayload
		_ = json.Unmarshal(body, &p)
		log.add(p.Step)

		return nil
	})

	s := se.rotating(t, "sched", vault.RotationConfig{
		TargetSystemDetails: fnTarget(), RotationInterval: new("P2D"), IsScheduledRotationEnabled: new(true),
	}, true)
	assert.Equal(t, vault.SecretRotationStatusScheduled, s.RotationStatus)
	require.NotNil(t, s.NextRotationTime)
	assert.WithinDuration(t, se.e.clock.Now().Add(2*24*time.Hour), s.NextRotationTime.Time, time.Minute)

	se.e.clock.Advance(24 * time.Hour)
	assert.EqualValues(t, 1, *se.get(t, s.Id).CurrentVersionNumber)

	se.e.clock.Advance(25 * time.Hour)

	got := se.get(t, s.Id)
	assert.EqualValues(t, 2, *got.CurrentVersionNumber)
	assert.Equal(t, vault.SecretRotationStatusScheduled, got.RotationStatus)
	require.NotNil(t, got.LastRotationTime)
	assert.True(t, got.NextRotationTime.After(got.LastRotationTime.Time))
	assert.Len(t, log.get(), 4)

	_, err := se.e.vsecret.CancelSecretRotation(t.Context(), vault.CancelSecretRotationRequest{SecretId: s.Id})
	require.NoError(t, err)
	assert.Equal(t, vault.SecretRotationStatusCancelling, se.get(t, s.Id).RotationStatus)

	se.e.settle()
	assert.Equal(t, vault.SecretRotationStatusNotEnabled, se.get(t, s.Id).RotationStatus)

	se.e.clock.Advance(10 * 24 * time.Hour)
	assert.EqualValues(t, 2, *se.get(t, s.Id).CurrentVersionNumber)
	assert.Len(t, log.get(), 4)

	_, err = se.e.vsecret.CancelSecretRotation(t.Context(), vault.CancelSecretRotationRequest{SecretId: s.Id})
	assert.Equal(t, "IncorrectState", svcErr(t, err).GetCode())
}

func TestRotationValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mut  func(*vault.CreateSecretDetails)
		name string
	}{
		{func(d *vault.CreateSecretDetails) {
			d.RotationConfig = &vault.RotationConfig{TargetSystemDetails: fnTarget(), RotationInterval: new("PT1H")}
		}, "interval below one day"},
		{func(d *vault.CreateSecretDetails) {
			d.RotationConfig = &vault.RotationConfig{TargetSystemDetails: fnTarget(), RotationInterval: new("P361D")}
		}, "interval above 360 days"},
		{func(d *vault.CreateSecretDetails) {
			d.RotationConfig = &vault.RotationConfig{
				TargetSystemDetails: fnTarget(), IsScheduledRotationEnabled: new(true),
			}
		}, "scheduled without interval"},
		{func(d *vault.CreateSecretDetails) {
			d.RotationConfig = &vault.RotationConfig{
				TargetSystemDetails: vault.FunctionTargetSystemDetails{FunctionId: new("")},
			}
		}, "empty function id"},
		{func(d *vault.CreateSecretDetails) { d.EnableAutoGeneration = new(true) }, "auto generation without context"},
		{func(d *vault.CreateSecretDetails) {
			d.EnableAutoGeneration = new(true)
			d.SecretGenerationContext = passGen()
		}, "auto generation with content"},
		{func(d *vault.CreateSecretDetails) {
			d.SecretContent = nil
			d.EnableAutoGeneration = new(true)
			d.SecretGenerationContext = vault.PassphraseGenerationContext{
				GenerationTemplate: vault.PassphraseGenerationContextGenerationTemplateDbaasDefaultPassword,
				PassphraseLength:   new(8),
			}
		}, "db passphrase too short"},
		{func(d *vault.CreateSecretDetails) {
			d.SecretContent = nil
			d.EnableAutoGeneration = new(true)
			g := passGen()
			g.SecretTemplate = new("no placeholder")
			d.SecretGenerationContext = g
		}, "template without placeholder"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := newSecretEnv(t)
			d := vault.CreateSecretDetails{
				CompartmentId: new(testCompartment), VaultId: se.v.Id, KeyId: se.key.Id, SecretName: new("bad"),
				SecretContent: content("x"),
			}
			tt.mut(&d)

			_, err := se.e.vsecret.CreateSecret(t.Context(), vault.CreateSecretRequest{CreateSecretDetails: d})
			assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())
		})
	}

	se := newSecretEnv(t)
	plain := se.create(t, "plain", "x", nil)

	_, err := se.e.vsecret.RotateSecret(t.Context(), vault.RotateSecretRequest{SecretId: plain.Id})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode())

	fn := se.rotating(t, "needs-fn", vault.RotationConfig{TargetSystemDetails: fnTarget()}, false)

	_, err = se.e.vsecret.RotateSecret(t.Context(), vault.RotateSecretRequest{SecretId: fn.Id})
	assert.Equal(t, http.StatusBadRequest, svcErr(t, err).GetHTTPStatusCode(), "no invoker wired")
}

func TestGeneratedSecrets(t *testing.T) {
	t.Parallel()

	hasClass := strings.ContainsAny

	tests := []struct {
		gen   vault.SecretGenerationContext
		check func(t *testing.T, got string)
		name  string
	}{
		{
			name: "secrets passphrase",
			gen:  passGen(),
			check: func(t *testing.T, got string) {
				t.Helper()
				assert.Len(t, got, 20)
				assert.True(
					t,
					hasClass(got, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") && hasClass(got, "abcdefghijklmnopqrstuvwxyz") &&
						hasClass(got, "0123456789"),
				)
			},
		},
		{
			name: "db passphrase has no quotes",
			gen: vault.PassphraseGenerationContext{
				GenerationTemplate: vault.PassphraseGenerationContextGenerationTemplateDbaasDefaultPassword,
				PassphraseLength:   new(30),
			},
			check: func(t *testing.T, got string) {
				t.Helper()
				assert.Len(t, got, 30)
				assert.NotContains(t, got, `"`)
				assert.True(t, hasClass(got, "0123456789"))
			},
		},
		{
			name: "passphrase template",
			gen: vault.PassphraseGenerationContext{
				GenerationTemplate: vault.PassphraseGenerationContextGenerationTemplateSecretsDefaultPassword,
				SecretTemplate:     new(`{"user":"admin","password":"%GENERATED_PASSPHRASE%"}`),
			},
			check: func(t *testing.T, got string) {
				t.Helper()

				var m map[string]string
				require.NoError(t, json.Unmarshal([]byte(got), &m))
				assert.Equal(t, "admin", m["user"])
				assert.Len(t, m["password"], 16)
			},
		},
		{
			name: "bytes",
			gen:  vault.BytesGenerationContext{GenerationTemplate: vault.BytesGenerationContextGenerationTemplate512},
			check: func(t *testing.T, got string) {
				t.Helper()
				assert.Len(t, got, 512)
			},
		},
		{
			name: "ssh key",
			gen: vault.SshKeyGenerationContext{
				GenerationTemplate: vault.SshKeyGenerationContextGenerationTemplate2048,
			},
			check: func(t *testing.T, got string) {
				t.Helper()

				var m map[string]string
				require.NoError(t, json.Unmarshal([]byte(got), &m))

				pub, _ := pem.Decode([]byte(m["publicKey"]))
				require.NotNil(t, pub)
				_, err := x509.ParsePKIXPublicKey(pub.Bytes)
				require.NoError(t, err)

				priv, _ := pem.Decode([]byte(m["privateKey"]))
				require.NotNil(t, priv)
				_, err = x509.ParsePKCS8PrivateKey(priv.Bytes)
				require.NoError(t, err)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			se := newSecretEnv(t)
			s := se.create(t, "gen", "", func(d *vault.CreateSecretDetails) {
				d.SecretContent = nil
				d.SecretGenerationContext = tt.gen
				d.EnableAutoGeneration = new(true)
			})
			assert.EqualValues(t, 1, *s.CurrentVersionNumber)
			require.NotNil(t, s.SecretGenerationContext)

			b, err := se.bundle(t, s.Id, nil)
			require.NoError(t, err)
			tt.check(t, bundleText(t, b))
		})
	}
}
