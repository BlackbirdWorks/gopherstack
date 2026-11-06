package gcpcloudbuild_test

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"

	"cloud.google.com/go/cloudbuild/apiv1/v2/cloudbuildpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/gcpcloudbuild"
)

const kmsKey = "projects/p/locations/global/keyRings/r/cryptoKeys/k"

// fakeKMS decrypts "enc:<plaintext>" ciphertexts under kmsKey.
type fakeKMS struct{}

var errBadCiphertext = errors.New("decryption failed")

func (fakeKMS) Decrypt(key string, ct []byte) ([]byte, error) {
	pt, ok := strings.CutPrefix(string(ct), "enc:")
	if key != kmsKey || !ok {
		return nil, errBadCiphertext
	}

	return []byte(pt), nil
}

func TestKMSSecrets(t *testing.T) {
	t.Parallel()

	inline := func(key string, env map[string][]byte) *cloudbuildpb.Secrets {
		return &cloudbuildpb.Secrets{Inline: []*cloudbuildpb.InlineSecret{{KmsKeyName: key, EnvMap: env}}}
	}

	tests := []struct {
		mut     func(*cloudbuildpb.Build)
		name    string
		wantErr string
		wantEnv string
		noKMS   bool
	}{
		{
			name: "inline",
			mut: func(b *cloudbuildpb.Build) {
				b.AvailableSecrets = inline(kmsKey, map[string][]byte{"TOKEN": []byte("enc:s3cret")})
			},
			wantEnv: "TOKEN=s3cret",
		},
		{
			name: "deprecated secrets field",
			mut: func(b *cloudbuildpb.Build) {
				b.Secrets = []*cloudbuildpb.Secret{
					{KmsKeyName: kmsKey, SecretEnv: map[string][]byte{"TOKEN": []byte("enc:old")}},
				}
			},
			wantEnv: "TOKEN=old",
		},
		{
			name: "secret manager and inline together",
			mut: func(b *cloudbuildpb.Build) {
				b.AvailableSecrets = inline(kmsKey, map[string][]byte{"TOKEN": []byte("enc:s3cret")})
				b.AvailableSecrets.SecretManager = []*cloudbuildpb.SecretManagerSecret{
					{VersionName: "projects/p/secrets/s/versions/1", Env: "OTHER"},
				}
			},
			wantEnv: "OTHER=hunter2",
		},
		{
			name: "wrong key",
			mut: func(b *cloudbuildpb.Build) {
				b.AvailableSecrets = inline("projects/p/locations/global/keyRings/r/cryptoKeys/other",
					map[string][]byte{"TOKEN": []byte("enc:x")})
			},
			wantErr: "decrypt TOKEN",
		},
		{
			name: "bad ciphertext",
			mut: func(b *cloudbuildpb.Build) {
				b.AvailableSecrets = inline(kmsKey, map[string][]byte{"TOKEN": []byte("garbage")})
			},
			wantErr: "decryption failed",
		},
		{
			name: "duplicate env",
			mut: func(b *cloudbuildpb.Build) {
				b.AvailableSecrets = inline(kmsKey, map[string][]byte{"TOKEN": []byte("enc:a")})
				b.Secrets = []*cloudbuildpb.Secret{
					{KmsKeyName: kmsKey, SecretEnv: map[string][]byte{"TOKEN": []byte("enc:b")}},
				}
			},
			wantErr: "defined more than once",
		},
		{
			name:  "no kms",
			noKMS: true,
			mut: func(b *cloudbuildpb.Build) {
				b.AvailableSecrets = inline(kmsKey, map[string][]byte{"TOKEN": []byte("enc:x")})
			},
			wantErr: "cloud KMS is not available",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, map[string]behavior{"x": func(_ context.Context, spec container.Spec, out io.Writer) int {
				for _, kv := range spec.Env {
					_, _ = io.WriteString(out, "env "+kv+"\n")
				}

				return 0
			}})

			if !tt.noKMS {
				e.configure(func(s *gcpcloudbuild.Services) { s.KMS = fakeKMS{} })
			}

			b := buildOf(step("x"))
			tt.mut(b)
			b.Steps[0].SecretEnv = definedSecretEnvs(b)

			got := e.run(t, b)

			if tt.wantErr != "" {
				assert.Equal(t, cloudbuildpb.Build_FAILURE, got.GetStatus())
				assert.Contains(t, got.GetStatusDetail(), tt.wantErr)
				assert.Empty(t, e.rt.startedImages())

				return
			}

			require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())
			assert.Contains(t, e.rt.specOf("x").Env, tt.wantEnv)

			for _, l := range e.logs.lines() {
				assert.NotContains(t, l, "s3cret")
				assert.NotContains(t, l, "hunter2")
			}
		})
	}
}

func TestAutomapSubstitutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		want    []string
		notWant []string
		automap bool
	}{
		{
			name: "enabled", automap: true,
			want: []string{"_TARGET=prod", "PROJECT_ID=" + project, "LOCATION=global", "OVERRIDE=step"},
		},
		{name: "disabled", notWant: []string{"_TARGET=prod", "PROJECT_ID=" + project}, want: []string{"OVERRIDE=step"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t, map[string]behavior{"img": exits(0)})

			b := buildOf(step("img", func(s *cloudbuildpb.BuildStep) { s.Env = []string{"OVERRIDE=step"} }))
			b.Substitutions = map[string]string{"_TARGET": "prod", "_OVERRIDE": "sub"}
			b.Options = &cloudbuildpb.BuildOptions{
				AutomapSubstitutions: tt.automap, Env: []string{"OVERRIDE=options"},
			}

			got := e.run(t, b)
			require.Equal(t, cloudbuildpb.Build_SUCCESS, got.GetStatus(), got.GetStatusDetail())

			env := e.rt.specOf("img").Env
			for _, w := range tt.want {
				assert.Contains(t, env, w)
			}

			for _, n := range tt.notWant {
				assert.NotContains(t, env, n)
			}
		})
	}
}

func definedSecretEnvs(b *cloudbuildpb.Build) []string {
	seen := map[string]bool{}

	for _, s := range b.GetAvailableSecrets().GetSecretManager() {
		seen[s.GetEnv()] = true
	}

	for _, s := range b.GetAvailableSecrets().GetInline() {
		for env := range s.GetEnvMap() {
			seen[env] = true
		}
	}

	for _, s := range b.GetSecrets() {
		for env := range s.GetSecretEnv() {
			seen[env] = true
		}
	}

	return slices.Sorted(maps.Keys(seen))
}
