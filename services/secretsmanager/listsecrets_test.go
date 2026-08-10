package secretsmanager_test

// listsecrets_test.go consolidates every ListSecrets-specific test that was previously
// scattered across several older test files. Ported verbatim (assertions unchanged).

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/secretsmanager"
)

// ---------------------------------------------------------------------------
// SortBy / NextRotationDate wire-shape
// ---------------------------------------------------------------------------

// TestListSecrets_SortBy verifies the ListSecrets SortBy field (created-date,
// last-changed-date, last-accessed-date, and the default name ordering), combined
// with SortOrder asc/desc. Matches the real AWS SortByType enum
// (aws-sdk-go-v2/service/secretsmanager/types.SortByType) which this mock previously
// ignored entirely, always ordering by name regardless of the client's request.
func TestListSecrets_SortBy(t *testing.T) {
	t.Parallel()

	t.Run("created-date", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()

		// Create secrets out of name order but in a known CreatedDate order:
		// zebra (oldest) -> mango -> apple (newest).
		times := []time.Time{
			time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC),
			time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC),
		}
		names := []string{"zebra", "mango", "apple"}

		for i, name := range names {
			ts := times[i]
			b.SetNowForTest(func() time.Time { return ts })
			_, err := b.CreateSecret(
				context.Background(),
				&secretsmanager.CreateSecretInput{Name: name, SecretString: "v"},
			)
			require.NoError(t, err)
		}
		b.SetNowForTest(time.Now)

		out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{SortBy: "created-date"})
		require.NoError(t, err)
		require.Len(t, out.SecretList, 3)
		gotNames := []string{out.SecretList[0].Name, out.SecretList[1].Name, out.SecretList[2].Name}
		assert.Equal(t, []string{"zebra", "mango", "apple"}, gotNames, "ascending created-date order")

		outDesc, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
			SortBy: "created-date", SortOrder: "desc",
		})
		require.NoError(t, err)
		require.Len(t, outDesc.SecretList, 3)
		gotNamesDesc := []string{outDesc.SecretList[0].Name, outDesc.SecretList[1].Name, outDesc.SecretList[2].Name}
		assert.Equal(t, []string{"apple", "mango", "zebra"}, gotNamesDesc, "descending created-date order")
	})

	t.Run("last-changed-date", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()

		for _, name := range []string{"c-secret", "a-secret", "b-secret"} {
			_, err := b.CreateSecret(
				context.Background(),
				&secretsmanager.CreateSecretInput{Name: name, SecretString: "v0"},
			)
			require.NoError(t, err)
		}

		// Change values out of name order but in a known LastChangedDate order:
		// b-secret (oldest change) -> c-secret -> a-secret (most recent change).
		changeOrder := []struct {
			ts   time.Time
			name string
		}{
			{time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "b-secret"},
			{time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC), "c-secret"},
			{time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC), "a-secret"},
		}
		for _, step := range changeOrder {
			ts := step.ts
			b.SetNowForTest(func() time.Time { return ts })
			_, err := b.UpdateSecret(context.Background(), &secretsmanager.UpdateSecretInput{
				SecretID: step.name, SecretString: "v1",
			})
			require.NoError(t, err)
		}
		b.SetNowForTest(time.Now)

		out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{SortBy: "last-changed-date"})
		require.NoError(t, err)
		require.Len(t, out.SecretList, 3)
		gotNames := []string{out.SecretList[0].Name, out.SecretList[1].Name, out.SecretList[2].Name}
		assert.Equal(t, []string{"b-secret", "c-secret", "a-secret"}, gotNames, "ascending last-changed-date order")
	})

	t.Run("last-accessed-date_nil_sorts_first", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()

		_, err := b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "never-read", SecretString: "v"},
		)
		require.NoError(t, err)
		_, err = b.CreateSecret(
			context.Background(),
			&secretsmanager.CreateSecretInput{Name: "was-read", SecretString: "v"},
		)
		require.NoError(t, err)

		_, err = b.GetSecretValue(context.Background(), &secretsmanager.GetSecretValueInput{SecretID: "was-read"})
		require.NoError(t, err)

		out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{SortBy: "last-accessed-date"})
		require.NoError(t, err)
		require.Len(t, out.SecretList, 2)
		// A secret that has never been read has no LastAccessedDate, which sorts as the
		// earliest possible value — it must come first in ascending order.
		assert.Equal(t, "never-read", out.SecretList[0].Name)
		assert.Equal(t, "was-read", out.SecretList[1].Name)
	})

	t.Run("default_is_name", func(t *testing.T) {
		t.Parallel()

		b := secretsmanager.NewInMemoryBackend()

		for _, name := range []string{"zebra", "apple", "mango"} {
			_, err := b.CreateSecret(
				context.Background(),
				&secretsmanager.CreateSecretInput{Name: name, SecretString: "v"},
			)
			require.NoError(t, err)
		}

		out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{})
		require.NoError(t, err)
		require.Len(t, out.SecretList, 3)
		gotNames := []string{out.SecretList[0].Name, out.SecretList[1].Name, out.SecretList[2].Name}
		assert.Equal(t, []string{"apple", "mango", "zebra"}, gotNames, "default SortBy is name ascending")
	})
}

// TestListSecrets_NextRotationDate verifies that ListSecrets' SecretListEntry carries
// NextRotationDate (matching aws-sdk-go-v2/service/secretsmanager/types.SecretListEntry),
// which this mock previously omitted — only DescribeSecret computed it.
func TestListSecrets_NextRotationDate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		configureFn  func(t *testing.T, b *secretsmanager.InMemoryBackend, secretName string)
		name         string
		wantHasValue bool
	}{
		{
			name: "rotation_not_configured_omits_field",
			configureFn: func(_ *testing.T, _ *secretsmanager.InMemoryBackend, _ string) {
				// no rotation configured
			},
			wantHasValue: false,
		},
		{
			name: "rotation_configured_populates_field",
			configureFn: func(t *testing.T, b *secretsmanager.InMemoryBackend, secretName string) {
				t.Helper()

				_, err := b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
					SecretID:          secretName,
					RotationLambdaARN: testLambdaARN,
					RotationRules: &secretsmanager.RotationRulesType{
						AutomaticallyAfterDays: ptr64(30),
					},
				})
				require.NoError(t, err)
			},
			wantHasValue: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
				Name: "rot-" + tc.name, SecretString: "v",
			})
			require.NoError(t, err)

			tc.configureFn(t, b, "rot-"+tc.name)

			out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
				Filters: []secretsmanager.SecretFilter{{Key: "name", Values: []string{"rot-" + tc.name}}},
			})
			require.NoError(t, err)
			require.Len(t, out.SecretList, 1)

			if tc.wantHasValue {
				assert.NotNil(t, out.SecretList[0].NextRotationDate)
			} else {
				assert.Nil(t, out.SecretList[0].NextRotationDate)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ListSecrets comprehensive
// ---------------------------------------------------------------------------

func TestListSecrets_Empty(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{})
	require.NoError(t, err)
	assert.Empty(t, out.SecretList)
}

func TestListSecrets_Basic(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	for _, name := range []string{"a-secret", "b-secret", "c-secret"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 3)
}

func TestListSecrets_MaxResultsZeroReturnsError(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	mr := int64(0)
	_, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{MaxResults: &mr})
	require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter, "MaxResults=0 must be rejected")
}

func TestListSecrets_MaxResults101ReturnsError(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	mr := int64(101)
	_, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{MaxResults: &mr})
	require.ErrorIs(t, err, secretsmanager.ErrInvalidParameter, "MaxResults=101 must be rejected")
}

func TestListSecrets_MaxResultsBoundsHTTP(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.ListSecrets", `{"MaxResults":200}`)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestListSecrets_Pagination(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	for i := range 10 {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
			Name:         fmt.Sprintf("page-secret-%02d", i),
			SecretString: "v",
		})
		require.NoError(t, err)
	}

	mr := int64(3)
	page1, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{MaxResults: &mr})
	require.NoError(t, err)
	assert.Len(t, page1.SecretList, 3)
	assert.NotEmpty(t, page1.NextToken)

	page2, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		MaxResults: &mr,
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, page2.SecretList, 3)

	// Collect all pages
	all := make([]secretsmanager.SecretListEntry, 0, 10)
	all = append(all, page1.SecretList...)
	all = append(all, page2.SecretList...)
	token := page2.NextToken
	for token != "" {
		var pageErr error
		var page *secretsmanager.ListSecretsOutput
		page, pageErr = b.ListSecrets(
			context.Background(),
			&secretsmanager.ListSecretsInput{MaxResults: &mr, NextToken: token},
		)
		require.NoError(t, pageErr)
		all = append(all, page.SecretList...)
		token = page.NextToken
	}
	assert.Len(t, all, 10)
}

func TestListSecrets_SortAsc(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	for _, name := range []string{"charlie", "alpha", "bravo"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{SortOrder: "asc"})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 3)
	assert.Equal(t, "alpha", out.SecretList[0].Name)
	assert.Equal(t, "charlie", out.SecretList[2].Name)
}

func TestListSecrets_SortDesc(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	for _, name := range []string{"charlie", "alpha", "bravo"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{SortOrder: "desc"})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 3)
	assert.Equal(t, "charlie", out.SecretList[0].Name)
	assert.Equal(t, "alpha", out.SecretList[2].Name)
}

func TestListSecrets_FilterByNamePrefix(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	for _, name := range []string{"prod/db", "prod/api", "dev/db"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "name", Values: []string{"prod/"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 2)
}

func TestListSecrets_FilterByDescription(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "desc-match",
		SecretString: "v",
		Description:  "database credentials",
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "no-match",
		SecretString: "v",
		Description:  "api key",
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "description", Values: []string{"database"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 1)
	assert.Equal(t, "desc-match", out.SecretList[0].Name)
}

func TestListSecrets_FilterByTagKey(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "tagged-secret",
		SecretString: "v",
		Tags:         []secretsmanager.Tag{{Key: "environment", Value: "prod"}},
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "untagged",
		SecretString: "v",
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "tag-key", Values: []string{"environment"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 1)
	assert.Equal(t, "tagged-secret", out.SecretList[0].Name)
}

func TestListSecrets_FilterByTagValue(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "prod-secret",
		SecretString: "v",
		Tags:         []secretsmanager.Tag{{Key: "env", Value: "prod"}},
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "dev-secret",
		SecretString: "v",
		Tags:         []secretsmanager.Tag{{Key: "env", Value: "dev"}},
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "tag-value", Values: []string{"prod"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 1)
	assert.Equal(t, "prod-secret", out.SecretList[0].Name)
}

func TestListSecrets_IncludePlannedDeletion(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "alive", SecretString: "v"})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "dead", SecretString: "v"})
	require.NoError(t, err)
	_, err = b.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "dead"})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{IncludePlannedDeletion: true})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 2)

	out2, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{IncludePlannedDeletion: false})
	require.NoError(t, err)
	assert.Len(t, out2.SecretList, 1)
}

func TestListSecrets_SecretVersionsToStages(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:               "stages-list",
		SecretString:       "v1",
		ClientRequestToken: "ver-1",
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{})
	require.NoError(t, err)
	require.Len(t, out.SecretList, 1)

	entry := out.SecretList[0]
	require.NotNil(t, entry.SecretVersionsToStages, "SecretVersionsToStages must be present in list entry")
	assert.Contains(t, entry.SecretVersionsToStages["ver-1"], secretsmanager.StagingLabelCurrent)
}

// ---------------------------------------------------------------------------
// ListSecrets — filter coverage (table-driven)
// ---------------------------------------------------------------------------

func TestListSecrets_Filters(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	ctx := context.Background()

	secrets := []secretsmanager.CreateSecretInput{
		{Name: "alpha-one", SecretString: "v", Description: "First alpha secret",
			Tags: []secretsmanager.Tag{{Key: "env", Value: "prod"}}},
		{Name: "alpha-two", SecretString: "v", Description: "Second alpha secret",
			Tags: []secretsmanager.Tag{{Key: "env", Value: "staging"}}},
		{Name: "beta-one", SecretString: "v", Description: "Beta secret",
			Tags: []secretsmanager.Tag{{Key: "team", Value: "platform"}}},
	}
	for i := range secrets {
		_, err := b.CreateSecret(ctx, &secrets[i])
		require.NoError(t, err)
	}

	tests := []struct {
		name      string
		filters   []secretsmanager.SecretFilter
		wantNames []string
	}{
		{
			name:      "filter_by_name_prefix",
			filters:   []secretsmanager.SecretFilter{{Key: "name", Values: []string{"alpha"}}},
			wantNames: []string{"alpha-one", "alpha-two"},
		},
		{
			name:      "filter_by_description",
			filters:   []secretsmanager.SecretFilter{{Key: "description", Values: []string{"Beta"}}},
			wantNames: []string{"beta-one"},
		},
		{
			name:      "filter_by_tag_key",
			filters:   []secretsmanager.SecretFilter{{Key: "tag-key", Values: []string{"env"}}},
			wantNames: []string{"alpha-one", "alpha-two"},
		},
		{
			name:      "filter_by_tag_value",
			filters:   []secretsmanager.SecretFilter{{Key: "tag-value", Values: []string{"prod"}}},
			wantNames: []string{"alpha-one"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			out, err := b.ListSecrets(ctx, &secretsmanager.ListSecretsInput{Filters: tc.filters})
			require.NoError(t, err)

			var gotNames []string
			for _, s := range out.SecretList {
				gotNames = append(gotNames, s.Name)
			}

			for _, wantName := range tc.wantNames {
				assert.Contains(t, gotNames, wantName)
			}
			assert.Len(t, gotNames, len(tc.wantNames),
				"filter %v returned unexpected secrets: %v", tc.filters, gotNames)
		})
	}
}

// ---------------------------------------------------------------------------
// owning-service / primary-region filters
//
// NOTE: the "owning-service" filter key was previously named "owned-by-me" in this
// test suite, which is not a real AWS FilterNameStringType value (the real key is
// "owning-service" — see aws-sdk-go-v2/service/secretsmanager/types.FilterNameStringType).
// It is a prefix match against DescribeSecretOutput.OwningService, which no
// CreateSecret/UpdateSecret input field can ever set in real AWS either (only AWS
// itself sets it, for service-linked secrets like RDS-managed rotation, which this
// mock does not model) -- so every secret has an empty OwningService and a
// non-empty filter value must match none of them, same as it would against a real
// AWS secret with no owning service.
// ---------------------------------------------------------------------------

// TestListSecrets_FilterOwningServiceMatchesNone verifies that "owning-service"
// matches no secrets in this mock, since OwningService is never set (gopherstack-9wuh:
// this previously asserted the wrong always-pass behavior, which would make a real
// client's "find secrets owned by rds.amazonaws.com" filter wrongly return every
// user-created secret).
func TestListSecrets_FilterOwningServiceMatchesNone(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()

	for _, name := range []string{"sec-a", "sec-b", "sec-c"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "owning-service", Values: []string{"rds"}}},
	})
	require.NoError(t, err)
	assert.Empty(t, out.SecretList, "owning-service must match nothing: no secret has an owning service")
}

// TestListSecrets_FilterOwningServiceWithOtherFilters verifies owning-service can be
// combined with other filters (AND semantics across distinct filter keys), and that
// a non-matching owning-service filter excludes secrets that would otherwise match.
func TestListSecrets_FilterOwningServiceWithOtherFilters(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "alpha-secret",
		SecretString: "v",
	})
	require.NoError(t, err)
	_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
		Name:         "beta-secret",
		SecretString: "v",
	})
	require.NoError(t, err)

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{
			{Key: "owning-service", Values: []string{"rds"}},
			{Key: "name", Values: []string{"alpha"}},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, out.SecretList, "owning-service=rds must exclude alpha-secret, which has no owning service")
}

// TestListSecrets_OwningServiceHTTP verifies the owning-service filter via HTTP.
func TestListSecrets_OwningServiceHTTP(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	for _, name := range []string{"om-1", "om-2"} {
		rec := doR1Request(t, h, "secretsmanager.CreateSecret",
			`{"Name":"`+name+`","SecretString":"v"}`)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	filterBody := `{"Filters":[{"Key":"owning-service","Values":["rds"]}]}`
	rec := doR1Request(t, h, "secretsmanager.ListSecrets", filterBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.SecretList, "owning-service=rds must match neither om-1 nor om-2")
}

// TestListSecrets_FilterByPrimaryRegion verifies that the "primary-region" filter
// always passes.
func TestListSecrets_FilterByPrimaryRegion(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	for _, name := range []string{"pr-a", "pr-b"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
		Filters: []secretsmanager.SecretFilter{{Key: "primary-region", Values: []string{"us-east-1"}}},
	})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 2)
}

// ---------------------------------------------------------------------------
// RotationRules in list entries
// ---------------------------------------------------------------------------

// TestListSecrets_IncludesRotationRules verifies that ListSecrets returns
// RotationRules in each secret entry when rotation has been configured.
// AWS includes RotationRules in the ListSecrets response.
func TestListSecrets_IncludesRotationRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(*testing.T, *secretsmanager.InMemoryBackend)
		checkFn func(*testing.T, *secretsmanager.ListSecretsOutput)
		name    string
	}{
		{
			name: "rotation_rules_returned_in_list",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "ls-rot", SecretString: "v"},
				)
				require.NoError(t, err)
				days := int64(30)
				_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
					SecretID:          "ls-rot",
					RotationLambdaARN: testLambdaARN,
					RotationRules: &secretsmanager.RotationRulesType{
						AutomaticallyAfterDays: &days,
					},
				})
				require.NoError(t, err)
			},
			checkFn: func(t *testing.T, out *secretsmanager.ListSecretsOutput) {
				t.Helper()
				require.Len(t, out.SecretList, 1)
				entry := out.SecretList[0]
				assert.True(t, entry.RotationEnabled)
				require.NotNil(t, entry.RotationRules, "RotationRules must be present in ListSecrets response")
				require.NotNil(t, entry.RotationRules.AutomaticallyAfterDays)
				assert.Equal(t, int64(30), *entry.RotationRules.AutomaticallyAfterDays)
			},
		},
		{
			name: "no_rotation_rules_when_not_configured",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "ls-norot", SecretString: "v"},
				)
				require.NoError(t, err)
			},
			checkFn: func(t *testing.T, out *secretsmanager.ListSecretsOutput) {
				t.Helper()
				require.Len(t, out.SecretList, 1)
				entry := out.SecretList[0]
				assert.False(t, entry.RotationEnabled)
				assert.Nil(t, entry.RotationRules, "RotationRules must be nil when rotation not configured")
			},
		},
		{
			name: "rotation_rules_in_http_response",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "ls-http-rot", SecretString: "v"},
				)
				require.NoError(t, err)
				days := int64(7)
				_, err = b.RotateSecret(context.Background(), &secretsmanager.RotateSecretInput{
					SecretID:          "ls-http-rot",
					RotationLambdaARN: testLambdaARN,
					RotationRules: &secretsmanager.RotationRulesType{
						AutomaticallyAfterDays: &days,
					},
				})
				require.NoError(t, err)
			},
			checkFn: func(t *testing.T, out *secretsmanager.ListSecretsOutput) {
				t.Helper()
				require.Len(t, out.SecretList, 1)
				entry := out.SecretList[0]
				require.NotNil(t, entry.RotationRules)
				assert.Equal(t, int64(7), *entry.RotationRules.AutomaticallyAfterDays)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			tt.setup(t, b)

			out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{})
			require.NoError(t, err)

			tt.checkFn(t, out)
		})
	}
}

// ---------------------------------------------------------------------------
// Filter-by-field consolidation
// ---------------------------------------------------------------------------

// TestListSecrets_FilterTypes consolidates the individual name/description/tag-key/
// tag-value filter checks into one table-driven test.
func TestListSecrets_FilterTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup     func(t *testing.T, b *secretsmanager.InMemoryBackend)
		name      string
		filterKey string
		filterVal string
		wantNames []string
	}{
		{
			name: "by_name",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				for _, name := range []string{"alpha-1", "alpha-2", "beta-1"} {
					_, err := b.CreateSecret(
						context.Background(),
						&secretsmanager.CreateSecretInput{Name: name, SecretString: "v"},
					)
					require.NoError(t, err)
				}
			},
			filterKey: "name",
			filterVal: "alpha",
			wantNames: []string{"alpha-1", "alpha-2"},
		},
		{
			name: "by_description",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name:         "desc-a",
					SecretString: "v",
					Description:  "production secret",
				})
				require.NoError(t, err)
				_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name:         "desc-b",
					SecretString: "v",
					Description:  "staging secret",
				})
				require.NoError(t, err)
			},
			filterKey: "description",
			filterVal: "prod",
			wantNames: []string{"desc-a"},
		},
		{
			name: "by_tag_key",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name:         "tagged",
					SecretString: "v",
					Tags:         []secretsmanager.Tag{{Key: "env", Value: "prod"}},
				})
				require.NoError(t, err)
				_, err = b.CreateSecret(
					context.Background(),
					&secretsmanager.CreateSecretInput{Name: "untagged", SecretString: "v"},
				)
				require.NoError(t, err)
			},
			filterKey: "tag-key",
			filterVal: "env",
			wantNames: []string{"tagged"},
		},
		{
			name: "by_tag_value",
			setup: func(t *testing.T, b *secretsmanager.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name:         "tv-a",
					SecretString: "v",
					Tags:         []secretsmanager.Tag{{Key: "env", Value: "prod"}},
				})
				require.NoError(t, err)
				_, err = b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{
					Name:         "tv-b",
					SecretString: "v",
					Tags:         []secretsmanager.Tag{{Key: "env", Value: "dev"}},
				})
				require.NoError(t, err)
			},
			filterKey: "tag-value",
			filterVal: "prod",
			wantNames: []string{"tv-a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := secretsmanager.NewInMemoryBackend()
			tc.setup(t, b)

			out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
				Filters: []secretsmanager.SecretFilter{{Key: tc.filterKey, Values: []string{tc.filterVal}}},
			})
			require.NoError(t, err)
			require.Len(t, out.SecretList, len(tc.wantNames))

			for i, want := range tc.wantNames {
				assert.Equal(t, want, out.SecretList[i].Name)
			}
		})
	}
}

// TestListSecrets_FilterByNameHTTP verifies ListSecrets filter via HTTP.
func TestListSecrets_FilterByNameHTTP(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()

	for _, name := range []string{"http-flt-a", "http-flt-b", "other"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	h := secretsmanager.NewHandler(b)
	rec := doR1Request(t, h, "secretsmanager.ListSecrets",
		`{"Filters":[{"Key":"name","Values":["http-flt"]}]}`)

	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Len(t, out.SecretList, 2)
}

// TestListSecrets_NoFilter verifies ListSecrets returns all secrets when no filters
// are supplied.
func TestListSecrets_NoFilter(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()

	for _, name := range []string{"x", "y", "z"} {
		_, err := b.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name, SecretString: "v"})
		require.NoError(t, err)
	}

	out, err := b.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{})
	require.NoError(t, err)
	assert.Len(t, out.SecretList, 3)
}

// ---------------------------------------------------------------------------
// SortOrder / FilterAll / list-entry fields
// ---------------------------------------------------------------------------

// TestListSecrets_SortOrder verifies SortOrder desc reverses the (default, by-name)
// list via the HTTP handler.
func TestListSecrets_SortOrder(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	for _, name := range []string{"alpha", "beta", "gamma"} {
		rec := doR1Request(t, h, "secretsmanager.CreateSecret",
			`{"Name":"`+name+`","SecretString":"v"}`)
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := doR1Request(t, h, "secretsmanager.ListSecrets", `{"SortOrder":"desc"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.SecretList, 3)
	assert.Equal(t, "gamma", out.SecretList[0].Name)
	assert.Equal(t, "alpha", out.SecretList[2].Name)
}

// TestListSecrets_FilterAll verifies the "all" filter key matches any field.
func TestListSecrets_FilterAll(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	body, _ := json.Marshal(map[string]any{
		"Name":         "findme/secret",
		"Description":  "unique-token",
		"SecretString": "v",
	})
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", string(body))
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"other","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	// Filter by description prefix via the "all" key.
	filterBody := `{"Filters":[{"Key":"all","Values":["unique-token"]}]}`
	rec = doR1Request(t, h, "secretsmanager.ListSecrets", filterBody)
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.SecretList, 1)
	assert.Equal(t, "findme/secret", out.SecretList[0].Name)
}

// TestListSecrets_EntryHasRotationFields verifies ListSecrets returns rotation
// metadata in each entry.
func TestListSecrets_EntryHasRotationFields(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	// Create a secret.
	const createBody = `{"Name":"list-fields-test","SecretString":"v","Description":"test desc"}`
	rec := doR1Request(t, h, "secretsmanager.CreateSecret", createBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// List secrets.
	rec = doR1Request(t, h, "secretsmanager.ListSecrets", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.SecretList, 1)

	entry := out.SecretList[0]
	assert.Equal(t, "list-fields-test", entry.Name)
	assert.Equal(t, "test desc", entry.Description)
	// LastChangedDate should be set (created with value).
	assert.NotNil(t, entry.LastChangedDate)
}

// TestListSecrets_EntryHasCreatedAndRotatedDate verifies that CreatedDate and
// LastRotatedDate are returned in ListSecrets entries.
func TestListSecrets_EntryHasCreatedAndRotatedDate(t *testing.T) {
	t.Parallel()

	b := secretsmanager.NewInMemoryBackend()
	h := secretsmanager.NewHandler(b)

	rec := doR1Request(t, h, "secretsmanager.CreateSecret",
		`{"Name":"datechecks","SecretString":"v"}`)
	require.Equal(t, http.StatusOK, rec.Code)

	rotateBody := fmt.Sprintf(`{"SecretId":"datechecks","RotationLambdaARN":%q}`, testLambdaARN)
	rec = doR1Request(t, h, "secretsmanager.RotateSecret", rotateBody)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doR1Request(t, h, "secretsmanager.ListSecrets", `{}`)
	require.Equal(t, http.StatusOK, rec.Code)

	var out secretsmanager.ListSecretsOutput
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.SecretList, 1)
	assert.NotNil(t, out.SecretList[0].CreatedDate)
	assert.NotNil(t, out.SecretList[0].LastRotatedDate)
}

// ---------------------------------------------------------------------------
// Backend scenarios (ported from table-style subtests)
// ---------------------------------------------------------------------------

func TestListSecrets_BackendScenarios(t *testing.T) {
	t.Parallel()

	t.Run("Basic", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		for _, name := range []string{"alpha", "beta", "gamma"} {
			_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name})
		}

		out, err := backend.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{})
		require.NoError(t, err)
		assert.Len(t, out.SecretList, 3)
	})

	t.Run("ExcludesDeleted", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "active"})
		_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "deleted"})
		_, _ = backend.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "deleted"})

		out, err := backend.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{})
		require.NoError(t, err)
		assert.Len(t, out.SecretList, 1)
		assert.Equal(t, "active", out.SecretList[0].Name)
	})

	t.Run("IncludesDeleted", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()
		_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "active"})
		_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: "deleted"})
		_, _ = backend.DeleteSecret(context.Background(), &secretsmanager.DeleteSecretInput{SecretID: "deleted"})

		out, err := backend.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
			IncludePlannedDeletion: true,
		})
		require.NoError(t, err)
		assert.Len(t, out.SecretList, 2)
	})

	t.Run("Pagination", func(t *testing.T) {
		t.Parallel()

		backend := secretsmanager.NewInMemoryBackend()

		for _, name := range []string{"a", "b", "c", "d", "e"} {
			_, _ = backend.CreateSecret(context.Background(), &secretsmanager.CreateSecretInput{Name: name})
		}

		limit := int64(2)
		out, err := backend.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{MaxResults: &limit})
		require.NoError(t, err)
		assert.Len(t, out.SecretList, 2)
		assert.NotEmpty(t, out.NextToken)

		out2, err := backend.ListSecrets(context.Background(), &secretsmanager.ListSecretsInput{
			MaxResults: &limit,
			NextToken:  out.NextToken,
		})
		require.NoError(t, err)
		assert.Len(t, out2.SecretList, 2)
	})
}
