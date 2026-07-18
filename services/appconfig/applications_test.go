package appconfig_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestBackend_GetApplication_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	_, err := b.GetApplication("nonexistent")
	require.Error(t, err)
}

func TestBackend_ListApplications_Empty(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	apps, _ := b.ListApplications("", 0)
	assert.Empty(t, apps)
}

func TestBackend_DeleteApplication_NotFound(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	err := b.DeleteApplication("nonexistent")
	require.Error(t, err)
}

func TestBackend_appConfigPaginate_EdgeCases(t *testing.T) {
	t.Parallel()

	b := appconfig.NewInMemoryBackend("123456789012", "us-east-1")
	secret := b.PaginationSecret()

	// Create 4 apps.
	for _, name := range []string{"a", "b", "c", "d"} {
		_, err := b.CreateApplication(name, "")
		require.NoError(t, err)
	}

	tests := []struct {
		name          string
		nextToken     string
		maxResults    int
		wantCount     int
		wantNextToken bool
	}{
		{
			name:       "zero_max_returns_all",
			maxResults: 0,
			wantCount:  4,
		},
		{
			name:          "first_page",
			maxResults:    2,
			wantCount:     2,
			wantNextToken: true,
		},
		{
			name:       "second_page",
			maxResults: 2,
			nextToken:  page.EncodeHMACToken(2, secret),
			wantCount:  2,
		},
		{
			name:       "token_beyond_end",
			maxResults: 2,
			nextToken:  page.EncodeHMACToken(50, secret),
			wantCount:  0,
		},
		{
			name:          "invalid_token_treated_as_start",
			maxResults:    2,
			nextToken:     "bogus",
			wantCount:     2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			apps, outToken := b.ListApplications(tt.nextToken, tt.maxResults)
			assert.Len(t, apps, tt.wantCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, outToken)
			} else {
				assert.Empty(t, outToken)
			}
		})
	}
}
