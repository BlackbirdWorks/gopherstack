package transcribe_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transcribe"
)

func TestCreateVocabularyFilter_Words(t *testing.T) {
	t.Parallel()

	t.Run("words_required", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
			VocabularyFilterName: "my-filter-no-words",
			LanguageCode:         "en-US",
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})

	t.Run("words_accepted", func(t *testing.T) {
		t.Parallel()

		b := transcribe.NewInMemoryBackend()
		vf, err := b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
			VocabularyFilterName: "my-filter",
			LanguageCode:         "en-US",
			Words:                []string{"profanity", "slur"},
		})
		require.NoError(t, err)
		assert.Len(t, vf.Words, 2)
	})

	t.Run("too_many_words_rejected", func(t *testing.T) {
		t.Parallel()

		words := make([]string, 10001)
		for i := range words {
			words[i] = "word"
		}

		b := transcribe.NewInMemoryBackend()
		_, err := b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
			VocabularyFilterName: "my-filter-too-many-words",
			LanguageCode:         "en-US",
			Words:                words,
		})
		require.ErrorIs(t, err, transcribe.ErrValidation)
	})
}

func TestHTTP_GetVocabularyFilter(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	_, _ = b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
		VocabularyFilterName: "test-filter",
		LanguageCode:         "en-US",
		Words:                []string{"word"},
	})

	rec := doTranscribeRequest(t, h, "GetVocabularyFilter", map[string]any{
		"VocabularyFilterName": "test-filter",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHTTP_UpdateVocabularyFilter(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	_, _ = b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
		VocabularyFilterName: "test-filter",
		LanguageCode:         "en-US",
		Words:                []string{"word"},
	})

	rec := doTranscribeRequest(t, h, "UpdateVocabularyFilter", map[string]any{
		"VocabularyFilterName": "test-filter",
		"Words":                []string{"new"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHTTP_DeleteVocabularyFilter(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	_, _ = b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
		VocabularyFilterName: "test-filter-delete",
		LanguageCode:         "en-US",
		Words:                []string{"word"},
	})

	rec := doTranscribeRequest(t, h, "DeleteVocabularyFilter", map[string]any{
		"VocabularyFilterName": "test-filter-delete",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHTTP_ListVocabularyFilters(t *testing.T) {
	t.Parallel()

	h, b := newHandlerWithBackend(t)
	_, _ = b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
		VocabularyFilterName: "test-filter-list",
		LanguageCode:         "en-US",
		Words:                []string{"word"},
	})

	rec := doTranscribeRequest(t, h, "ListVocabularyFilters", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "test-filter-list")
}

func TestCreateVocabularyFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*testing.T, *transcribe.InMemoryBackend)
		body     map[string]any
		name     string
		wantKey  string
		wantCode int
	}{
		{
			name:  "success",
			setup: func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body: map[string]any{
				"VocabularyFilterName": "my-filter",
				"LanguageCode":         "en-US",
				"Words":                []string{"badword"},
			},
			wantCode: http.StatusOK,
			wantKey:  "my-filter",
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, b *transcribe.InMemoryBackend) {
				t.Helper()
				_, err := b.CreateVocabularyFilter(
					&transcribe.VocabularyFilter{
						VocabularyFilterName: "dup-filter",
						LanguageCode:         "en-US",
						Words:                []string{"test"},
					},
				)
				require.NoError(t, err)
			},
			body: map[string]any{
				"VocabularyFilterName": "dup-filter",
				"LanguageCode":         "en-US",
				"Words":                []string{"word"},
			},
			wantCode: http.StatusConflict,
		},
		{
			name:     "missing_name",
			setup:    func(_ *testing.T, _ *transcribe.InMemoryBackend) {},
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := transcribe.NewInMemoryBackend()
			h := transcribe.NewHandler(b)
			tt.setup(t, b)

			rec := doTranscribeRequest(t, h, "CreateVocabularyFilter", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantKey != "" {
				assert.Contains(t, rec.Body.String(), tt.wantKey)
			}
		})
	}
}

// TestCreateVocabularyFilter_OutputShape verifies response shape.
func TestCreateVocabularyFilter_OutputShape(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	h := transcribe.NewHandler(b)

	rec := doTranscribeRequest(t, h, "CreateVocabularyFilter", map[string]any{
		"VocabularyFilterName": "output-filter",
		"LanguageCode":         "en-US",
		"Words":                []string{"badword"},
	})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "output-filter")
	assert.Contains(t, rec.Body.String(), "en-US")
}

func TestUpdateVocabularyFilter(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	_, err := b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
		VocabularyFilterName: "my-filter",
		LanguageCode:         "en-US",
		Words:                []string{"initial"},
	})
	require.NoError(t, err)

	tests := []struct {
		input   *transcribe.VocabularyFilter
		name    string
		wantErr bool
	}{
		{
			name: "success_update",
			input: &transcribe.VocabularyFilter{
				VocabularyFilterName: "my-filter",
				Words:                []string{"updated"},
			},
			wantErr: false,
		},
		{
			name: "missing_name",
			input: &transcribe.VocabularyFilter{
				LanguageCode: "en-US",
			},
			wantErr: true,
		},
		{
			name: "not_found",
			input: &transcribe.VocabularyFilter{
				VocabularyFilterName: "missing-filter",
				Words:                []string{"word"},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, updateErr := b.UpdateVocabularyFilter(tt.input)
			if tt.wantErr {
				require.Error(t, updateErr)
			} else {
				require.NoError(t, updateErr)
				f, getErr := b.GetVocabularyFilter(tt.input.VocabularyFilterName)
				require.NoError(t, getErr)
				if len(tt.input.Words) > 0 {
					assert.Equal(t, tt.input.Words, f.Words)
				}
			}
		})
	}
}

func TestDeleteVocabularyFilter(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	_, err := b.CreateVocabularyFilter(&transcribe.VocabularyFilter{
		VocabularyFilterName: "delete-filter",
		LanguageCode:         "en-US",
		Words:                []string{"word"},
	})
	require.NoError(t, err)

	err = b.DeleteVocabularyFilter("")
	require.ErrorIs(t, err, transcribe.ErrValidation)

	err = b.DeleteVocabularyFilter("missing-filter")
	require.Error(t, err)

	err = b.DeleteVocabularyFilter("delete-filter")
	require.NoError(t, err)

	_, err = b.GetVocabularyFilter("delete-filter")
	require.Error(t, err)
}

func TestListVocabularyFilters(t *testing.T) {
	t.Parallel()

	b := transcribe.NewInMemoryBackend()
	b.AddVocabularyFilterInternal(&transcribe.VocabularyFilter{
		VocabularyFilterName: "filter-1",
	})
	b.AddVocabularyFilterInternal(&transcribe.VocabularyFilter{
		VocabularyFilterName: "filter-2",
	})

	list, _ := b.ListVocabularyFilters("")
	require.Len(t, list, 2)
	assert.Equal(t, "filter-1", list[0].VocabularyFilterName)
	assert.Equal(t, "filter-2", list[1].VocabularyFilterName)
}
