package organizations_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/organizations"
)

func TestHandler_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		nextToken  string
		setupCount int
		maxResults int
		wantCount  int
		wantNext   bool
	}{
		{
			name:       "no_pagination_all_items",
			nextToken:  "",
			setupCount: 5,
			maxResults: 0, // Should default to defaultMaxResults
			wantCount:  6, // 5 created + 1 master account
			wantNext:   false,
		},
		{
			name:       "paginate_first_page",
			setupCount: 5,
			maxResults: 2,
			wantCount:  2,
			wantNext:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBackend()
			h := organizations.NewHandler(b)

			_, _, err := b.CreateOrganization("ALL")
			require.NoError(t, err)

			// Setup data
			for i := range tt.setupCount {
				_, errCreate := b.CreateAccount(
					fmt.Sprintf("Test Account %d", i),
					fmt.Sprintf("test%d@example.com", i),
					"ROLE",
					"ALLOW",
					nil,
				)
				require.NoError(t, errCreate)
			}

			// Perform request
			e := echo.New()
			reqBody := map[string]any{
				"MaxResults": tt.maxResults,
			}
			if tt.nextToken != "" {
				reqBody["NextToken"] = tt.nextToken
			}
			bodyBytes, _ := json.Marshal(reqBody)

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			req.Header.Set("X-Amz-Target", "AWSOrganizationsV20161128.ListAccounts")
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			// Inject logger
			ctx := logger.WithService(req.Context(), "organizations")
			c.SetRequest(req.WithContext(ctx))

			// Route using echo directly or via handler. Since we have to map the route,
			// let's just call the Echo handler directly.
			// The handler registers routes, so we can pass it through h.Handler()(c)
			err = h.Handler()(c)
			require.NoError(t, err)

			require.Equal(t, http.StatusOK, rec.Code)

			var res map[string]any
			err = json.Unmarshal(rec.Body.Bytes(), &res)
			require.NoError(t, err)

			accounts, ok := res["Accounts"].([]any)
			require.True(t, ok)
			require.Len(t, accounts, tt.wantCount)

			_, hasNext := res["NextToken"]
			require.Equal(t, tt.wantNext, hasNext)
		})
	}
}
