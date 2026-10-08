package translate_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateOps_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	pd := func(token, s3 string) map[string]any {
		m := map[string]any{
			"Name":               "pd",
			"ParallelDataConfig": map[string]any{"S3Uri": s3, "Format": "TMX"},
		}
		if token != "" {
			m["ClientToken"] = token
		}

		return m
	}
	job := func(token, jobName string) map[string]any {
		m := map[string]any{
			"JobName":             jobName,
			"SourceLanguageCode":  "en",
			"TargetLanguageCodes": []string{"fr"},
			"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/TranslateRole",
			"InputDataConfig":     map[string]any{"S3Uri": "s3://b/i/", "ContentType": "text/plain"},
			"OutputDataConfig":    map[string]any{"S3Uri": "s3://b/o/"},
		}
		if token != "" {
			m["ClientToken"] = token
		}

		return m
	}

	tests := []struct {
		first      map[string]any
		second     map[string]any
		name       string
		op         string
		idKey      string
		wantType   string
		wantStatus int
		wantSame   bool
	}{
		{
			name: "parallel data same token replays", op: "CreateParallelData", idKey: "Name",
			first: pd("t1", "s3://b/a.tmx"), second: pd("t1", "s3://b/a.tmx"),
			wantStatus: http.StatusOK, wantSame: true,
		},
		{
			name: "parallel data token reuse with new params conflicts", op: "CreateParallelData", idKey: "Name",
			first: pd("t1", "s3://b/a.tmx"), second: pd("t1", "s3://b/other.tmx"),
			wantStatus: http.StatusBadRequest, wantType: "ConflictException",
		},
		{
			name: "job same token replays", op: "StartTextTranslationJob", idKey: "JobId",
			first: job("t1", "j"), second: job("t1", "j"),
			wantStatus: http.StatusOK, wantSame: true,
		},
		{
			name: "job without token starts twice", op: "StartTextTranslationJob", idKey: "JobId",
			first: job("", "j"), second: job("", "j"),
			wantStatus: http.StatusOK,
		},
		{
			name: "job token reuse with new params is invalid", op: "StartTextTranslationJob", idKey: "JobId",
			first: job("t1", "j"), second: job("t1", "other"),
			wantStatus: http.StatusBadRequest, wantType: "InvalidRequestException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			r1 := doRequest(t, h, tt.op, tt.first)
			require.Equal(t, http.StatusOK, r1.Code, r1.Body.String())
			id1 := unmarshalJSON(t, r1.Body.Bytes())[tt.idKey]

			r2 := doRequest(t, h, tt.op, tt.second)
			require.Equal(t, tt.wantStatus, r2.Code, r2.Body.String())

			if tt.wantType != "" {
				assert.Contains(t, r2.Body.String(), tt.wantType)

				return
			}

			id2 := unmarshalJSON(t, r2.Body.Bytes())[tt.idKey]
			if tt.wantSame {
				assert.Equal(t, id1, id2)
			} else {
				assert.NotEqual(t, id1, id2)
			}
		})
	}
}

func TestUpdateParallelData_ClientTokenReplay(t *testing.T) {
	t.Parallel()

	update := func(token, s3 string) map[string]any {
		return map[string]any{
			"Name":               "pd",
			"ClientToken":        token,
			"ParallelDataConfig": map[string]any{"S3Uri": s3, "Format": "TMX"},
		}
	}

	tests := []struct {
		second     map[string]any
		name       string
		wantType   string
		wantStatus int
	}{
		{name: "same token replays", second: update("u1", "s3://b/new.tmx"), wantStatus: http.StatusOK},
		{
			name: "token reuse with new params conflicts", second: update("u1", "s3://b/other.tmx"),
			wantStatus: http.StatusBadRequest, wantType: "ConflictException",
		},
		{
			name: "new token while updating is a concurrent modification", second: update("u2", "s3://b/new.tmx"),
			wantStatus: http.StatusBadRequest, wantType: "ConcurrentModificationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			r := doRequest(t, h, "CreateParallelData", map[string]any{
				"Name":               "pd",
				"ParallelDataConfig": map[string]any{"S3Uri": "s3://b/a.tmx", "Format": "TMX"},
			})
			require.Equal(t, http.StatusOK, r.Code)

			r = doRequest(t, h, "GetParallelData", map[string]any{"Name": "pd"})
			require.Equal(t, http.StatusOK, r.Code)

			r1 := doRequest(t, h, "UpdateParallelData", update("u1", "s3://b/new.tmx"))
			require.Equal(t, http.StatusOK, r1.Code, r1.Body.String())

			r2 := doRequest(t, h, "UpdateParallelData", tt.second)
			require.Equal(t, tt.wantStatus, r2.Code, r2.Body.String())

			if tt.wantType != "" {
				assert.Contains(t, r2.Body.String(), tt.wantType)
			}
		})
	}
}
