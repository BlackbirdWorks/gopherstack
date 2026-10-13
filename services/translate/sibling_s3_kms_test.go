package translate_test

import (
	"bytes"
	"io"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	kmsbackend "github.com/blackbirdworks/gopherstack/services/kms"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/translate"
)

type siblings struct {
	s3  *s3backend.S3Handler
	kms *kmsbackend.Handler
}

func (s siblings) GetS3Handler() service.Registerable  { return s.s3 }
func (s siblings) GetKMSHandler() service.Registerable { return s.kms }

func newSiblingHandler(t *testing.T) (*translate.Handler, siblings) {
	t.Helper()

	sib := siblings{
		s3:  s3backend.NewHandler(s3backend.NewInMemoryBackend(nil)),
		kms: kmsbackend.NewHandler(kmsbackend.NewInMemoryBackend()),
	}
	_, err := sib.s3.Backend.CreateBucket(t.Context(), &awss3.CreateBucketInput{Bucket: aws.String("bkt")})
	require.NoError(t, err)

	backend := translate.NewInMemoryBackend("000000000000", "us-east-1")
	backend.SetAppConfig(sib)

	return translate.NewHandler(backend), sib
}

func putObject(t *testing.T, sib siblings, key, body string) {
	t.Helper()

	_, err := sib.s3.Backend.PutObject(t.Context(), &awss3.PutObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String(key), Body: bytes.NewReader([]byte(body)),
	})
	require.NoError(t, err)
}

func TestImportTerminology_EncryptionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		key      map[string]any
		name     string
		wantCode int
	}{
		{name: "none", wantCode: http.StatusOK},
		{
			name:     "unknown_kms_key",
			key:      map[string]any{"Type": "KMS", "Id": "no-such-key"},
			wantCode: http.StatusBadRequest,
		},
		{name: "bad_type", key: map[string]any{"Type": "AES", "Id": "k"}, wantCode: http.StatusBadRequest},
		{name: "missing_id", key: map[string]any{"Type": "KMS"}, wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newSiblingHandler(t)
			body := map[string]any{
				"Name": "t1", "MergeStrategy": "OVERWRITE",
				"TerminologyData": map[string]any{
					"File": b64("en,es\nhello,hola\n"), "Format": "CSV",
				},
			}
			if tt.key != nil {
				body["EncryptionKey"] = tt.key
			}

			rec := doRequest(t, h, "ImportTerminology", body)
			assert.Equal(t, tt.wantCode, rec.Code, rec.Body.String())
		})
	}
}

func TestImportTerminology_SkippedAndFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		format      string
		file        string
		wantSource  string
		wantTerms   float64
		wantSkipped float64
	}{
		{
			name:        "csv",
			format:      "CSV",
			file:        "en,es\nhello,hola\nbye,\n\n,vacio\ncat,gato\n",
			wantTerms:   2,
			wantSkipped: 3,
			wantSource:  "en",
		},
		{name: "tsv", format: "TSV", file: "en\tfr\nhello\tbonjour\n", wantTerms: 1, wantSource: "en"},
		{
			name: "tmx", format: "TMX", wantTerms: 1, wantSource: "en",
			file: `<tmx version="1.4"><header srclang="en"/><body><tu><tuv xml:lang="en"><seg>hi</seg></tuv>` +
				`<tuv xml:lang="de"><seg>hallo</seg></tuv></tu><tu><tuv xml:lang="en"><seg>x</seg></tuv></tu></body></tmx>`,
			wantSkipped: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "ImportTerminology", map[string]any{
				"Name":            "t",
				"MergeStrategy":   "OVERWRITE",
				"TerminologyData": map[string]any{"File": b64(tt.file), "Format": tt.format},
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			props := unmarshalJSON(t, rec.Body.Bytes())["TerminologyProperties"].(map[string]any)
			assert.InDelta(t, tt.wantTerms, props["TermCount"], 0)
			assert.InDelta(t, tt.wantSkipped, props["SkippedTermCount"], 0)
			assert.Equal(t, tt.wantSource, props["SourceLanguageCode"])
		})
	}
}

func TestParallelData_ImportStatisticsFromS3(t *testing.T) {
	t.Parallel()

	h, sib := newSiblingHandler(t)
	putObject(t, sib, "pd.csv", "en,fr,de\nhello,bonjour,hallo\nbye,,\n,vide,leer\n")

	rec := doRequest(t, h, "CreateParallelData", map[string]any{
		"Name":               "pd",
		"ParallelDataConfig": map[string]any{"S3Uri": "s3://bkt/pd.csv", "Format": "CSV"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	rec = doRequest(t, h, "GetParallelData", map[string]any{"Name": "pd"})
	require.Equal(t, http.StatusOK, rec.Code)

	props := unmarshalJSON(t, rec.Body.Bytes())["ParallelDataProperties"].(map[string]any)
	assert.Equal(t, "ACTIVE", props["Status"])
	assert.Equal(t, "en", props["SourceLanguageCode"])
	assert.Equal(t, []any{"fr", "de"}, props["TargetLanguageCodes"])
	assert.InDelta(t, 1, props["ImportedRecordCount"], 0)
	assert.InDelta(t, 2, props["SkippedRecordCount"], 0)
	assert.InDelta(t, 0, props["FailedRecordCount"], 0)
	assert.InDelta(t, len("hello")+len("bonjour")+len("hallo"), props["ImportedDataSize"], 0)
}

func TestParallelData_UnreadableInputLeavesStatisticsUnset(t *testing.T) {
	t.Parallel()

	h, _ := newSiblingHandler(t)

	rec := doRequest(t, h, "CreateParallelData", map[string]any{
		"Name":               "pd",
		"ParallelDataConfig": map[string]any{"S3Uri": "s3://bkt/missing.csv", "Format": "CSV"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "GetParallelData", map[string]any{"Name": "pd"})
	props := unmarshalJSON(t, rec.Body.Bytes())["ParallelDataProperties"].(map[string]any)
	assert.NotContains(t, props, "ImportedRecordCount")
}

func TestTextTranslationJob_ProcessesS3Documents(t *testing.T) {
	t.Parallel()

	h, sib := newSiblingHandler(t)
	putObject(t, sib, "in/a.txt", "hello world")
	putObject(t, sib, "in/b.txt", "good day")

	rec := doRequest(t, h, "StartTextTranslationJob", map[string]any{
		"JobName":             "j",
		"SourceLanguageCode":  "en",
		"TargetLanguageCodes": []string{"fr", "es"},
		"DataAccessRoleArn":   "arn:aws:iam::000000000000:role/r",
		"InputDataConfig":     map[string]any{"S3Uri": "s3://bkt/in/", "ContentType": "text/plain"},
		"OutputDataConfig":    map[string]any{"S3Uri": "s3://bkt/out/"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	jobID := unmarshalJSON(t, rec.Body.Bytes())["JobId"].(string)

	var details map[string]any

	for range 3 {
		rec = doRequest(t, h, "DescribeTextTranslationJob", map[string]any{"JobId": jobID})
		job := unmarshalJSON(t, rec.Body.Bytes())["TextTranslationJobProperties"].(map[string]any)
		details = job["JobDetails"].(map[string]any)
	}

	assert.InDelta(t, 2, details["InputDocumentsCount"], 0)
	assert.InDelta(t, 2, details["TranslatedDocumentsCount"], 0)
	assert.InDelta(t, 0, details["DocumentsWithErrorsCount"], 0)

	out, err := sib.s3.Backend.GetObject(t.Context(), &awss3.GetObjectInput{
		Bucket: aws.String("bkt"), Key: aws.String("out/000000000000-TranslateText-" + jobID + "/fr.a.txt"),
	})
	require.NoError(t, err)

	got, err := io.ReadAll(out.Body)
	require.NoError(t, err)
	assert.Equal(t, "fr: hello world", string(got))
}
