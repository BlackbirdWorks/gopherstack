package sns_test

import (
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// snsPostWithRegion sends a form-encoded SNS request with the X-Amz-Region header set
// so that httputils.ExtractRegionFromRequest returns the desired region.
func snsPostWithRegion(
	t *testing.T,
	h *sns.Handler,
	region string,
	form url.Values,
) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body := form.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("X-Amz-Region", region)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestInMemoryBackend_CreateTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *sns.InMemoryBackend)
		attrs     map[string]string
		wantAttr  map[string]string
		name      string
		topicName string
		wantArn   string
	}{
		{
			name:      "success",
			topicName: "my-topic",
			attrs:     map[string]string{"DisplayName": "My Topic"},
			wantArn:   "arn:aws:sns:us-east-1:000000000000:my-topic",
			wantAttr:  map[string]string{"DisplayName": "My Topic"},
		},
		{
			name: "duplicate",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("dup-topic", nil)
			},
			topicName: "dup-topic",
			// AWS CreateTopic is idempotent: no error, returns existing ARN.
			wantArn: "arn:aws:sns:us-east-1:000000000000:dup-topic",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			topic, err := b.CreateTopic(tt.topicName, tt.attrs)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantArn, topic.TopicArn)
			for k, v := range tt.wantAttr {
				assert.Equal(t, v, topic.Attributes[k])
			}
		})
	}
}

func TestInMemoryBackend_DeleteTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		setup    func(b *sns.InMemoryBackend)
		name     string
		topicArn string
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("del-topic", nil)
			},
			topicArn: "arn:aws:sns:us-east-1:000000000000:del-topic",
		},
		{
			name:     "not found",
			topicArn: "arn:aws:sns:us-east-1:000000000000:missing",
			wantErr:  sns.ErrTopicNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			err := b.DeleteTopic(tt.topicArn)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Empty(t, b.ListAllTopics())
		})
	}
}

func TestInMemoryBackend_ListTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		setup     func(b *sns.InMemoryBackend)
		name      string
		token     string
		wantCount int
		wantNext  bool
	}{
		{
			name:      "empty",
			token:     "",
			wantCount: 0,
			wantNext:  false,
		},
		{
			name: "with items",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("topic-a", nil)
				b.CreateTopic("topic-b", nil)
			},
			token:     "",
			wantCount: 2,
			wantNext:  false,
		},
		{
			name:    "invalid token",
			token:   "not-base64!!!",
			wantErr: sns.ErrInvalidParameter,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			topics, next, err := b.ListTopics(tt.token)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Len(t, topics, tt.wantCount)
			if tt.wantNext {
				assert.NotEmpty(t, next)
			} else {
				assert.Empty(t, next)
			}
		})
	}
}

func TestInMemoryBackend_ListAll(t *testing.T) {
	t.Parallel()

	t.Run("topics", func(t *testing.T) {
		t.Parallel()
		b := sns.NewInMemoryBackend()
		mustCreateTopic(t, b, "z-topic")
		mustCreateTopic(t, b, "a-topic")
		all := b.ListAllTopics()
		require.Len(t, all, 2)
		assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:a-topic", all[0].TopicArn)
		assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:z-topic", all[1].TopicArn)
	})

	t.Run("subscriptions", func(t *testing.T) {
		t.Parallel()
		b := sns.NewInMemoryBackend()
		arn := mustCreateTopic(t, b, "la-topic")
		mustSubscribe(t, b, arn, "sqs", "q")
		all := b.ListAllSubscriptions()
		require.Len(t, all, 1)
		assert.Equal(t, arn, all[0].TopicArn)
	})
}

func TestSNSHandler_CreateTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend)
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "success",
			form: url.Values{
				"Action":  {"CreateTopic"},
				"Version": {"2010-03-31"},
				"Name":    {"test-topic"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"test-topic"},
		},
		{
			name: "missing name",
			form: url.Values{
				"Action":  {"CreateTopic"},
				"Version": {"2010-03-31"},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"InvalidParameter"},
		},
		{
			name: "duplicate",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("dup", nil)
			},
			form: url.Values{
				"Action":  {"CreateTopic"},
				"Version": {"2010-03-31"},
				"Name":    {"dup"},
			},
			// AWS CreateTopic is idempotent: duplicate name returns existing ARN (200 OK).
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"dup"},
		},
		{
			name: "with attributes",
			form: url.Values{
				"Action":                   {"CreateTopic"},
				"Version":                  {"2010-03-31"},
				"Name":                     {"attrs-topic"},
				"Attributes.entry.1.key":   {"DisplayName"},
				"Attributes.entry.1.value": {"My Display Name"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"attrs-topic"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			h := sns.NewHandler(b)
			rec := snsPost(t, h, tt.form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_DeleteTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend)
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "success",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("del-topic", nil)
			},
			form: url.Values{
				"Action":   {"DeleteTopic"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:del-topic"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "not found",
			form: url.Values{
				"Action":   {"DeleteTopic"},
				"Version":  {"2010-03-31"},
				"TopicArn": {"arn:aws:sns:us-east-1:000000000000:missing"},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing ARN",
			form: url.Values{
				"Action":  {"DeleteTopic"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			h := sns.NewHandler(b)
			rec := snsPost(t, h, tt.form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

func TestSNSHandler_ListTopics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(b *sns.InMemoryBackend)
		form             url.Values
		name             string
		wantBodyContains []string
		wantStatus       int
	}{
		{
			name: "empty",
			form: url.Values{
				"Action":  {"ListTopics"},
				"Version": {"2010-03-31"},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "with items",
			setup: func(b *sns.InMemoryBackend) {
				b.CreateTopic("t1", nil)
			},
			form: url.Values{
				"Action":  {"ListTopics"},
				"Version": {"2010-03-31"},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: []string{"t1"},
		},
		{
			name: "invalid token",
			form: url.Values{
				"Action":    {"ListTopics"},
				"Version":   {"2010-03-31"},
				"NextToken": {"!!!not-base64"},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := sns.NewInMemoryBackend()
			if tt.setup != nil {
				tt.setup(b)
			}
			h := sns.NewHandler(b)
			rec := snsPost(t, h, tt.form)
			assert.Equal(t, tt.wantStatus, rec.Code)
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, rec.Body.String(), want)
			}
		})
	}
}

// TestCreateTopicXMLResponse verifies the XML structure of CreateTopic response.
func TestCreateTopicXMLResponse(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	h := sns.NewHandler(b)

	rec := snsPost(t, h, url.Values{
		"Action":  {"CreateTopic"},
		"Version": {"2010-03-31"},
		"Name":    {"xml-test"},
	})

	require.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)

	var resp sns.CreateTopicResponse
	require.NoError(t, xml.Unmarshal(body, &resp))
	assert.Contains(t, resp.CreateTopicResult.TopicArn, "xml-test")
	assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
}

// TestCreateTopic_RegionExtraction verifies that the handler uses the region from the SigV4
// Authorization header when creating a topic, and falls back to the default region otherwise.
func TestCreateTopic_RegionExtraction(t *testing.T) {
	t.Parallel()

	bk := sns.NewInMemoryBackend()
	h := sns.NewHandler(bk)
	h.DefaultRegion = "us-east-1"

	t.Run("default region when no Authorization header", func(t *testing.T) {
		t.Parallel()

		rec := snsPost(t, h, url.Values{
			"Action":  {"CreateTopic"},
			"Version": {"2010-03-31"},
			"Name":    {"default-topic"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "arn:aws:sns:us-east-1:000000000000:default-topic")
	})

	t.Run("region extracted from SigV4 Authorization header", func(t *testing.T) {
		t.Parallel()

		bk2 := sns.NewInMemoryBackend()
		h2 := sns.NewHandler(bk2)
		h2.DefaultRegion = "us-east-1"

		e := echo.New()
		form := url.Values{
			"Action":  {"CreateTopic"},
			"Version": {"2010-03-31"},
			"Name":    {"eu-topic"},
		}
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		// Minimal SigV4 Authorization header with eu-west-1 region in credential scope
		req.Header.Set("Authorization",
			"AWS4-HMAC-SHA256 Credential=AKID/20240101/eu-west-1/sns/aws4_request, SignedHeaders=host, Signature=abc")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		require.NoError(t, h2.Handler()(c))

		assert.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "arn:aws:sns:eu-west-1:000000000000:eu-topic")
	})
}

// TestCreateTopicInRegion_Backend tests the CreateTopicInRegion backend method directly.
func TestCreateTopicInRegion_Backend(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()

	topic, err := b.CreateTopicInRegion("my-topic", "eu-west-1", nil)
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sns:eu-west-1:000000000000:my-topic", topic.TopicArn)

	// Duplicate returns existing topic (idempotent), no error.
	tp2, err := b.CreateTopicInRegion("my-topic", "eu-west-1", nil)
	require.NoError(t, err)
	assert.Equal(t, topic.TopicArn, tp2.TopicArn, "idempotent CreateTopicInRegion must return same ARN")

	// Empty region falls back to backend default.
	topic2, err := b.CreateTopicInRegion("default-topic", "", nil)
	require.NoError(t, err)
	assert.Contains(t, topic2.TopicArn, "arn:aws:sns:us-east-1:000000000000:default-topic")
}

// TestSNS_TopicNameValidation validates CreateTopic name format rules.
func TestSNS_TopicNameValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		topicName string
		wantValid bool
	}{
		{name: "valid_alphanumeric", topicName: "MyTopic123", wantValid: true},
		{name: "valid_with_hyphen", topicName: "my-topic", wantValid: true},
		{name: "valid_with_underscore", topicName: "my_topic", wantValid: true},
		{name: "valid_fifo", topicName: "my-topic.fifo", wantValid: true},
		{name: "max_length", topicName: strings.Repeat("a", sns.ExportedMaxTopicNameLen), wantValid: true},
		{name: "too_long", topicName: strings.Repeat("a", sns.ExportedMaxTopicNameLen+1), wantValid: false},
		{name: "empty", topicName: "", wantValid: false},
		{name: "space", topicName: "my topic", wantValid: false},
		{name: "dot_only_fifo", topicName: ".fifo", wantValid: false},
		{name: "special_chars", topicName: "my!topic", wantValid: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.wantValid, sns.IsValidTopicNameForTest(tt.topicName))
		})
	}
}

// TestSNS_ListTopicsRegionIsolation verifies that ListTopics only returns topics
// belonging to the request region, not topics from other regions.
func TestSNS_ListTopicsRegionIsolation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		requestRegion    string
		wantBodyContains []string
		wantBodyAbsent   []string
	}{
		{
			name:             "us-east-1 sees only us-east topics",
			requestRegion:    "us-east-1",
			wantBodyContains: []string{"us-east-topic"},
			wantBodyAbsent:   []string{"eu-west-topic"},
		},
		{
			name:             "eu-west-1 sees only eu-west topics",
			requestRegion:    "eu-west-1",
			wantBodyContains: []string{"eu-west-topic"},
			wantBodyAbsent:   []string{"us-east-topic"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			_, err := b.CreateTopicInRegion("us-east-topic", "us-east-1", nil)
			require.NoError(t, err)
			_, err = b.CreateTopicInRegion("eu-west-topic", "eu-west-1", nil)
			require.NoError(t, err)

			h := sns.NewHandler(b)
			rec := snsPostWithRegion(t, h, tt.requestRegion, url.Values{
				"Action":  {"ListTopics"},
				"Version": {"2010-03-31"},
			})

			require.Equal(t, http.StatusOK, rec.Code)
			body := rec.Body.String()
			for _, want := range tt.wantBodyContains {
				assert.Contains(t, body, want)
			}
			for _, absent := range tt.wantBodyAbsent {
				assert.NotContains(t, body, absent)
			}
		})
	}
}

// TestSNS_ListTopicsInRegion_Backend verifies the backend ListTopicsInRegion method directly.
func TestSNS_ListTopicsInRegion_Backend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupRegions  map[string][]string
		name          string
		queryRegion   string
		wantTopicName string
		wantCount     int
	}{
		{
			name: "filters to queried region",
			setupRegions: map[string][]string{
				"us-east-1": {"topic-a", "topic-b"},
				"eu-west-1": {"topic-c"},
			},
			queryRegion:   "us-east-1",
			wantCount:     2,
			wantTopicName: "topic-a",
		},
		{
			name: "empty result for region with no topics",
			setupRegions: map[string][]string{
				"us-east-1": {"topic-a"},
			},
			queryRegion: "ap-southeast-1",
			wantCount:   0,
		},
		{
			name: "all topics in single region",
			setupRegions: map[string][]string{
				"us-west-2": {"x", "y", "z"},
			},
			queryRegion: "us-west-2",
			wantCount:   3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sns.NewInMemoryBackend()
			for region, names := range tt.setupRegions {
				for _, name := range names {
					_, err := b.CreateTopicInRegion(name, region, nil)
					require.NoError(t, err)
				}
			}

			topics, next, err := b.ListTopicsInRegion(tt.queryRegion, "")
			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, topics, tt.wantCount)
			if tt.wantTopicName != "" {
				found := false
				for _, tp := range topics {
					if strings.Contains(tp.TopicArn, tt.wantTopicName) {
						found = true

						break
					}
				}
				assert.True(t, found, "expected topic %q in results", tt.wantTopicName)
			}
		})
	}
}

// TestSNS_ListTopicsInRegion_Pagination verifies that pagination tokens are scoped
// correctly within a single region.
func TestSNS_ListTopicsInRegion_Pagination(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	for i := range 110 {
		_, err := b.CreateTopicInRegion(
			"paged-topic-"+strings.Repeat(
				"0",
				3-len([]rune(string(rune('0'+i/100)))),
			)+string(
				rune('0'+i/100),
			)+string(
				rune('0'+i%100/10),
			)+string(
				rune('0'+i%10),
			),
			"us-east-1",
			nil,
		)
		_ = err // name format is not critical; focus on count
	}
	// Also create topics in another region that must not appear.
	for i := range 5 {
		_, err := b.CreateTopicInRegion("other-"+strings.Repeat("x", i+1), "eu-west-1", nil)
		require.NoError(t, err)
	}

	page1, tok1, err := b.ListTopicsInRegion("us-east-1", "")
	require.NoError(t, err)
	assert.Len(t, page1, 100)
	assert.NotEmpty(t, tok1)

	page2, tok2, err := b.ListTopicsInRegion("us-east-1", tok1)
	require.NoError(t, err)
	assert.Len(t, page2, 10)
	assert.Empty(t, tok2)

	// Verify that eu-west-1 topics never appear in us-east-1 pages.
	allArns := make([]string, 0, len(page1)+len(page2))
	for _, tp := range append(page1, page2...) {
		allArns = append(allArns, tp.TopicArn)
	}
	for _, a := range allArns {
		assert.Contains(t, a, ":us-east-1:", "ARN must be scoped to us-east-1")
	}
}

// TestOps2_StandardTopicFifoTopicFalse verifies that GetTopicAttributes
// returns FifoTopic="false" for a standard (non-FIFO) topic.
// AWS always returns this attribute for all topics; omitting it for standard
// topics causes SDK clients to treat the topic as unknown type.
func TestStandardTopicFifoTopicAttributeFalse(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("standard-topic", nil)
	require.NoError(t, err)

	attrs, err := b.GetTopicAttributes(tp.TopicArn)
	require.NoError(t, err)

	assert.Equal(t, "false", attrs["FifoTopic"], "standard topic must report FifoTopic=false")
}

// TestOps2_FIFOTopicFifoTopicTrue verifies that the existing FifoTopic=true
// behaviour for FIFO topics is unaffected by the fix.
func TestFIFOTopicFifoTopicAttributeTrue(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp, err := b.CreateTopic("fifo-topic.fifo", map[string]string{"FifoTopic": "true"})
	require.NoError(t, err)

	attrs, err := b.GetTopicAttributes(tp.TopicArn)
	require.NoError(t, err)

	assert.Equal(t, "true", attrs["FifoTopic"])
}

// TestOps2_StandardTopicFifoTopicViaHandler verifies the FifoTopic=false
// attribute is visible in GetTopicAttributes HTTP responses.
func TestStandardTopicFifoTopicAttributeViaHandler(t *testing.T) {
	t.Parallel()

	h, _ := newB2Handler(t)

	createRec := doB2Request(t, h, url.Values{
		"Action": {"CreateTopic"},
		"Name":   {"standard-via-handler"},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	arnRe := regexp.MustCompile(`<TopicArn>(arn:[^<]+)</TopicArn>`)
	matches := arnRe.FindStringSubmatch(createRec.Body.String())
	require.Len(t, matches, 2, "could not extract TopicArn")
	topicArn := matches[1]

	getAttrsRec := doB2Request(t, h, url.Values{
		"Action":   {"GetTopicAttributes"},
		"TopicArn": {topicArn},
	})
	require.Equal(t, http.StatusOK, getAttrsRec.Code)

	body := getAttrsRec.Body.String()
	assert.Contains(t, body, "<key>FifoTopic</key>")
	assert.Contains(t, body, "<value>false</value>")
}

// TestBatch2_CreateTopicIdempotentReturnsSameARN verifies that CreateTopic with
// an already-existing name returns the existing topic ARN (AWS idempotency).
func TestCreateTopicIdempotentReturnsSameARN(t *testing.T) {
	t.Parallel()

	b := newB2Backend(t)
	tp1, err := b.CreateTopic("idem-topic", nil)
	require.NoError(t, err)

	tp2, err := b.CreateTopic("idem-topic", nil)
	require.NoError(t, err, "CreateTopic must be idempotent, not return an error on duplicate name")
	assert.Equal(t, tp1.TopicArn, tp2.TopicArn, "idempotent CreateTopic must return the same ARN")
}

// TestBatch2_CreateTopicIdempotentViaHandler verifies the handler returns 200 with
// the existing ARN when CreateTopic is called twice with the same name.
func TestCreateTopicIdempotentViaHandler(t *testing.T) {
	t.Parallel()

	h, _ := newB2Handler(t)

	rec1 := doB2Request(t, h, url.Values{
		"Action":  {"CreateTopic"},
		"Name":    {"idem-handler-topic"},
		"Version": {"2010-03-31"},
	})
	require.Equal(t, http.StatusOK, rec1.Code)

	rec2 := doB2Request(t, h, url.Values{
		"Action":  {"CreateTopic"},
		"Name":    {"idem-handler-topic"},
		"Version": {"2010-03-31"},
	})
	assert.Equal(t, http.StatusOK, rec2.Code, "duplicate CreateTopic must return 200")
	assert.Contains(t, rec2.Body.String(), "idem-handler-topic")
}
