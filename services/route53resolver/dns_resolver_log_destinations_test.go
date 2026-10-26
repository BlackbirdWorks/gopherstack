package route53resolver_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/miekg/dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

var errNoObject = errors.New("no such key")

type fakeObjects struct {
	mu   *lockmetrics.RWMutex
	data map[string][]byte
}

func (f *fakeObjects) GetObjectBytes(_ context.Context, bucket, key string) ([]byte, error) {
	f.mu.RLock("get")
	defer f.mu.RUnlock()

	if d, ok := f.data[bucket+"/"+key]; ok {
		return d, nil
	}

	return nil, errNoObject
}

func (f *fakeObjects) PutObjectBytes(_ context.Context, bucket, key string, data []byte) error {
	f.mu.Lock("put")
	defer f.mu.Unlock()

	f.data[bucket+"/"+key] = data

	return nil
}

func (f *fakeObjects) keys() []string {
	f.mu.RLock("keys")
	defer f.mu.RUnlock()

	out := make([]string, 0, len(f.data))
	for k := range f.data {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

type firehoseRecord struct {
	stream, region string
	data           []byte
}

type fakeStreams struct {
	mu      *lockmetrics.RWMutex
	records []firehoseRecord
}

func (f *fakeStreams) PutRecord(ctx context.Context, stream string, data []byte) error {
	f.mu.Lock("put")
	defer f.mu.Unlock()

	f.records = append(f.records, firehoseRecord{stream: stream, region: awsmeta.Region(ctx), data: data})

	return nil
}

func unzip(t *testing.T, data []byte) []string {
	t.Helper()

	zr, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)

	body, err := io.ReadAll(zr)
	require.NoError(t, err)

	return strings.Split(strings.TrimSuffix(string(body), "\n"), "\n")
}

func TestDNSResolverQueryLogS3Destination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dest       string
		keyPattern string
		src        string
		wantLines  int
	}{
		{
			name: "bucket_root", dest: "arn:aws:s3:::logs-bucket", src: vpc1Client, wantLines: 2,
			keyPattern: `^logs-bucket/AWSLogs/000000000000/vpcdnsquerylogs/vpc-11111111/\d{4}/\d{2}/\d{2}/` +
				`000000000000_vpcdnsquerylogs_vpc-11111111_\d{8}T\d{4}Z_[0-9a-f]{8}\.log\.gz$`,
		},
		{
			name: "with_prefix", dest: "arn:aws:s3:::logs-bucket/team/dns", src: vpc1Client, wantLines: 2,
			keyPattern: `^logs-bucket/team/dns/AWSLogs/000000000000/vpcdnsquerylogs/vpc-11111111/\d{4}/\d{2}/\d{2}/` +
				`000000000000_vpcdnsquerylogs_vpc-11111111_\d{8}T\d{4}Z_[0-9a-f]{8}\.log\.gz$`,
		},
		{name: "other_vpc_not_delivered", dest: "arn:aws:s3:::logs-bucket", src: vpc2Client},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newDNSEnv(t)
			objs := &fakeObjects{mu: lockmetrics.New("objs"), data: map[string][]byte{}}
			e.b.SetQueryLogDestinations(objs, nil)
			e.logConfig(t, tt.dest, vpc1)

			e.ask(tt.src, "q.example", dns.TypeA)
			e.ask(tt.src, "r.example", dns.TypeAAAA)

			keys := objs.keys()
			if tt.wantLines == 0 {
				assert.Empty(t, keys)

				return
			}

			re := regexp.MustCompile(tt.keyPattern)

			var names []string

			for _, k := range keys {
				require.Regexp(t, re, k)

				objs.mu.RLock("read")
				lines := unzip(t, objs.data[k])
				objs.mu.RUnlock()

				for _, l := range lines {
					var rec map[string]any
					require.NoError(t, json.Unmarshal([]byte(l), &rec))
					assert.Equal(t, vpc1, rec["vpc_id"])
					names = append(names, rec["query_name"].(string))
				}
			}

			assert.ElementsMatch(t, []string{"q.example.", "r.example."}, names)
			assert.Len(t, names, tt.wantLines)
		})
	}
}

func TestDNSResolverQueryLogFirehoseDestination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dest       string
		src        string
		wantStream string
		wantCount  int
	}{
		{
			name: "delivered", dest: "arn:aws:firehose:us-east-1:000000000000:deliverystream/qlogs",
			src: vpc1Client, wantStream: "qlogs", wantCount: 1,
		},
		{
			name: "other_vpc_not_delivered", dest: "arn:aws:firehose:us-east-1:000000000000:deliverystream/qlogs",
			src: vpc2Client,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newDNSEnv(t)
			streams := &fakeStreams{mu: lockmetrics.New("streams")}
			e.b.SetQueryLogDestinations(nil, streams)
			e.logConfig(t, tt.dest, vpc1)

			e.ask(tt.src, "q.example", dns.TypeA)

			streams.mu.RLock("read")
			defer streams.mu.RUnlock()

			require.Len(t, streams.records, tt.wantCount)

			if tt.wantCount == 0 {
				return
			}

			rec := streams.records[0]
			assert.Equal(t, tt.wantStream, rec.stream)
			assert.Equal(t, testRegion, rec.region)
			assert.True(t, bytes.HasSuffix(rec.data, []byte("\n")))

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.data, &body))
			assert.Equal(t, "q.example.", body["query_name"])
			assert.Equal(t, vpc1, body["vpc_id"])
		})
	}
}

func TestDNSResolverQueryLogMultipleDestinations(t *testing.T) {
	t.Parallel()

	e := newDNSEnv(t)
	_, err := e.logs.CreateLogGroup(context.Background(), queryLogGroup, "", "")
	require.NoError(t, err)

	objs := &fakeObjects{mu: lockmetrics.New("objs"), data: map[string][]byte{}}
	streams := &fakeStreams{mu: lockmetrics.New("streams")}
	e.b.SetQueryLogDestinations(objs, streams)

	e.logConfig(t, "arn:aws:logs:us-east-1:000000000000:log-group:"+queryLogGroup+":*", vpc1)
	e.logConfig(t, "arn:aws:s3:::logs-bucket", vpc1)
	e.logConfig(t, "arn:aws:firehose:us-east-1:000000000000:deliverystream/qlogs", vpc1)

	e.ask(vpc1Client, "q.example", dns.TypeA)

	assert.Len(t, e.loggedMessages(t, vpc1), 1)
	assert.Len(t, objs.keys(), 1)

	streams.mu.RLock("read")
	defer streams.mu.RUnlock()

	assert.Len(t, streams.records, 1)
}
