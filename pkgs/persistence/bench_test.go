package persistence_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
	"github.com/blackbirdworks/gopherstack/services/s3"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

const (
	benchObjects  = 2000
	benchItems    = 5000
	benchMessages = 5000
)

type benchStack struct {
	s3  *s3.InMemoryBackend
	ddb *dynamodb.InMemoryDB
	sqs *sqs.InMemoryBackend
}

func newBenchStack(b *testing.B) *benchStack {
	b.Helper()

	ctx := context.Background()
	svcCtx, cancel := context.WithCancel(ctx)
	b.Cleanup(cancel)

	st := &benchStack{
		s3:  s3.NewInMemoryBackend(&s3.GzipCompressor{}),
		ddb: dynamodb.NewInMemoryDB(),
		sqs: sqs.NewInMemoryBackendWithContext(svcCtx, "000000000000", "us-east-1"),
	}

	_, err := st.s3.CreateBucket(ctx, &awss3.CreateBucketInput{Bucket: aws.String("bench")})
	require.NoError(b, err)

	body := bytes.Repeat([]byte("x"), 512)
	for i := range benchObjects {
		_, err = st.s3.PutObject(ctx, &awss3.PutObjectInput{
			Bucket: aws.String("bench"),
			Key:    aws.String("obj/" + strconv.Itoa(i)),
			Body:   bytes.NewReader(body),
		})
		require.NoError(b, err)
	}

	ci := models.CreateTableInput{
		TableName:            "BenchTable",
		KeySchema:            []models.KeySchemaElement{{AttributeName: "id", KeyType: models.KeyTypeHash}},
		AttributeDefinitions: []models.AttributeDefinition{{AttributeName: "id", AttributeType: "S"}},
	}
	_, err = st.ddb.CreateTable(ctx, models.ToSDKCreateTableInput(&ci))
	require.NoError(b, err)

	for i := range benchItems {
		pi := models.PutItemInput{
			TableName: "BenchTable",
			Item: map[string]any{
				"id":  map[string]any{"S": strconv.Itoa(i)},
				"val": map[string]any{"N": strconv.Itoa(i)},
			},
		}
		in, convErr := models.ToSDKPutItemInput(&pi)
		require.NoError(b, convErr)

		_, err = st.ddb.PutItem(ctx, in)
		require.NoError(b, err)
	}

	q, err := st.sqs.CreateQueue(&sqs.CreateQueueInput{QueueName: "bench"})
	require.NoError(b, err)

	for i := range benchMessages {
		_, err = st.sqs.SendMessage(&sqs.SendMessageInput{
			QueueURL:    q.QueueURL,
			MessageBody: "message-body-" + strconv.Itoa(i),
		})
		require.NoError(b, err)
	}

	return st
}

func (st *benchStack) register(m *persistence.Manager) {
	m.Register("s3", st.s3)
	m.Register("dynamodb", st.ddb)
	m.Register("sqs", st.sqs)
}

func BenchmarkSaveAllRestoreAll(b *testing.B) {
	ctx := context.Background()
	st := newBenchStack(b)

	fs, err := persistence.NewFileStore(b.TempDir())
	require.NoError(b, err)

	m := persistence.NewManager(ctx, fs)
	st.register(m)

	b.Run("save", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			m.SaveAll(ctx)
		}
	})

	b.Run("restore", func(b *testing.B) {
		m.SaveAll(ctx)
		b.ReportAllocs()

		for b.Loop() {
			m.RestoreAll(ctx)
		}
	})
}

type lightPersistable struct{ data []byte }

func (l lightPersistable) Snapshot(context.Context) []byte { return l.data }

func (lightPersistable) Restore(context.Context, []byte) error { return nil }

func BenchmarkSaveRestoreManyLight(b *testing.B) {
	ctx := context.Background()

	fs, err := persistence.NewFileStore(b.TempDir())
	require.NoError(b, err)

	m := persistence.NewManager(ctx, fs)
	payload := bytes.Repeat([]byte("a"), 4096)
	payload[0], payload[len(payload)-1] = '"', '"'

	for i := range 150 {
		m.Register("svc"+strconv.Itoa(i), lightPersistable{data: payload})
	}

	b.Run("save", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			m.SaveAll(ctx)
		}
	})

	b.Run("restore", func(b *testing.B) {
		b.ReportAllocs()

		for b.Loop() {
			m.RestoreAll(ctx)
		}
	})
}
