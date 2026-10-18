package s3vectors_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIndexValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t, "idx-bucket")

	base := func() *sdk.CreateIndexInput {
		return &sdk.CreateIndexInput{
			VectorBucketName: aws.String("idx-bucket"), IndexName: aws.String("good"), DataType: types.DataTypeFloat32,
			Dimension: aws.Int32(4), DistanceMetric: types.DistanceMetricCosine,
		}
	}

	tests := []struct {
		name     string
		mutate   func(in *sdk.CreateIndexInput)
		wantCode string
	}{
		{"ok", func(*sdk.CreateIndexInput) {}, ""},
		{"duplicate", func(*sdk.CreateIndexInput) {}, "ConflictException"},
		{
			"dimension zero",
			func(in *sdk.CreateIndexInput) { in.IndexName = aws.String("zero"); in.Dimension = aws.Int32(0) },
			"ValidationException",
		},
		{
			"dimension too large",
			func(in *sdk.CreateIndexInput) { in.IndexName = aws.String("huge"); in.Dimension = aws.Int32(4097) },
			"ValidationException",
		},
		{
			"max dimension",
			func(in *sdk.CreateIndexInput) { in.IndexName = aws.String("maxdim"); in.Dimension = aws.Int32(4096) },
			"",
		},
		{"bad name", func(in *sdk.CreateIndexInput) { in.IndexName = aws.String("UPPER") }, "ValidationException"},
		{"missing bucket", func(in *sdk.CreateIndexInput) {
			in.IndexName = aws.String("orphan")
			in.VectorBucketName = aws.String("nope-bucket")
		}, "NotFoundException"},
		{"too many non-filterable", func(in *sdk.CreateIndexInput) {
			in.IndexName = aws.String("many")

			keys := make([]string, 11)
			for i := range keys {
				keys[i] = fmt.Sprintf("k%d", i)
			}

			in.MetadataConfiguration = &types.MetadataConfiguration{NonFilterableMetadataKeys: keys}
		}, "ValidationException"},
		{"by bucket arn", func(in *sdk.CreateIndexInput) {
			in.IndexName = aws.String("via-arn")
			in.VectorBucketName = nil
			in.VectorBucketArn = aws.String("arn:aws:s3vectors:us-east-1:" + testAccount + ":bucket/idx-bucket")
		}, ""},
	}

	for _, tt := range tests {
		in := base()
		tt.mutate(in)

		_, err := f.client.CreateIndex(t.Context(), in)
		if tt.wantCode == "" {
			require.NoError(t, err, tt.name)

			continue
		}

		assert.Equal(t, tt.wantCode, apiCode(err), tt.name)
	}

	got, err := f.client.GetIndex(
		t.Context(),
		&sdk.GetIndexInput{VectorBucketName: aws.String("idx-bucket"), IndexName: aws.String("good")},
	)
	require.NoError(t, err)
	assert.Equal(t, int32(4), aws.ToInt32(got.Index.Dimension))
	assert.Equal(t, types.DistanceMetricCosine, got.Index.DistanceMetric)
	assert.Equal(t, types.DataTypeFloat32, got.Index.DataType)

	list, err := f.client.ListIndexes(
		t.Context(),
		&sdk.ListIndexesInput{VectorBucketName: aws.String("idx-bucket"), Prefix: aws.String("m")},
	)
	require.NoError(t, err)
	require.Len(t, list.Indexes, 1)
	assert.Equal(t, "maxdim", aws.ToString(list.Indexes[0].IndexName))
}

func TestVectorCRUD(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	f.bucket(t, "vec-bucket")
	f.index(t, "vec-bucket", "docs", "euclidean", 3, "body")

	target := func() (*string, *string) { return aws.String("vec-bucket"), aws.String("docs") }
	bucket, index := target()

	f.put(t, "vec-bucket", "docs",
		vec("a", map[string]any{"genre": "doc", "year": 2020, "body": "long text"}, 1, 0, 0),
		vec("b", map[string]any{"genre": "film"}, 0, 1, 0),
		vec("c", nil, 0, 0, 1),
	)

	got, err := f.client.GetVectors(ctx, &sdk.GetVectorsInput{
		VectorBucketName: bucket,
		IndexName:        index,
		Keys:             []string{"a", "missing"},
		ReturnData:       true,
		ReturnMetadata:   true,
	})
	require.NoError(t, err)
	require.Len(t, got.Vectors, 1)
	assert.Equal(t, []float32{1, 0, 0}, got.Vectors[0].Data.(*types.VectorDataMemberFloat32).Value)
	assert.NotNil(t, got.Vectors[0].Metadata)

	bare, err := f.client.GetVectors(
		ctx,
		&sdk.GetVectorsInput{VectorBucketName: bucket, IndexName: index, Keys: []string{"a"}},
	)
	require.NoError(t, err)
	assert.Nil(t, bare.Vectors[0].Data)
	assert.Nil(t, bare.Vectors[0].Metadata)

	f.put(t, "vec-bucket", "docs", vec("a", map[string]any{"genre": "replaced"}, 9, 9, 9))

	got, err = f.client.GetVectors(
		ctx,
		&sdk.GetVectorsInput{VectorBucketName: bucket, IndexName: index, Keys: []string{"a"}, ReturnData: true},
	)
	require.NoError(t, err)
	assert.Equal(
		t,
		[]float32{9, 9, 9},
		got.Vectors[0].Data.(*types.VectorDataMemberFloat32).Value,
		"PutVectors overwrites a key",
	)

	_, err = f.client.DeleteVectors(
		ctx,
		&sdk.DeleteVectorsInput{VectorBucketName: bucket, IndexName: index, Keys: []string{"a", "ghost"}},
	)
	require.NoError(t, err)

	list, err := f.client.ListVectors(ctx, &sdk.ListVectorsInput{VectorBucketName: bucket, IndexName: index})
	require.NoError(t, err)
	require.Len(t, list.Vectors, 2)
	assert.Equal(t, "b", aws.ToString(list.Vectors[0].Key))
	assert.Nil(t, list.Vectors[0].Data)
}

func TestPutVectorsValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t, "put-bucket")
	f.index(t, "put-bucket", "cos", "cosine", 2, "blob")

	many := make([]types.PutInputVector, 501)
	for i := range many {
		many[i] = vec(fmt.Sprintf("k%d", i), nil, 1, 1)
	}

	manyKeys := make(map[string]any, 51)
	for i := range 51 {
		manyKeys[fmt.Sprintf("m%d", i)] = i
	}

	tests := []struct {
		name     string
		wantCode string
		vectors  []types.PutInputVector
	}{
		{name: "ok", vectors: []types.PutInputVector{vec("ok", map[string]any{"a": 1}, 1, 2)}, wantCode: ""},
		{
			name:     "wrong dimension",
			vectors:  []types.PutInputVector{vec("d", nil, 1, 2, 3)},
			wantCode: "ValidationException",
		},
		{
			name:     "zero vector with cosine",
			vectors:  []types.PutInputVector{vec("z", nil, 0, 0)},
			wantCode: "ValidationException",
		},
		{name: "empty key", vectors: []types.PutInputVector{vec("", nil, 1, 1)}, wantCode: "ValidationException"},
		{name: "batch too large", vectors: many, wantCode: "ValidationException"},
		{
			name:     "too many metadata keys",
			vectors:  []types.PutInputVector{vec("mk", manyKeys, 1, 1)},
			wantCode: "ValidationException",
		},
		{
			name:     "filterable metadata too large",
			vectors:  []types.PutInputVector{vec("fm", map[string]any{"f": string(make([]byte, 3000))}, 1, 1)},
			wantCode: "ValidationException",
		},
		{name: "non-filterable may be large", vectors: []types.PutInputVector{
			vec("nf", map[string]any{"blob": fmt.Sprintf("%02000d", 7) + string(make([]byte, 5000))}, 1, 1),
		}, wantCode: ""},
		{
			name:     "bad metadata value",
			vectors:  []types.PutInputVector{vec("bm", map[string]any{"o": map[string]any{"x": 1}}, 1, 1)},
			wantCode: "ValidationException",
		},
		{
			name:     "atomic batch",
			vectors:  []types.PutInputVector{vec("good", nil, 1, 1), vec("bad", nil, 1)},
			wantCode: "ValidationException",
		},
	}

	for _, tt := range tests {
		_, err := f.client.PutVectors(t.Context(), &sdk.PutVectorsInput{
			VectorBucketName: aws.String("put-bucket"), IndexName: aws.String("cos"), Vectors: tt.vectors,
		})
		if tt.wantCode == "" {
			require.NoError(t, err, tt.name)

			continue
		}

		assert.Equal(t, tt.wantCode, apiCode(err), tt.name)
	}

	got, err := f.client.GetVectors(t.Context(), &sdk.GetVectorsInput{
		VectorBucketName: aws.String("put-bucket"), IndexName: aws.String("cos"), Keys: []string{"good"},
	})
	require.NoError(t, err)
	assert.Empty(t, got.Vectors, "a failed batch stores nothing")

	_, err = f.client.GetVectors(t.Context(), &sdk.GetVectorsInput{
		VectorBucketName: aws.String("put-bucket"), IndexName: aws.String("cos"), Keys: make([]string, 101),
	})
	assert.Equal(t, "ValidationException", apiCode(err))
}

func TestListVectorsSegments(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	f.bucket(t, "seg-bucket")
	f.index(t, "seg-bucket", "seg", "euclidean", 1)

	vs := make([]types.PutInputVector, 0, 50)
	for i := range 50 {
		vs = append(vs, vec(fmt.Sprintf("key-%02d", i), nil, float32(i)))
	}

	f.put(t, "seg-bucket", "seg", vs...)

	bucket, index := aws.String("seg-bucket"), aws.String("seg")
	seen := map[string]int{}

	for segment := range int32(4) {
		in := &sdk.ListVectorsInput{
			VectorBucketName: bucket,
			IndexName:        index,
			SegmentCount:     aws.Int32(4),
			SegmentIndex:     segment,
			MaxResults:       aws.Int32(7),
		}

		for {
			out, err := f.client.ListVectors(ctx, in)
			require.NoError(t, err)

			for _, v := range out.Vectors {
				seen[aws.ToString(v.Key)]++
			}

			if out.NextToken == nil {
				break
			}

			in.NextToken = out.NextToken
		}
	}

	assert.Len(t, seen, 50, "the four segments together cover every key")

	for k, n := range seen {
		assert.Equal(t, 1, n, k)
	}

	tests := []struct {
		in    *sdk.ListVectorsInput
		name  string
		valid bool
	}{
		{
			name:  "index out of range",
			in:    &sdk.ListVectorsInput{SegmentCount: aws.Int32(2), SegmentIndex: 2},
			valid: false,
		},
		{name: "count too large", in: &sdk.ListVectorsInput{SegmentCount: aws.Int32(17)}, valid: false},
		{name: "index without count", in: &sdk.ListVectorsInput{SegmentIndex: 1}, valid: false},
		{name: "max results too large", in: &sdk.ListVectorsInput{MaxResults: aws.Int32(1001)}, valid: false},
		{name: "valid", in: &sdk.ListVectorsInput{SegmentCount: aws.Int32(2), SegmentIndex: 1}, valid: true},
	}

	for _, tt := range tests {
		tt.in.VectorBucketName, tt.in.IndexName = bucket, index

		_, err := f.client.ListVectors(ctx, tt.in)
		if tt.valid {
			require.NoError(t, err, tt.name)

			continue
		}

		assert.Equal(t, "ValidationException", apiCode(err), tt.name)
	}
}

func TestIndexModes(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	ctx := t.Context()
	f.bucket(t, "mode-bucket")
	f.index(t, "mode-bucket", "mdx", "cosine", 2)

	name := aws.String("mode-bucket")
	idx := aws.String("mdx")

	_, err := f.client.UpdateIndexMode(
		ctx,
		&sdk.UpdateIndexModeInput{VectorBucketName: name, IndexName: idx, IndexMode: types.IndexModeEnhanced},
	)
	require.NoError(t, err)

	got, err := f.client.GetIndex(ctx, &sdk.GetIndexInput{VectorBucketName: name, IndexName: idx})
	require.NoError(t, err)
	assert.Equal(t, types.IndexModeEnhanced, got.Index.IndexMode)

	f.put(t, "mode-bucket", "mdx", vec("a", nil, 1, 1))

	_, err = f.client.QueryVectors(ctx, &sdk.QueryVectorsInput{
		VectorBucketName: name, IndexName: idx, TopK: aws.Int32(1), QueryMode: types.IndexModeClassic,
		QueryVector: &types.VectorDataMemberFloat32{Value: []float32{1, 1}},
	})
	assert.Equal(t, "ValidationException", apiCode(err), "CLASSIC cannot query an ENHANCED index")

	_, err = f.client.UpdateIndexMode(
		ctx,
		&sdk.UpdateIndexModeInput{VectorBucketName: name, IndexName: idx, IndexMode: types.IndexModeClassic},
	)
	require.NoError(t, err, "buckets created before the cutoff may use CLASSIC")
}
