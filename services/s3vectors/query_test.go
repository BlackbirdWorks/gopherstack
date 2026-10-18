package s3vectors_test

import (
	"math"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk "github.com/aws/aws-sdk-go-v2/service/s3vectors"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/document"
	"github.com/aws/aws-sdk-go-v2/service/s3vectors/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func query(
	t *testing.T,
	f *fixture,
	bucket, index string,
	k int32,
	filter map[string]any,
	q ...float32,
) (*sdk.QueryVectorsOutput, error) {
	t.Helper()

	in := &sdk.QueryVectorsInput{
		VectorBucketName: aws.String(bucket), IndexName: aws.String(index), TopK: aws.Int32(k),
		QueryVector: &types.VectorDataMemberFloat32{Value: q}, ReturnDistance: true, ReturnMetadata: true,
	}
	if filter != nil {
		in.Filter = document.NewLazyDocument(filter)
	}

	return f.client.QueryVectors(t.Context(), in)
}

func keysOf(out *sdk.QueryVectorsOutput) []string {
	keys := make([]string, 0, len(out.Vectors))
	for _, v := range out.Vectors {
		keys = append(keys, aws.ToString(v.Key))
	}

	return keys
}

func TestQueryMatchesBruteForce(t *testing.T) {
	t.Parallel()

	for _, metric := range []string{"euclidean", "cosine"} {
		t.Run(metric, func(t *testing.T) {
			t.Parallel()

			f := newFixture(t)
			f.bucket(t, "knn-bucket")
			f.index(t, "knn-bucket", "knn", metric, 8)

			rng := rand.New(rand.NewPCG(1, 2))
			data := map[string][]float32{}
			batch := make([]types.PutInputVector, 0, 200)

			for i := range 200 {
				v := make([]float32, 8)
				for j := range v {
					v[j] = rng.Float32()*2 - 1
				}

				key := string(rune('a'+i%26)) + string(rune('a'+i/26))
				data[key] = v
				batch = append(batch, vec(key, nil, v...))
			}

			f.put(t, "knn-bucket", "knn", batch...)

			q := []float32{0.3, -0.2, 0.9, 0.1, -0.7, 0.5, 0.0, 0.25}

			type pair struct {
				key  string
				dist float64
			}

			want := make([]pair, 0, len(data))
			for k, v := range data {
				var dot, na, nb, sq float64
				for i := range v {
					dot += float64(v[i]) * float64(q[i])
					na += float64(v[i]) * float64(v[i])
					nb += float64(q[i]) * float64(q[i])
					sq += math.Pow(float64(v[i])-float64(q[i]), 2)
				}

				d := math.Sqrt(sq)
				if metric == "cosine" {
					d = 1 - dot/(math.Sqrt(na)*math.Sqrt(nb))
				}

				want = append(want, pair{k, d})
			}

			sort.Slice(want, func(i, j int) bool { return want[i].dist < want[j].dist })

			out, err := query(t, f, "knn-bucket", "knn", 10, nil, q...)
			require.NoError(t, err)
			require.Len(t, out.Vectors, 10)
			assert.Equal(t, types.DistanceMetric(metric), out.DistanceMetric)

			for i, v := range out.Vectors {
				assert.Equal(t, want[i].key, aws.ToString(v.Key), "rank %d", i)
				assert.InDelta(t, want[i].dist, float64(aws.ToFloat32(v.Distance)), 1e-4)
			}

			first, err := query(t, f, "knn-bucket", "knn", 5, nil, q...)
			require.NoError(t, err)
			require.NotNil(t, first.NextToken, "more matches than topK leave a continuation token")

			second, err := f.client.QueryVectors(t.Context(), &sdk.QueryVectorsInput{
				VectorBucketName: aws.String("knn-bucket"), IndexName: aws.String("knn"), TopK: aws.Int32(5),
				QueryVector: &types.VectorDataMemberFloat32{Value: q}, NextToken: first.NextToken,
			})
			require.NoError(t, err)
			assert.Equal(t, keysOf(out)[5:], keysOf(second))
			assert.Nil(t, second.Vectors[0].Distance, "distance is only returned on request")
		})
	}
}

func TestQueryFilters(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t, "flt-bucket")
	f.index(t, "flt-bucket", "flt", "euclidean", 1, "notes")
	f.put(t, "flt-bucket", "flt",
		vec("a", map[string]any{"genre": "doc", "year": 2019, "live": true, "tags": []any{"x", "y"}, "notes": "n"}, 1),
		vec("b", map[string]any{"genre": "film", "year": 2021, "live": false}, 2),
		vec("c", map[string]any{"genre": "doc", "year": 2023}, 3),
		vec("d", nil, 4),
	)

	tests := []struct {
		filter   map[string]any
		name     string
		wantCode string
		want     []string
	}{
		{name: "shorthand eq", filter: map[string]any{"genre": "doc"}, want: []string{"a", "c"}, wantCode: ""},
		{name: "eq", filter: map[string]any{"genre": map[string]any{"$eq": "film"}}, want: []string{"b"}, wantCode: ""},
		{
			name:     "ne includes missing",
			filter:   map[string]any{"genre": map[string]any{"$ne": "doc"}},
			want:     []string{"b", "d"},
			wantCode: "",
		},
		{
			name:     "gt",
			filter:   map[string]any{"year": map[string]any{"$gt": 2019}},
			want:     []string{"b", "c"},
			wantCode: "",
		},
		{
			name:     "gte",
			filter:   map[string]any{"year": map[string]any{"$gte": 2021}},
			want:     []string{"b", "c"},
			wantCode: "",
		},
		{name: "lt", filter: map[string]any{"year": map[string]any{"$lt": 2021}}, want: []string{"a"}, wantCode: ""},
		{
			name:     "lte",
			filter:   map[string]any{"year": map[string]any{"$lte": 2021}},
			want:     []string{"a", "b"},
			wantCode: "",
		},
		{
			name:     "range",
			filter:   map[string]any{"year": map[string]any{"$gt": 2019, "$lt": 2023}},
			want:     []string{"b"},
			wantCode: "",
		},
		{
			name:     "in",
			filter:   map[string]any{"genre": map[string]any{"$in": []any{"film", "doc"}}},
			want:     []string{"a", "b", "c"},
			wantCode: "",
		},
		{
			name:     "nin",
			filter:   map[string]any{"genre": map[string]any{"$nin": []any{"film"}}},
			want:     []string{"a", "c", "d"},
			wantCode: "",
		},
		{
			name:     "exists true",
			filter:   map[string]any{"year": map[string]any{"$exists": true}},
			want:     []string{"a", "b", "c"},
			wantCode: "",
		},
		{
			name:     "exists false",
			filter:   map[string]any{"year": map[string]any{"$exists": false}},
			want:     []string{"d"},
			wantCode: "",
		},
		{name: "bool", filter: map[string]any{"live": true}, want: []string{"a"}, wantCode: ""},
		{name: "list element", filter: map[string]any{"tags": "y"}, want: []string{"a"}, wantCode: ""},
		{name: "and", filter: map[string]any{
			"$and": []any{map[string]any{"genre": "doc"}, map[string]any{"year": map[string]any{"$gt": 2020}}},
		}, want: []string{"c"}, wantCode: ""},
		{
			name:     "or",
			filter:   map[string]any{"$or": []any{map[string]any{"genre": "film"}, map[string]any{"year": 2019}}},
			want:     []string{"a", "b"},
			wantCode: "",
		},
		{name: "implicit and", filter: map[string]any{"genre": "doc", "year": 2023}, want: []string{"c"}, wantCode: ""},
		{name: "nested logic", filter: map[string]any{"$or": []any{
			map[string]any{"$and": []any{map[string]any{"genre": "doc"}, map[string]any{"live": true}}},
			map[string]any{"genre": "film"},
		}}, want: []string{"a", "b"}, wantCode: ""},
		{name: "empty filter", filter: map[string]any{}, want: []string{"a", "b", "c", "d"}, wantCode: ""},
		{
			name:     "unknown operator",
			filter:   map[string]any{"year": map[string]any{"$regex": "x"}},
			want:     nil,
			wantCode: "ValidationException",
		},
		{
			name:     "top-level unknown operator",
			filter:   map[string]any{"$not": []any{}},
			want:     nil,
			wantCode: "ValidationException",
		},
		{
			name:     "gt on string",
			filter:   map[string]any{"year": map[string]any{"$gt": "2019"}},
			want:     nil,
			wantCode: "ValidationException",
		},
		{
			name:     "in empty",
			filter:   map[string]any{"genre": map[string]any{"$in": []any{}}},
			want:     nil,
			wantCode: "ValidationException",
		},
		{
			name:     "exists non-bool",
			filter:   map[string]any{"year": map[string]any{"$exists": "yes"}},
			want:     nil,
			wantCode: "ValidationException",
		},
		{name: "and empty", filter: map[string]any{"$and": []any{}}, want: nil, wantCode: "ValidationException"},
		{name: "non-filterable key", filter: map[string]any{"notes": "n"}, want: nil, wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := query(t, f, "flt-bucket", "flt", 10, tt.filter, 0)
			if tt.wantCode != "" {
				assert.Equal(t, tt.wantCode, apiCode(err))

				return
			}

			require.NoError(t, err)

			got := keysOf(out)
			sort.Strings(got)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestQueryValidation(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t, "qv-bucket")
	f.index(t, "qv-bucket", "qvx", "cosine", 2)
	f.put(t, "qv-bucket", "qvx", vec("a", map[string]any{"k": "v"}, 1, 1))

	tests := []struct {
		name     string
		wantCode string
		q        []float32
		k        int32
	}{
		{name: "ok", k: 1, q: []float32{1, 1}, wantCode: ""},
		{name: "topK zero", k: 0, q: []float32{1, 1}, wantCode: "ValidationException"},
		{name: "topK too large", k: 101, q: []float32{1, 1}, wantCode: "ValidationException"},
		{name: "wrong dimension", k: 1, q: []float32{1}, wantCode: "ValidationException"},
		{name: "zero query with cosine", k: 1, q: []float32{0, 0}, wantCode: "ValidationException"},
	}

	for _, tt := range tests {
		_, err := query(t, f, "qv-bucket", "qvx", tt.k, nil, tt.q...)
		if tt.wantCode == "" {
			require.NoError(t, err, tt.name)

			continue
		}

		assert.Equal(t, tt.wantCode, apiCode(err), tt.name)
	}

	out, err := query(t, f, "qv-bucket", "qvx", 1, nil, 1, 1)
	require.NoError(t, err)
	assert.InDelta(t, 0, float64(aws.ToFloat32(out.Vectors[0].Distance)), 1e-6)
	assert.NotNil(t, out.Vectors[0].Metadata)

	_, err = f.client.QueryVectors(t.Context(), &sdk.QueryVectorsInput{
		IndexArn: aws.String(
			"arn:aws:s3vectors:us-east-1:" + testAccount + ":bucket/qv-bucket/index/qvx",
		),
		TopK:        aws.Int32(1),
		QueryVector: &types.VectorDataMemberFloat32{Value: []float32{1, 1}},
	})
	require.NoError(t, err, "an index ARN replaces bucket and index names")

	_, err = f.client.QueryVectors(t.Context(), &sdk.QueryVectorsInput{
		IndexName: aws.String(
			"qvx",
		),
		TopK:        aws.Int32(1),
		QueryVector: &types.VectorDataMemberFloat32{Value: []float32{1, 1}},
	})
	assert.Equal(t, "ValidationException", apiCode(err), "an index name needs its bucket")
}

func TestVectorPersistence(t *testing.T) {
	t.Parallel()

	f := newFixture(t)
	f.bucket(t, "persist-bucket")
	f.index(t, "persist-bucket", "pers", "euclidean", 3)
	f.put(t, "persist-bucket", "pers", vec("a", map[string]any{"x": 1}, 0.5, -1.25, 3))

	snap := f.backend.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	g := newFixture(t)
	require.NoError(t, g.backend.Restore(t.Context(), snap))

	got, err := g.client.GetVectors(t.Context(), &sdk.GetVectorsInput{
		VectorBucketName: aws.String(
			"persist-bucket",
		),
		IndexName:      aws.String("pers"),
		Keys:           []string{"a"},
		ReturnData:     true,
		ReturnMetadata: true,
	})
	require.NoError(t, err)
	require.Len(t, got.Vectors, 1)
	assert.Equal(t, []float32{0.5, -1.25, 3}, got.Vectors[0].Data.(*types.VectorDataMemberFloat32).Value)
}
