package s3vectors

import "github.com/blackbirdworks/gopherstack/pkgs/awstime"

type encryptionIn struct {
	SseType   string `json:"sseType"`
	KmsKeyArn string `json:"kmsKeyArn"`
}

type bucketRef struct {
	VectorBucketName string `json:"vectorBucketName"`
	VectorBucketArn  string `json:"vectorBucketArn"`
}

type indexRef struct {
	bucketRef
	IndexName string `json:"indexName"`
	IndexArn  string `json:"indexArn"`
}

type vectorData struct {
	Float32 []float32 `json:"float32"`
}

type encryptionOut struct {
	SseType   string `json:"sseType"`
	KmsKeyArn string `json:"kmsKeyArn,omitempty"`
}

func encOut(e Encryption) *encryptionOut {
	return &encryptionOut{SseType: e.SseType, KmsKeyArn: e.KmsKeyArn}
}

type bucketOut struct {
	Encryption       *encryptionOut `json:"encryptionConfiguration"`
	VectorBucketArn  string         `json:"vectorBucketArn"`
	VectorBucketName string         `json:"vectorBucketName"`
	DefaultIndexMode string         `json:"defaultIndexMode"`
	CreationTime     float64        `json:"creationTime"`
}

func toBucketOut(b *Bucket) bucketOut {
	return bucketOut{
		CreationTime: awstime.Epoch(b.Created), VectorBucketArn: b.ARN, VectorBucketName: b.Name,
		DefaultIndexMode: b.DefaultIndexMode, Encryption: encOut(b.Encryption),
	}
}

type bucketSummary struct {
	VectorBucketArn  string  `json:"vectorBucketArn"`
	VectorBucketName string  `json:"vectorBucketName"`
	CreationTime     float64 `json:"creationTime"`
}

type metadataConfig struct {
	NonFilterableMetadataKeys []string `json:"nonFilterableMetadataKeys"`
}

type indexOut struct {
	Encryption       *encryptionOut  `json:"encryptionConfiguration"`
	Metadata         *metadataConfig `json:"metadataConfiguration,omitempty"`
	DataType         string          `json:"dataType"`
	DistanceMetric   string          `json:"distanceMetric"`
	IndexArn         string          `json:"indexArn"`
	IndexName        string          `json:"indexName"`
	VectorBucketName string          `json:"vectorBucketName"`
	IndexMode        string          `json:"indexMode"`
	CreationTime     float64         `json:"creationTime"`
	Dimension        int             `json:"dimension"`
}

func toIndexOut(i *Index) indexOut {
	out := indexOut{
		CreationTime: awstime.Epoch(i.Created), DataType: i.DataType, Dimension: i.Dimension,
		DistanceMetric: i.DistanceMetric, IndexArn: i.ARN, IndexName: i.Name, VectorBucketName: i.Bucket,
		IndexMode: i.Mode, Encryption: encOut(i.Encryption),
	}
	if len(i.NonFilterable) > 0 {
		out.Metadata = &metadataConfig{NonFilterableMetadataKeys: i.NonFilterable}
	}

	return out
}

type indexSummary struct {
	IndexArn         string  `json:"indexArn"`
	IndexName        string  `json:"indexName"`
	VectorBucketName string  `json:"vectorBucketName"`
	CreationTime     float64 `json:"creationTime"`
}

type putVector struct {
	Metadata any        `json:"metadata"`
	Key      string     `json:"key"`
	Data     vectorData `json:"data"`
}

type outVector struct {
	Metadata any         `json:"metadata,omitempty"`
	Data     *vectorData `json:"data,omitempty"`
	Key      string      `json:"key"`
}

type queryVector struct {
	Distance *float32 `json:"distance,omitempty"`
	Metadata any      `json:"metadata,omitempty"`
	Key      string   `json:"key"`
}
