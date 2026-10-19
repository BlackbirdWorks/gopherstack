package s3vectors

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"time"
)

// f32s stores a vector as float32 and serialises as base64 little-endian bytes, 4 bytes per dimension.
type f32s []float32

const bytesPerFloat = 4

var errVectorBytes = errors.New("vector bytes are not a multiple of 4")

// MarshalJSON implements json.Marshaler.
func (v f32s) MarshalJSON() ([]byte, error) {
	buf := make([]byte, bytesPerFloat*len(v))
	for i, f := range v {
		binary.LittleEndian.PutUint32(buf[bytesPerFloat*i:], math.Float32bits(f))
	}

	return json.Marshal(base64.StdEncoding.EncodeToString(buf))
}

// UnmarshalJSON implements json.Unmarshaler.
func (v *f32s) UnmarshalJSON(data []byte) error {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	buf, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return err
	}

	if len(buf)%bytesPerFloat != 0 {
		return errVectorBytes
	}

	out := make(f32s, len(buf)/bytesPerFloat)
	for i := range out {
		out[i] = math.Float32frombits(binary.LittleEndian.Uint32(buf[bytesPerFloat*i:]))
	}

	*v = out

	return nil
}

// Encryption is a bucket or index encryption configuration.
type Encryption struct {
	SseType   string `json:"sseType"`
	KmsKeyArn string `json:"kmsKeyArn,omitempty"`
}

// Bucket is a vector bucket.
type Bucket struct {
	Created          time.Time         `json:"created"`
	Tags             map[string]string `json:"tags,omitempty"`
	Encryption       Encryption        `json:"encryption"`
	Account          string            `json:"account"`
	Region           string            `json:"region"`
	Name             string            `json:"name"`
	ARN              string            `json:"arn"`
	DefaultIndexMode string            `json:"defaultIndexMode"`
	Policy           string            `json:"policy,omitempty"`
}

// Index is a vector index inside a bucket.
type Index struct {
	Created        time.Time         `json:"created"`
	Tags           map[string]string `json:"tags,omitempty"`
	Encryption     Encryption        `json:"encryption"`
	Account        string            `json:"account"`
	Region         string            `json:"region"`
	Bucket         string            `json:"bucket"`
	Name           string            `json:"name"`
	ARN            string            `json:"arn"`
	DataType       string            `json:"dataType"`
	DistanceMetric string            `json:"distanceMetric"`
	Mode           string            `json:"mode"`
	NonFilterable  []string          `json:"nonFilterable,omitempty"`
	Dimension      int               `json:"dimension"`
}

// Vector is one stored vector.
type Vector struct {
	Meta map[string]any `json:"meta,omitempty"`
	Key  string         `json:"key"`
	Data f32s           `json:"data"`
}

// VectorSet holds every vector of one index.
type VectorSet struct {
	Vectors map[string]*Vector `json:"vectors"`
	Key     string             `json:"key"`
}

func bucketKey(b *Bucket) string { return bucketID(b.Account, b.Region, b.Name) }

func indexKey(i *Index) string { return indexID(i.Account, i.Region, i.Bucket, i.Name) }

func setKey(s *VectorSet) string { return s.Key }

func bucketID(account, region, name string) string { return account + "|" + region + "|" + name }

func indexID(account, region, bucket, name string) string {
	return account + "|" + region + "|" + bucket + "|" + name
}
