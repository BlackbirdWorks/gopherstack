package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// buildIndex writes deserializersSrc and one api_op_<op>.go per entry in
// apiOpSrcs into a fresh temp dir laid out like a real
// $(go env GOMODCACHE)/.../service/<mod>@<ver> module, then indexes it
// exactly as classifyService would.
func buildIndex(t *testing.T, deserializersSrc string, apiOpSrcs map[string]string) *serviceIndex {
	t.Helper()

	dir := t.TempDir()

	require.NoError(t, os.WriteFile(filepath.Join(dir, "deserializers.go"), []byte(deserializersSrc), 0o600))

	for op, src := range apiOpSrcs {
		require.NoError(t, os.WriteFile(filepath.Join(dir, "api_op_"+op+".go"), []byte(src), 0o600))
	}

	idx, err := indexDeserializers(dir)
	require.NoError(t, err)

	return idx
}

const wrappedSrc = `package svc

func (m *awsRestjson1_deserializeOpFoo) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (out middleware.DeserializeOutput, metadata middleware.Metadata, err error) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	response, ok := out.RawResponse.(*smithyhttp.Response)
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return out, metadata, err
	}
	output := &FooOutput{}
	out.Result = output
	var shape interface{}
	if err := decoder.Decode(&shape); err != nil {
		return out, metadata, err
	}
	err = awsRestjson1_deserializeOpDocumentFooOutput(&output, shape)
	return out, metadata, err
}

func awsRestjson1_deserializeOpDocumentFooOutput(v **FooOutput, value interface{}) error {
	shape, ok := value.(map[string]interface{})
	var sv *FooOutput
	for key, value := range shape {
		switch key {
		case "bar":
			sv.Bar = value
		default:
			_, _ = key, value
		}
	}
	*v = sv
	return nil
}
`

const wrappedOutputSrc = `package svc

type FooOutput struct {
	Bar *string

	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`

const deadHelperFlatSrc = `package svc

func (m *awsRestjson1_deserializeOpBar) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (out middleware.DeserializeOutput, metadata middleware.Metadata, err error) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	response, ok := out.RawResponse.(*smithyhttp.Response)
	output := &BarOutput{}
	out.Result = output
	var shape interface{}
	decoder.Decode(&shape)
	err = awsRestjson1_deserializeDocumentBarData(&output.BarData, shape)
	return out, metadata, err
}
`

const deadHelperFlatOutputSrc = `package svc

type BarOutput struct {
	BarData *BarData

	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`

const payloadFlatSrc = `package svc

func (m *awsRestjson1_deserializeOpBaz) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (out middleware.DeserializeOutput, metadata middleware.Metadata, err error) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	response, ok := out.RawResponse.(*smithyhttp.Response)
	output := &BazOutput{}
	out.Result = output
	err = awsRestjson1_deserializeOpDocumentBazOutput(output, response.Body)
	return out, metadata, err
}

func awsRestjson1_deserializeOpDocumentBazOutput(v *BazOutput, body io.ReadCloser) error {
	v.Stream = body
	return nil
}
`

const payloadFlatOutputSrc = `package svc

type BazOutput struct {
	Stream io.ReadCloser

	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`

const headerOnlySrc = `package svc

func (m *awsRestjson1_deserializeOpQux) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (out middleware.DeserializeOutput, metadata middleware.Metadata, err error) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	response, ok := out.RawResponse.(*smithyhttp.Response)
	output := &QuxOutput{}
	out.Result = output
	err = awsRestjson1_deserializeOpHttpBindingsQuxOutput(output, response)
	return out, metadata, err
}
`

const headerOnlyOutputSrc = `package svc

type QuxOutput struct {
	Name *string

	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`

const voidSrc = `package svc

func (m *awsRestjson1_deserializeOpVoid) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (out middleware.DeserializeOutput, metadata middleware.Metadata, err error) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	output := &VoidOutput{}
	out.Result = output
	return out, metadata, err
}
`

const voidOutputSrc = `package svc

type VoidOutput struct {
	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`

const unsupportedProtocolSrc = `package svc

func (m *awsRestxml_deserializeOpXML) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (out middleware.DeserializeOutput, metadata middleware.Metadata, err error) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	output := &XMLOutput{}
	out.Result = output
	err = awsRestxml_deserializeOpDocumentXMLOutput(&output, shape)
	return out, metadata, err
}
`

const unsupportedProtocolOutputSrc = `package svc

type XMLOutput struct {
	Name *string

	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`

const multiFieldPayloadSrc = `package svc

func (m *awsRestjson1_deserializeOpMulti) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (out middleware.DeserializeOutput, metadata middleware.Metadata, err error) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	output := &MultiOutput{}
	out.Result = output
	err = awsRestjson1_deserializeOpDocumentMultiOutput(output, response.Body)
	return out, metadata, err
}

func awsRestjson1_deserializeOpDocumentMultiOutput(v *MultiOutput, body io.ReadCloser) error {
	v.A = body
	v.B = body
	return nil
}
`

const multiFieldPayloadOutputSrc = `package svc

type MultiOutput struct {
	A io.ReadCloser
	B io.ReadCloser

	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`

const noCallButFieldsSrc = `package svc

func (m *awsRestjson1_deserializeOpWeird) HandleDeserialize(
	ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler,
) (out middleware.DeserializeOutput, metadata middleware.Metadata, err error) {
	out, metadata, err = next.HandleDeserialize(ctx, in)
	output := &WeirdOutput{}
	out.Result = output
	return out, metadata, err
}
`

const noCallButFieldsOutputSrc = `package svc

type WeirdOutput struct {
	Name *string

	ResultMetadata middleware.Metadata

	noSmithyDocumentSerde
}
`

func TestClassifyOp(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		op               string
		deserializersSrc string
		outputSrc        string
		wantClass        class
		wantMember       string
		wantDetailSubstr string
		wantKeys         []string
	}{
		{
			name:             "live wrapper reports key list",
			op:               "Foo",
			deserializersSrc: wrappedSrc,
			outputSrc:        wrappedOutputSrc,
			wantClass:        classWrapped,
			wantKeys:         []string{"bar"},
		},
		{
			name:             "dead wrapper flattened onto one field",
			op:               "Bar",
			deserializersSrc: deadHelperFlatSrc,
			outputSrc:        deadHelperFlatOutputSrc,
			wantClass:        classFlat,
			wantMember:       "BarData",
		},
		{
			name:             "payload bound helper flattened onto one field",
			op:               "Baz",
			deserializersSrc: payloadFlatSrc,
			outputSrc:        payloadFlatOutputSrc,
			wantClass:        classFlat,
			wantMember:       "Stream",
		},
		{
			name:             "http bindings only, no body",
			op:               "Qux",
			deserializersSrc: headerOnlySrc,
			outputSrc:        headerOnlyOutputSrc,
			wantClass:        classHeaderOnly,
		},
		{
			name:             "no members beyond result metadata",
			op:               "Void",
			deserializersSrc: voidSrc,
			outputSrc:        voidOutputSrc,
			wantClass:        classVoid,
		},
		{
			name:             "unsupported protocol reported loudly",
			op:               "XML",
			deserializersSrc: unsupportedProtocolSrc,
			outputSrc:        unsupportedProtocolOutputSrc,
			wantClass:        classUnknown,
			wantDetailSubstr: "not covered by this classifier",
		},
		{
			name:             "payload helper assigning multiple fields flagged, not guessed",
			op:               "Multi",
			deserializersSrc: multiFieldPayloadSrc,
			outputSrc:        multiFieldPayloadOutputSrc,
			wantClass:        classUnknown,
			wantDetailSubstr: "expected exactly one",
		},
		{
			name:             "no document or http-bindings call despite real fields",
			op:               "Weird",
			deserializersSrc: noCallButFieldsSrc,
			outputSrc:        noCallButFieldsOutputSrc,
			wantClass:        classUnknown,
			wantDetailSubstr: "no document-decode or HTTP-bindings call",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			idx := buildIndex(t, tt.deserializersSrc, map[string]string{tt.op: tt.outputSrc})

			got := classifyOp(idx, tt.op)

			assert.Equal(t, tt.wantClass, got.Class)
			assert.Equal(t, tt.wantMember, got.Member)

			if tt.wantKeys != nil {
				assert.Equal(t, tt.wantKeys, got.Keys)
			}

			if tt.wantDetailSubstr != "" {
				assert.Contains(t, got.Detail, tt.wantDetailSubstr)
			}
		})
	}
}

func TestClassifyOpMissingHandleDeserialize(t *testing.T) {
	t.Parallel()

	idx := buildIndex(t, "package svc\n", nil)

	got := classifyOp(idx, "Missing")

	assert.Equal(t, classUnknown, got.Class)
	assert.Contains(t, got.Detail, "no HandleDeserialize method found")
}

func TestSupportedProtocol(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		prefix string
		want   bool
	}{
		{name: "restjson1", prefix: "awsRestjson1", want: true},
		{name: "awsjson11", prefix: "awsAwsjson11", want: true},
		{name: "awsjson10", prefix: "awsAwsjson10", want: true},
		{name: "restxml", prefix: "awsRestxml", want: false},
		{name: "ec2query", prefix: "awsEc2query", want: false},
		{name: "awsquery", prefix: "awsAwsquery", want: false},
		{name: "empty", prefix: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, supportedProtocol(tt.prefix))
		})
	}
}
