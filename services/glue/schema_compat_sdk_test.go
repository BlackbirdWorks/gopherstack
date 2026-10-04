package glue_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	avroBase = `{"type":"record","name":"P","fields":[` +
		`{"name":"first","type":"string"},{"name":"last","type":"string"},{"name":"email","type":"string"},` +
		`{"name":"phone","type":["null","string"],"default":null}]}`
	avroNoEmail = `{"type":"record","name":"P","fields":[` +
		`{"name":"first","type":"string"},{"name":"last","type":"string"},` +
		`{"name":"phone","type":["null","string"],"default":null}]}`
	avroNoFirst = `{"type":"record","name":"P","fields":[` +
		`{"name":"last","type":"string"},{"name":"email","type":"string"},` +
		`{"name":"phone","type":["null","string"],"default":null}]}`
	avroNoPhone = `{"type":"record","name":"P","fields":[` +
		`{"name":"first","type":"string"},{"name":"last","type":"string"},{"name":"email","type":"string"}]}`
	avroReqZip = `{"type":"record","name":"P","fields":[` +
		`{"name":"first","type":"string"},{"name":"last","type":"string"},{"name":"email","type":"string"},` +
		`{"name":"phone","type":["null","string"],"default":null},{"name":"zip","type":"string"}]}`
	avroOptZip = `{"type":"record","name":"P","fields":[` +
		`{"name":"first","type":"string"},{"name":"last","type":"string"},{"name":"email","type":"string"},` +
		`{"name":"phone","type":["null","string"],"default":null},{"name":"zip","type":["null","string"]}]}`
	avroDefZip = `{"type":"record","name":"P","fields":[` +
		`{"name":"first","type":"string"},{"name":"last","type":"string"},{"name":"email","type":"string"},` +
		`{"name":"phone","type":["null","string"],"default":null},{"name":"zip","type":"string","default":"00000"}]}`
	avroReqPhone = `{"type":"record","name":"P","fields":[` +
		`{"name":"first","type":"string"},{"name":"last","type":"string"},{"name":"email","type":"string"},` +
		`{"name":"phone","type":"string"}]}`
	avroIntV  = `{"type":"record","name":"R","fields":[{"name":"n","type":"int"}]}`
	avroLongV = `{"type":"record","name":"R","fields":[{"name":"n","type":"long"}]}`
	avroAB    = `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},{"name":"b","type":"string"}]}`
	avroABDef = `{"type":"record","name":"R","fields":[{"name":"a","type":"int"},` +
		`{"name":"b","type":"string","default":""}]}`
	avroAOnly  = `{"type":"record","name":"R","fields":[{"name":"a","type":"int"}]}`
	avroEnumV1 = `{"type":"enum","name":"E","symbols":["A","B"]}`
	avroEnumV2 = `{"type":"enum","name":"E","symbols":["A","B","C"]}`
)

const (
	jsonClosed = `{"type":"object","properties":{"first":{"type":"string"},"last":{"type":"string"}},` +
		`"additionalProperties":false}`
	jsonClosedPhone = `{"type":"object","properties":{"first":{"type":"string"},"last":{"type":"string"},` +
		`"phone":{"type":"number"}},"additionalProperties":false}`
	jsonOpen      = `{"type":"object","properties":{"first":{"type":"string"},"last":{"type":"string"}}}`
	jsonOpenPhone = `{"type":"object","properties":{"first":{"type":"string"},"last":{"type":"string"},` +
		`"phone":{"type":"number"}}}`
	jsonReqFirst = `{"type":"object","properties":{"first":{"type":"string"},"last":{"type":"string"}},` +
		`"required":["first"],"additionalProperties":false}`
	jsonStrAge = `{"type":"object","properties":{"age":{"type":"string"}},"additionalProperties":false}`
	jsonIntAge = `{"type":"object","properties":{"age":{"type":"integer"}},"additionalProperties":false}`
	jsonNumAge = `{"type":"object","properties":{"age":{"type":"number"}},"additionalProperties":false}`
)

const (
	protoBase = `syntax = "proto2"; message Person { required string first = 1; required string last = 2; ` +
		`required string email = 3; optional string phone = 4; }`
	protoNoEmail = `syntax = "proto2"; message Person { required string first = 1; required string last = 2; ` +
		`optional string phone = 4; }`
	protoNoFirst = `syntax = "proto2"; message Person { required string last = 2; ` +
		`required string email = 3; optional string phone = 4; }`
	protoNoPhone = `syntax = "proto2"; message Person { required string first = 1; required string last = 2; ` +
		`required string email = 3; }`
	protoReqZip = `syntax = "proto2"; message Person { required string first = 1; required string last = 2; ` +
		`required string email = 3; optional string phone = 4; required string zip = 5; }`
	protoOptZip = `syntax = "proto2"; message Person { required string first = 1; required string last = 2; ` +
		`required string email = 3; optional string phone = 4; optional string zip = 5; }`
	protoPhoneInt = `syntax = "proto2"; message Person { required string first = 1; required string last = 2; ` +
		`required string email = 3; optional int32 phone = 4; }`
	protoSvc2 = `syntax = "proto3"; message M { string a = 1; } service S { rpc Foo (M) returns (M); ` +
		`rpc Bar (M) returns (M); }`
	protoSvc3 = `syntax = "proto3"; message M { string a = 1; } service S { rpc Foo (M) returns (M); ` +
		`rpc Bar (M) returns (M); rpc Baz (M) returns (M); }`
	protoSvc1 = `syntax = "proto3"; message M { string a = 1; } service S { rpc Bar (M) returns (M); }`
)

type compatCase struct {
	name      string
	format    types.DataFormat
	mode      types.Compatibility
	candidate string
	versions  []string
	wantErr   bool
}

func compatCases() []compatCase {
	a, j, p := types.DataFormatAvro, types.DataFormatJson, types.DataFormatProtobuf
	bw, fw, full := types.CompatibilityBackward, types.CompatibilityForward, types.CompatibilityFull
	bwAll, fwAll, fullAll := types.CompatibilityBackwardAll, types.CompatibilityForwardAll, types.CompatibilityFullAll

	return []compatCase{
		{"avro_backward_remove_field", a, bw, avroNoEmail, []string{avroBase}, false},
		{"avro_backward_add_required", a, bw, avroReqZip, []string{avroBase}, true},
		{"avro_backward_add_optional", a, bw, avroOptZip, []string{avroBase}, false},
		{"avro_backward_add_default", a, bw, avroDefZip, []string{avroBase}, false},
		{"avro_backward_widen_int_long", a, bw, avroLongV, []string{avroIntV}, false},
		{"avro_backward_narrow_long_int", a, bw, avroIntV, []string{avroLongV}, true},
		{"avro_backward_enum_symbol_added", a, bw, avroEnumV2, []string{avroEnumV1}, false},
		{"avro_backward_enum_symbol_removed", a, bw, avroEnumV1, []string{avroEnumV2}, true},
		{"avro_backward_record_renamed", a, bw, avroIntV, []string{avroBase}, true},
		{"avro_forward_add_required", a, fw, avroReqPhone, []string{avroNoPhone}, false},
		{"avro_forward_remove_required", a, fw, avroNoFirst, []string{avroBase}, true},
		{"avro_forward_remove_optional", a, fw, avroNoPhone, []string{avroBase}, false},
		{"avro_forward_narrow_long_int", a, fw, avroIntV, []string{avroLongV}, false},
		{"avro_forward_widen_int_long", a, fw, avroLongV, []string{avroIntV}, true},
		{"avro_full_add_default", a, full, avroDefZip, []string{avroBase}, false},
		{"avro_full_add_required", a, full, avroReqZip, []string{avroBase}, true},
		{"avro_full_remove_required", a, full, avroNoEmail, []string{avroBase}, true},
		{"avro_backward_checks_latest_only", a, bw, avroAB, []string{avroAOnly, avroABDef}, false},
		{"avro_backward_all_checks_every_version", a, bwAll, avroAB, []string{avroAOnly, avroABDef}, true},
		{"avro_forward_checks_latest_only", a, fw, avroAOnly, []string{avroAB, avroABDef}, false},
		{"avro_forward_all_checks_every_version", a, fwAll, avroAOnly, []string{avroAB, avroABDef}, true},
		{"avro_full_all_checks_every_version", a, fullAll, avroAOnly, []string{avroAB, avroABDef}, true},
		{"avro_none_accepts_anything", a, types.CompatibilityNone, avroIntV, []string{avroBase}, false},

		{"json_backward_add_property_closed_old", j, bw, jsonClosedPhone, []string{jsonClosed}, false},
		{"json_backward_add_property_open_old", j, bw, jsonOpenPhone, []string{jsonOpen}, true},
		{"json_backward_add_required", j, bw, jsonReqFirst, []string{jsonClosed}, true},
		{"json_backward_remove_required", j, bw, jsonClosed, []string{jsonReqFirst}, false},
		{"json_backward_change_type", j, bw, jsonIntAge, []string{jsonStrAge}, true},
		{"json_backward_widen_integer_number", j, bw, jsonNumAge, []string{jsonIntAge}, false},
		{"json_backward_narrow_number_integer", j, bw, jsonIntAge, []string{jsonNumAge}, true},
		{"json_forward_remove_property_closed_new", j, fw, jsonClosed, []string{jsonClosedPhone}, false},
		{"json_forward_remove_property_open_new", j, fw, jsonOpen, []string{jsonOpenPhone}, true},
		{"json_forward_add_property_closed_new", j, fw, jsonClosedPhone, []string{jsonClosed}, true},
		{"json_full_add_property_closed", j, full, jsonClosedPhone, []string{jsonClosed}, true},
		{"json_full_equivalent_shape", j, full, jsonClosed, []string{jsonClosed + " "}, false},

		{"proto_backward_remove_required", p, bw, protoNoEmail, []string{protoBase}, false},
		{"proto_backward_add_required", p, bw, protoReqZip, []string{protoBase}, true},
		{"proto_backward_add_optional", p, bw, protoOptZip, []string{protoBase}, false},
		{"proto_backward_change_field_type", p, bw, protoPhoneInt, []string{protoBase}, true},
		{"proto_backward_add_rpc", p, bw, protoSvc3, []string{protoSvc2}, false},
		{"proto_backward_remove_rpc", p, bw, protoSvc1, []string{protoSvc2}, true},
		{"proto_forward_add_required", p, fw, protoReqZip, []string{protoBase}, false},
		{"proto_forward_remove_required", p, fw, protoNoFirst, []string{protoBase}, true},
		{"proto_forward_remove_optional", p, fw, protoNoPhone, []string{protoBase}, false},
		{"proto_forward_remove_rpc", p, fw, protoSvc1, []string{protoSvc2}, false},
		{"proto_forward_add_rpc", p, fw, protoSvc3, []string{protoSvc2}, true},
		{"proto_full_add_optional", p, full, protoOptZip, []string{protoBase}, false},
		{"proto_full_add_required", p, full, protoReqZip, []string{protoBase}, true},
		{"proto_backward_all_every_version", p, bwAll, protoReqZip, []string{protoBase, protoOptZip}, true},
	}
}

func registerVersionSDK(
	t *testing.T, c *gluesdk.Client, def string,
) (*gluesdk.RegisterSchemaVersionOutput, error) {
	t.Helper()

	return c.RegisterSchemaVersion(t.Context(), &gluesdk.RegisterSchemaVersionInput{
		SchemaId:         &types.SchemaId{SchemaName: aws.String("s")},
		SchemaDefinition: aws.String(def),
	})
}

func TestSDKSchemaCompatibility_Modes(t *testing.T) {
	t.Parallel()

	for _, tc := range compatCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestGlueClient(t, newTestHandler(t))

			_, err := c.CreateSchema(t.Context(), &gluesdk.CreateSchemaInput{
				SchemaName:       aws.String("s"),
				DataFormat:       tc.format,
				Compatibility:    tc.mode,
				SchemaDefinition: aws.String(tc.versions[0]),
			})
			require.NoError(t, err)

			for _, v := range tc.versions[1:] {
				_, err = registerVersionSDK(t, c, v)
				require.NoError(t, err, "seed version must be accepted")
			}

			out, err := registerVersionSDK(t, c, tc.candidate)
			assertInvalidInputIf(t, err, tc.wantErr)

			if tc.wantErr {
				list, lerr := c.ListSchemaVersions(t.Context(), &gluesdk.ListSchemaVersionsInput{
					SchemaId: &types.SchemaId{SchemaName: aws.String("s")},
				})
				require.NoError(t, lerr)
				assert.Len(t, list.Schemas, len(tc.versions), "rejected version must not be stored")

				return
			}

			assert.Equal(t, int64(len(tc.versions)+1), aws.ToInt64(out.VersionNumber))
			assert.Equal(t, types.SchemaVersionStatusAvailable, out.Status)
		})
	}
}

func TestSDKSchemaCompatibility_UpdatedModeApplies(t *testing.T) {
	t.Parallel()

	c := newTestGlueClient(t, newTestHandler(t))

	_, err := c.CreateSchema(t.Context(), &gluesdk.CreateSchemaInput{
		SchemaName: aws.String("s"), DataFormat: types.DataFormatAvro, Compatibility: types.CompatibilityNone,
		SchemaDefinition: aws.String(avroBase),
	})
	require.NoError(t, err)

	_, err = registerVersionSDK(t, c, avroReqZip)
	require.NoError(t, err)

	_, err = c.UpdateSchema(t.Context(), &gluesdk.UpdateSchemaInput{
		SchemaId:      &types.SchemaId{SchemaName: aws.String("s")},
		Compatibility: types.CompatibilityBackward,
	})
	require.NoError(t, err)

	_, err = registerVersionSDK(t, c, `{"type":"record","name":"P","fields":[`+
		`{"name":"first","type":"string"},{"name":"last","type":"string"},{"name":"email","type":"string"},`+
		`{"name":"phone","type":["null","string"],"default":null},{"name":"zip","type":"string"},`+
		`{"name":"country","type":"string"}]}`)
	assertInvalidInputIf(t, err, true)
}

func TestSDKRegisterSchemaVersion_IdenticalDefinitionReturnsExisting(t *testing.T) {
	t.Parallel()

	c := newTestGlueClient(t, newTestHandler(t))

	first, err := c.CreateSchema(t.Context(), &gluesdk.CreateSchemaInput{
		SchemaName: aws.String("s"), DataFormat: types.DataFormatAvro, Compatibility: types.CompatibilityBackward,
		SchemaDefinition: aws.String(avroBase),
	})
	require.NoError(t, err)

	again, err := registerVersionSDK(t, c, avroBase)
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(first.SchemaVersionId), aws.ToString(again.SchemaVersionId))
	assert.Equal(t, int64(1), aws.ToInt64(again.VersionNumber))
}

func TestSDKSchemaDefinitionValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		def     string
		format  types.DataFormat
		wantErr bool
	}{
		{"avro_valid_record", avroBase, types.DataFormatAvro, false},
		{"avro_valid_primitive", `"string"`, types.DataFormatAvro, false},
		{"avro_valid_union", `["null","string"]`, types.DataFormatAvro, false},
		{"avro_record_without_fields", `{"type":"record","name":"R"}`, types.DataFormatAvro, true},
		{"avro_record_without_name", `{"type":"record","fields":[]}`, types.DataFormatAvro, true},
		{
			"avro_unknown_type",
			`{"type":"record","name":"R","fields":[{"name":"a","type":"strng"}]}`,
			types.DataFormatAvro,
			true,
		},
		{"avro_enum_without_symbols", `{"type":"enum","name":"E"}`, types.DataFormatAvro, true},
		{"avro_not_json", `record R`, types.DataFormatAvro, true},
		{"json_valid", jsonClosed, types.DataFormatJson, false},
		{"json_unknown_type", `{"type":"strng"}`, types.DataFormatJson, true},
		{"json_bad_required", `{"type":"object","required":"a"}`, types.DataFormatJson, true},
		{"json_not_object", `[1]`, types.DataFormatJson, true},
		{"proto_valid", protoBase, types.DataFormatProtobuf, false},
		{"proto_valid_service", protoSvc2, types.DataFormatProtobuf, false},
		{
			"proto_duplicate_field_number",
			`syntax = "proto3"; message M { string a = 1; string b = 1; }`,
			types.DataFormatProtobuf,
			true,
		},
		{"proto_unterminated_message", `syntax = "proto3"; message M { string a = 1;`, types.DataFormatProtobuf, true},
		{"proto_missing_syntax", `message M { string a = 1; }`, types.DataFormatProtobuf, true},
		{"proto_bad_field_number", `syntax = "proto3"; message M { string a = x; }`, types.DataFormatProtobuf, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestGlueClient(t, newTestHandler(t))

			check, err := c.CheckSchemaVersionValidity(t.Context(), &gluesdk.CheckSchemaVersionValidityInput{
				DataFormat: tc.format, SchemaDefinition: aws.String(tc.def),
			})
			require.NoError(t, err)
			assert.Equal(t, !tc.wantErr, check.Valid)

			_, err = c.CreateSchema(t.Context(), &gluesdk.CreateSchemaInput{
				SchemaName: aws.String("s"), DataFormat: tc.format, SchemaDefinition: aws.String(tc.def),
			})
			assertInvalidInputIf(t, err, tc.wantErr)
		})
	}
}
