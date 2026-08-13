package cloudfrontkeyvaluestore

// Wire-shape JSON DTOs for the cloudfrontkeyvaluestore REST-JSON data plane,
// verified field-by-field against cloudfrontkeyvaluestore@v1.15.4
// serializers.go/deserializers.go's awsRestjson1_serializeOpDocument<Op>Input
// and awsRestjson1_deserializeOpDocument<Op>Output functions. Field names are
// PascalCase (this protocol does not lowerCamelCase JSON keys the way most
// other restJson1 services in this repo do -- verified directly against the
// generated (de)serializers, not assumed). ETag never appears in a body: on
// every op that returns one (PutKey/DeleteKey/UpdateKeys/DescribeKeyValueStore)
// it is an "ETag" response header, per each op's
// awsRestjson1_deserializeOpHttpBindings<Op>Output.

// getKeyOutput is GetKeyOutput's body. No ETag member exists on this shape.
type getKeyOutput struct {
	Key              string `json:"Key"`
	Value            string `json:"Value"`
	ItemCount        int32  `json:"ItemCount"`
	TotalSizeInBytes int64  `json:"TotalSizeInBytes"`
}

// putKeyInput is PutKeyInput's body (Key/KvsARN are URI path params, IfMatch
// is a header -- only Value travels in the JSON document per
// awsRestjson1_serializeOpDocumentPutKeyInput).
type putKeyInput struct {
	Value string `json:"Value"`
}

// mutateKeyOutput is the shared body shape of PutKeyOutput, DeleteKeyOutput,
// and UpdateKeysOutput -- all three carry only post-mutation store stats in
// the body; the new ETag is the response header.
type mutateKeyOutput struct {
	ItemCount        int32 `json:"ItemCount"`
	TotalSizeInBytes int64 `json:"TotalSizeInBytes"`
}

// keyValuePairJSON is ListKeysResponseListItem / PutKeyRequestListItem.
type keyValuePairJSON struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// deleteKeyItemJSON is DeleteKeyRequestListItem -- a bare {"Key": "..."},
// not a plain string, per awsRestjson1_serializeDocumentDeleteKeyRequestListItem.
type deleteKeyItemJSON struct {
	Key string `json:"Key"`
}

// updateKeysInput is UpdateKeysInput's body.
type updateKeysInput struct {
	Puts    []keyValuePairJSON  `json:"Puts"`
	Deletes []deleteKeyItemJSON `json:"Deletes"`
}

// listKeysOutput is ListKeysOutput's body. No ETag member exists on this shape.
type listKeysOutput struct {
	NextToken string             `json:"NextToken,omitempty"`
	Items     []keyValuePairJSON `json:"Items"`
}

// describeKeyValueStoreOutput is DescribeKeyValueStoreOutput's body.
// Created/LastModified are epoch-seconds JSON numbers (this protocol's
// timestamp binding, per smithytime.ParseEpochSeconds in the deserializer --
// unlike services/cloudfront's own REST-XML API, which uses RFC3339 strings).
// FailureReason is omitted: this emulator provisions synchronously and never
// reports a FAILED store.
type describeKeyValueStoreOutput struct {
	KvsARN           string  `json:"KvsARN"`
	Status           string  `json:"Status"`
	Created          float64 `json:"Created"`
	LastModified     float64 `json:"LastModified"`
	ItemCount        int32   `json:"ItemCount"`
	TotalSizeInBytes int64   `json:"TotalSizeInBytes"`
}

// awsErrorBody is the standard restJson1 error body: {"message": "..."},
// paired with the X-Amzn-ErrorType response header carrying the exception name.
type awsErrorBody struct {
	Message string `json:"message"`
}
