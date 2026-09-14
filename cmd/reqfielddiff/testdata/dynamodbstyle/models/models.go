// Package models is a fixture reproducing dynamodb/models's real shape:
// the wire-format request struct and its ToSDK<Op>Input converter live
// here, one package away from the handler that dispatches to it.
package models

type CreateTableInput struct {
	TableName      string `json:"TableName"`
	KMSMasterKeyId string `json:"KMSMasterKeyId,omitempty"` //nolint:revive,stylecheck // matches real dynamodb wire field name
}

type CreateTableOutput struct{}

type SDKCreateTableInput struct{}

type SDKCreateTableOutput struct{}

func ToSDKCreateTableInput(input *CreateTableInput) *SDKCreateTableInput {
	return &SDKCreateTableInput{}
}

func FromSDKCreateTableOutput(out *SDKCreateTableOutput) *CreateTableOutput {
	return &CreateTableOutput{}
}
