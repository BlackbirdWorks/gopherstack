package timestreamwrite

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

type createTableInput struct {
	Schema                       *schemaInput                  `json:"Schema,omitempty"`
	RetentionProperties          *retentionPropertiesInput     `json:"RetentionProperties,omitempty"`
	MagneticStoreWriteProperties *magneticStoreWritePropsInput `json:"MagneticStoreWriteProperties,omitempty"`
	DatabaseName                 string                        `json:"DatabaseName"`
	TableName                    string                        `json:"TableName"`
	Tags                         []tagInput                    `json:"Tags"`
}

type retentionPropertiesInput struct {
	MemoryStoreRetentionPeriodInHours  int64 `json:"MemoryStoreRetentionPeriodInHours"`
	MagneticStoreRetentionPeriodInDays int64 `json:"MagneticStoreRetentionPeriodInDays"`
}

type s3ConfigInput struct {
	BucketName       string `json:"BucketName,omitempty"`
	ObjectKeyPrefix  string `json:"ObjectKeyPrefix,omitempty"`
	EncryptionOption string `json:"EncryptionOption,omitempty"`
	KmsKeyID         string `json:"KmsKeyId,omitempty"`
}

type magneticStoreRejectedDataLocationInput struct {
	S3Configuration *s3ConfigInput `json:"S3Configuration,omitempty"`
}

type magneticStoreWritePropsInput struct {
	MagneticStoreRejectedDataLocation *magneticStoreRejectedDataLocationInput `json:"MagneticStoreRejectedDataLocation,omitempty"` //nolint:lll // AWS field name is inherently long
	EnableMagneticStoreWrites         bool                                    `json:"EnableMagneticStoreWrites"`
}

type partitionKeyInput struct {
	Type                string `json:"Type"`
	Name                string `json:"Name,omitempty"`
	EnforcementInRecord string `json:"EnforcementInRecord,omitempty"`
}

type schemaInput struct {
	CompositePartitionKey []partitionKeyInput `json:"CompositePartitionKey,omitempty"`
}

type tableInput struct {
	DatabaseName string `json:"DatabaseName"`
	TableName    string `json:"TableName"`
}

type updateTableInput struct {
	Schema                       *schemaInput                  `json:"Schema,omitempty"`
	RetentionProperties          *retentionPropertiesInput     `json:"RetentionProperties,omitempty"`
	MagneticStoreWriteProperties *magneticStoreWritePropsInput `json:"MagneticStoreWriteProperties,omitempty"`
	DatabaseName                 string                        `json:"DatabaseName"`
	TableName                    string                        `json:"TableName"`
}

type tableOutput struct {
	Table tableView `json:"Table"`
}

type partitionKeyView struct {
	Type                string `json:"Type"`
	Name                string `json:"Name,omitempty"`
	EnforcementInRecord string `json:"EnforcementInRecord,omitempty"`
}

type schemaView struct {
	CompositePartitionKey []partitionKeyView `json:"CompositePartitionKey,omitempty"`
}

type tableView struct {
	Schema                       *schemaView                   `json:"Schema,omitempty"`
	RetentionProperties          *retentionPropertiesInput     `json:"RetentionProperties,omitempty"`
	MagneticStoreWriteProperties *magneticStoreWritePropsInput `json:"MagneticStoreWriteProperties,omitempty"`
	Arn                          string                        `json:"Arn"`
	DatabaseName                 string                        `json:"DatabaseName"`
	TableName                    string                        `json:"TableName"`
	TableStatus                  string                        `json:"TableStatus"`
	CreationTime                 float64                       `json:"CreationTime"`
	LastUpdatedTime              float64                       `json:"LastUpdatedTime"`
}

type listTablesInput struct {
	DatabaseName string `json:"DatabaseName"`
	NextToken    string `json:"NextToken"`
	MaxResults   int    `json:"MaxResults"`
}

type listTablesOutput struct {
	NextToken string      `json:"NextToken,omitempty"`
	Tables    []tableView `json:"Tables"`
}

// buildCreateTableInput converts handler-level optional table-property inputs into a
// *CreateTableInput for the backend.  Returns nil when all inputs are nil.
func buildCreateTableInput(
	rp *retentionPropertiesInput,
	mswp *magneticStoreWritePropsInput,
	sc *schemaInput,
) *CreateTableInput {
	if rp == nil && mswp == nil && sc == nil {
		return nil
	}

	inp := &CreateTableInput{}

	if rp != nil {
		inp.RetentionProperties = &RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  rp.MemoryStoreRetentionPeriodInHours,
			MagneticStoreRetentionPeriodInDays: rp.MagneticStoreRetentionPeriodInDays,
		}
	}

	if mswp != nil {
		backendMSWP := &MagneticStoreWriteProperties{
			EnableMagneticStoreWrites: mswp.EnableMagneticStoreWrites,
		}

		if mswp.MagneticStoreRejectedDataLocation != nil {
			loc := mswp.MagneticStoreRejectedDataLocation
			backendLoc := &MagneticStoreRejectedDataLocation{}

			if loc.S3Configuration != nil {
				backendLoc.S3Configuration = &S3Configuration{
					BucketName:       loc.S3Configuration.BucketName,
					ObjectKeyPrefix:  loc.S3Configuration.ObjectKeyPrefix,
					EncryptionOption: loc.S3Configuration.EncryptionOption,
					KmsKeyID:         loc.S3Configuration.KmsKeyID,
				}
			}

			backendMSWP.MagneticStoreRejectedDataLocation = backendLoc
		}

		inp.MagneticStoreWriteProperties = backendMSWP
	}

	if sc != nil && len(sc.CompositePartitionKey) > 0 {
		keys := make([]PartitionKey, 0, len(sc.CompositePartitionKey))
		for _, pk := range sc.CompositePartitionKey {
			keys = append(keys, PartitionKey(pk))
		}

		inp.Schema = &Schema{CompositePartitionKey: keys}
	}

	return inp
}

func toTableView(tbl *Table) tableView {
	v := tableView{
		Arn:             tbl.ARN,
		CreationTime:    float64(tbl.CreationTime.Unix()),
		DatabaseName:    tbl.DatabaseName,
		LastUpdatedTime: float64(tbl.LastUpdatedTime.Unix()),
		TableName:       tbl.TableName,
		TableStatus:     tbl.TableStatus,
	}

	if tbl.RetentionProperties != nil {
		v.RetentionProperties = &retentionPropertiesInput{
			MemoryStoreRetentionPeriodInHours:  tbl.RetentionProperties.MemoryStoreRetentionPeriodInHours,
			MagneticStoreRetentionPeriodInDays: tbl.RetentionProperties.MagneticStoreRetentionPeriodInDays,
		}
	}

	if tbl.MagneticStoreWriteProperties != nil {
		mswp := &magneticStoreWritePropsInput{
			EnableMagneticStoreWrites: tbl.MagneticStoreWriteProperties.EnableMagneticStoreWrites,
		}

		if tbl.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation != nil {
			loc := tbl.MagneticStoreWriteProperties.MagneticStoreRejectedDataLocation
			inputLoc := &magneticStoreRejectedDataLocationInput{}

			if loc.S3Configuration != nil {
				inputLoc.S3Configuration = &s3ConfigInput{
					BucketName:       loc.S3Configuration.BucketName,
					ObjectKeyPrefix:  loc.S3Configuration.ObjectKeyPrefix,
					EncryptionOption: loc.S3Configuration.EncryptionOption,
					KmsKeyID:         loc.S3Configuration.KmsKeyID,
				}
			}

			mswp.MagneticStoreRejectedDataLocation = inputLoc
		}

		v.MagneticStoreWriteProperties = mswp
	}

	if tbl.Schema != nil && len(tbl.Schema.CompositePartitionKey) > 0 {
		keys := make([]partitionKeyView, 0, len(tbl.Schema.CompositePartitionKey))
		for _, pk := range tbl.Schema.CompositePartitionKey {
			keys = append(keys, partitionKeyView(pk))
		}

		v.Schema = &schemaView{CompositePartitionKey: keys}
	}

	return v
}

func (h *Handler) handleCreateTable(
	_ context.Context,
	in *createTableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	// Validate table name format and length per AWS API constraints.
	if err := validateTableName(in.TableName); err != nil {
		return nil, err
	}

	// Validate retention properties ranges per AWS API constraints.
	if err := validateRetentionPropertiesInput(in.RetentionProperties); err != nil {
		return nil, err
	}

	// Validate schema partition key configuration per AWS API constraints.
	if err := validateSchemaPartitionKeys(in.Schema); err != nil {
		return nil, err
	}

	// Validate tags per AWS API constraints.
	if err := validateTagInputs(in.Tags); err != nil {
		return nil, err
	}

	tags := tagsFromInput(in.Tags)

	inp := buildCreateTableInput(in.RetentionProperties, in.MagneticStoreWriteProperties, in.Schema)

	tbl, err := h.Backend.CreateTable(in.DatabaseName, in.TableName, tags, inp)
	if err != nil {
		return nil, err
	}

	return &tableOutput{Table: toTableView(tbl)}, nil
}

func (h *Handler) handleDescribeTable(
	_ context.Context,
	in *tableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	tbl, err := h.Backend.DescribeTable(in.DatabaseName, in.TableName)
	if err != nil {
		return nil, err
	}

	return &tableOutput{Table: toTableView(tbl)}, nil
}

func (h *Handler) handleListTables(
	_ context.Context,
	in *listTablesInput,
) (*listTablesOutput, error) {
	if in.DatabaseName == "" {
		return nil, fmt.Errorf("%w: DatabaseName is required", errInvalidRequest)
	}

	tbls, err := h.Backend.ListTables(in.DatabaseName)
	if err != nil {
		return nil, err
	}

	pg := page.New(tbls, in.NextToken, in.MaxResults, defaultTimestreamMaxResults)
	views := make([]tableView, 0, len(pg.Data))

	for i := range pg.Data {
		views = append(views, toTableView(&pg.Data[i]))
	}

	return &listTablesOutput{Tables: views, NextToken: pg.Next}, nil
}

func (h *Handler) handleDeleteTable(
	_ context.Context,
	in *tableInput,
) (*emptyOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	if err := h.Backend.DeleteTable(in.DatabaseName, in.TableName); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleUpdateTable(
	_ context.Context,
	in *updateTableInput,
) (*tableOutput, error) {
	if in.DatabaseName == "" || in.TableName == "" {
		return nil, fmt.Errorf("%w: DatabaseName and TableName are required", errInvalidRequest)
	}

	// Validate retention properties ranges per AWS API constraints.
	if err := validateRetentionPropertiesInput(in.RetentionProperties); err != nil {
		return nil, err
	}

	// Validate schema partition key configuration per AWS API constraints.
	if err := validateSchemaPartitionKeys(in.Schema); err != nil {
		return nil, err
	}

	var inp *UpdateTableInput
	cti := buildCreateTableInput(in.RetentionProperties, in.MagneticStoreWriteProperties, in.Schema)

	if cti != nil {
		inp = &UpdateTableInput{
			RetentionProperties:          cti.RetentionProperties,
			MagneticStoreWriteProperties: cti.MagneticStoreWriteProperties,
			Schema:                       cti.Schema,
		}
	}

	tbl, err := h.Backend.UpdateTable(in.DatabaseName, in.TableName, inp)
	if err != nil {
		return nil, err
	}

	return &tableOutput{Table: toTableView(tbl)}, nil
}
