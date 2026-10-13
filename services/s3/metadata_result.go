package s3

import (
	"encoding/xml"
	"strings"

	"github.com/google/uuid"
)

const (
	metadataTableBucketName = "aws-s3"
	metadataStateEnabled    = "ENABLED"
	metadataTableActive     = "ACTIVE"
)

type metadataRecordExpirationXML struct {
	Days       *int32 `xml:"Days,omitempty"`
	Expiration string `xml:"Expiration,omitempty"`
}

type metadataTableCfgXML struct {
	RecordExpiration   *metadataRecordExpirationXML `xml:"RecordExpiration"`
	ConfigurationState string                       `xml:"ConfigurationState"`
	Role               string                       `xml:"Role"`
}

type metadataConfigBodyXML struct {
	Journal    *metadataTableCfgXML `xml:"JournalTableConfiguration"`
	Inventory  *metadataTableCfgXML `xml:"InventoryTableConfiguration"`
	Annotation *metadataTableCfgXML `xml:"AnnotationTableConfiguration"`
}

type metadataDestinationResultXML struct {
	TableBucketArn  string `xml:"TableBucketArn"`
	TableBucketType string `xml:"TableBucketType"`
	TableNamespace  string `xml:"TableNamespace"`
}

type metadataTableResultXML struct {
	RecordExpiration   *metadataRecordExpirationXML `xml:"RecordExpiration,omitempty"`
	ConfigurationState string                       `xml:"ConfigurationState,omitempty"`
	Role               string                       `xml:"Role,omitempty"`
	TableArn           string                       `xml:"TableArn,omitempty"`
	TableName          string                       `xml:"TableName,omitempty"`
	TableStatus        string                       `xml:"TableStatus,omitempty"`
}

type getBucketMetadataConfigurationResponse struct {
	Result struct {
		Annotation  *metadataTableResultXML      `xml:"AnnotationTableConfigurationResult,omitempty"`
		Inventory   *metadataTableResultXML      `xml:"InventoryTableConfigurationResult,omitempty"`
		Journal     *metadataTableResultXML      `xml:"JournalTableConfigurationResult,omitempty"`
		Destination metadataDestinationResultXML `xml:"DestinationResult"`
	} `xml:"MetadataConfigurationResult"`
	XMLName xml.Name `xml:"GetBucketMetadataConfigurationResult"`
	Xmlns   string   `xml:"xmlns,attr"`
}

func overlayMetadataTableCfg(base, upd *metadataTableCfgXML) *metadataTableCfgXML {
	if upd == nil {
		return base
	}

	if base == nil {
		base = &metadataTableCfgXML{}
	}

	if upd.RecordExpiration != nil {
		base.RecordExpiration = upd.RecordExpiration
	}

	if upd.ConfigurationState != "" {
		base.ConfigurationState = upd.ConfigurationState
	}

	if upd.Role != "" {
		base.Role = upd.Role
	}

	return base
}

func parseMetadataTableCfg(raw string) *metadataTableCfgXML {
	if raw == "" {
		return nil
	}

	var cfg metadataTableCfgXML
	if xml.Unmarshal([]byte(raw), &cfg) != nil {
		return nil
	}

	return &cfg
}

func metadataTableArn(tableBucketArn, bucket, table string) string {
	id := uuid.NewSHA1(uuid.NameSpaceURL, []byte(bucket+"/"+table))

	return tableBucketArn + "/table/" + id.String()
}

// buildMetadataConfigurationResult renders the server-computed
// MetadataConfigurationResult (s3 types.MetadataConfigurationResult) from the
// stored create body plus any table updates. V2 configurations live in the AWS
// managed "aws-s3" table bucket under namespace "b_<bucket>".
func buildMetadataConfigurationResult(
	region, account, bucket, createXML, inventoryUpd, journalUpd, annotationUpd string,
) (string, error) {
	var body metadataConfigBodyXML
	if err := xml.Unmarshal([]byte(createXML), &body); err != nil {
		return createXML, nil //nolint:nilerr // pre-V2 free-form bodies are returned as stored
	}

	if body.Journal == nil && body.Inventory == nil && body.Annotation == nil {
		return createXML, nil
	}

	tableBucketArn := "arn:aws:s3tables:" + region + ":" + account + ":bucket/" + metadataTableBucketName

	resp := getBucketMetadataConfigurationResponse{Xmlns: xmlNamespaceS3}
	resp.Result.Destination = metadataDestinationResultXML{
		TableBucketArn:  tableBucketArn,
		TableBucketType: "aws",
		TableNamespace:  "b_" + bucket,
	}

	if j := overlayMetadataTableCfg(body.Journal, parseMetadataTableCfg(journalUpd)); j != nil {
		resp.Result.Journal = &metadataTableResultXML{
			RecordExpiration: j.RecordExpiration,
			TableName:        "journal",
			TableArn:         metadataTableArn(tableBucketArn, bucket, "journal"),
			TableStatus:      metadataTableActive,
		}
	}

	if i := overlayMetadataTableCfg(body.Inventory, parseMetadataTableCfg(inventoryUpd)); i != nil {
		res := &metadataTableResultXML{ConfigurationState: i.ConfigurationState}
		if strings.EqualFold(i.ConfigurationState, metadataStateEnabled) {
			res.TableName = "inventory"
			res.TableArn = metadataTableArn(tableBucketArn, bucket, "inventory")
			res.TableStatus = metadataTableActive
		}

		resp.Result.Inventory = res
	}

	if a := overlayMetadataTableCfg(body.Annotation, parseMetadataTableCfg(annotationUpd)); a != nil {
		res := &metadataTableResultXML{ConfigurationState: a.ConfigurationState, Role: a.Role}
		if strings.EqualFold(a.ConfigurationState, metadataStateEnabled) {
			res.TableStatus = metadataTableActive
		}

		resp.Result.Annotation = res
	}

	out, err := xml.Marshal(resp)
	if err != nil {
		return "", err
	}

	return xml.Header + string(out), nil
}
