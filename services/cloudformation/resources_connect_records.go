package cloudformation

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	connectbackend "github.com/blackbirdworks/gopherstack/services/connect"
)

// ---- DataTableRecord ----

func (c *connectCall) recordValues(key string) []connectbackend.DataTableRecordValue {
	items, _ := c.wire["DataTableRecord"].(map[string]any)

	list, _ := items[key].([]any)
	out := make([]connectbackend.DataTableRecordValue, 0, len(list))

	for _, e := range list {
		m, _ := e.(map[string]any)
		id, _ := m["AttributeId"].(string)
		val, _ := m["AttributeValue"].(string)
		out = append(out, connectbackend.DataTableRecordValue{AttributeID: id, Value: val})
	}

	return out
}

func (c *connectCall) recordInput(recordID string) *connectbackend.DataTableRecordInput {
	return &connectbackend.DataTableRecordInput{
		InstanceID:    lastPathSegment(c.instance()),
		TableID:       lastPathSegment(c.str("DataTableArn")),
		RecordID:      recordID,
		PrimaryValues: c.recordValues("PrimaryValues"),
		Values:        c.recordValues("Values"),
	}
}

// createConnectDataTableRecord stores a record. Ref is "<instance ID>|<data table ID>|<record ID>"; GetAtt RecordId
// is the record ID.
func createConnectDataTableRecord(c *connectCall) (string, error) {
	in := c.recordInput("")

	id, err := c.be.PutDataTableRecord(c.ctx, in)
	if err != nil {
		return "", err
	}

	return connectCompositeID(in.InstanceID, in.TableID, id), nil
}

func updateConnectDataTableRecord(c *connectCall, physicalID string) error {
	parts := strings.Split(physicalID, "|")
	if len(parts) != connectCompositeParts {
		return fmt.Errorf("%w: record ID %q", errUnsupportedCFNValue, physicalID)
	}

	_, err := c.be.PutDataTableRecord(c.ctx, c.recordInput(parts[2]))

	return err
}

func deleteConnectDataTableRecord(c *connectCall, physicalID string) error {
	parts := strings.Split(physicalID, "|")
	if len(parts) != connectCompositeParts {
		return nil
	}

	return c.be.DeleteDataTableRecord(c.ctx, parts[0], parts[1], parts[2])
}

// ---- Notification ----

// notificationLocales maps the property names of NotificationContent to the LocaleCode enum values.
func notificationLocales() map[string]string {
	return map[string]string{
		"EnUS": "en_US", "DeDE": "de_DE", "EsES": "es_ES", "FrFR": "fr_FR", "IdID": "id_ID", "ItIT": "it_IT",
		"JaJP": "ja_JP", "KoKR": "ko_KR", "PtBR": "pt_BR", "ZhCN": "zh_CN", "ZhTW": "zh_TW",
	}
}

func (c *connectCall) notificationContent() (map[string]string, error) {
	raw, _ := c.wire["Content"].(map[string]any)
	out := make(map[string]string, len(raw))
	locales := notificationLocales()

	for k, v := range raw {
		code, ok := locales[k]
		if !ok {
			return nil, fmt.Errorf("%w: Content locale %q", errUnsupportedCFNValue, k)
		}

		text, _ := v.(string)
		out[code] = text
	}

	return out, nil
}

func (c *connectCall) stashNotification(notificationARN string) {
	inst, id := splitConnectARN(notificationARN)

	n, err := c.be.DescribeNotification(inst, id)
	if err != nil {
		return
	}

	c.stash(map[string]string{"CreatedAt": connectTime(n.CreatedAt)})
}

func createConnectNotification(c *connectCall) (string, error) {
	content, err := c.notificationContent()
	if err != nil {
		return "", err
	}

	in := connectbackend.CreateNotificationInput{Content: content, Priority: c.str("Priority"), Tags: c.tags(),
		InstanceID: c.instance()}

	if list, ok := c.wire["Recipients"].([]any); ok {
		for _, r := range list {
			if s, isStr := r.(string); isStr {
				in.Recipients = append(in.Recipients, s)
			}
		}
	}

	if s := c.str("ExpiresAt"); s != "" {
		secs, perr := strconv.ParseInt(s, 10, 64)
		if perr != nil {
			return "", fmt.Errorf("%w: ExpiresAt %q", errUnsupportedCFNValue, s)
		}

		t := awstime.NewEpochTime(time.Unix(secs, 0))
		in.ExpiresAt = &t
	}

	out, err := c.be.CreateNotification(c.ctx, &in)
	if err != nil {
		return "", err
	}

	c.stashNotification(out.NotificationArn)

	return out.NotificationArn, nil
}

func updateConnectNotification(c *connectCall, physicalID string) error {
	content, err := c.notificationContent()
	if err != nil {
		return err
	}

	in := connectbackend.UpdateNotificationContentInput{Content: content}
	in.InstanceID, in.NotificationID = splitConnectARN(physicalID)

	if _, err = c.be.UpdateNotificationContent(c.ctx, &in); err != nil {
		return err
	}

	return c.syncTags(physicalID)
}

func deleteConnectNotification(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteNotification(inst, id)

	return err
}

// ---- TestCase ----

func (c *connectCall) stashTestCase(testCaseARN string) {
	inst, id := splitConnectARN(testCaseARN)

	tc, err := c.be.DescribeTestCaseVersion(inst, id, "")
	if err != nil {
		return
	}

	c.stash(
		map[string]string{
			connectLastModified:  connectTime(tc.LastModifiedTime),
			connectLastModRegion: tc.LastModifiedRegion,
		},
	)
}

// testCaseEntryPoint reads EntryPoint; the flow IDs may be given as ARNs. The bool reports whether it was given.
func (c *connectCall) testCaseEntryPoint() (connectbackend.TestCaseEntryPoint, bool, error) {
	var ep connectbackend.TestCaseEntryPoint

	raw, ok := c.wire["EntryPoint"].(map[string]any)
	if !ok {
		return ep, false, nil
	}

	for _, key := range []string{"VoiceCallEntryPointParameters", "ChatEntryPointParameters"} {
		if p, isMap := raw[key].(map[string]any); isMap {
			if id, isStr := p["FlowId"].(string); isStr {
				p["FlowId"] = lastPathSegment(id)
			}
		}
	}

	if err := decodeCFNProps(&ep, raw, nil, nil); err != nil {
		return ep, false, err
	}

	return ep, true, nil
}

// entryPoint is the EntryPoint property as the backend takes it: nil when absent.
func (c *connectCall) entryPoint() (*connectbackend.TestCaseEntryPoint, error) {
	ep, given, err := c.testCaseEntryPoint()
	if err != nil || !given {
		return nil, err
	}

	return &ep, nil
}

func createConnectTestCase(c *connectCall) (string, error) {
	ep, err := c.entryPoint()
	if err != nil {
		return "", err
	}

	in := connectbackend.CreateTestCaseInput{
		Name:        c.str("Name"),
		Description: c.str("Description"),
		Content:     c.str("Content"),
		InitializationData: c.str(
			"InitializationData",
		),
		Status:     c.str(connectStatusProp),
		EntryPoint: ep,
		Tags:       c.tags(),

		InstanceID: c.instance(),
	}

	out, err := c.be.CreateTestCase(c.ctx, &in)
	if err != nil {
		return "", err
	}

	c.stashTestCase(out.TestCaseArn)

	return out.TestCaseArn, nil
}

func updateConnectTestCase(c *connectCall, physicalID string) error {
	ep, err := c.entryPoint()
	if err != nil {
		return err
	}

	in := connectbackend.UpdateTestCaseInput{
		Name: new(c.str("Name")), Description: new(c.str("Description")), Content: new(c.str("Content")),
		InitializationData: new(c.str("InitializationData")), Status: c.str(connectStatusProp), EntryPoint: ep,
	}
	in.InstanceID, in.TestCaseID = splitConnectARN(physicalID)

	if _, err = c.be.UpdateTestCase(c.ctx, &in); err != nil {
		return err
	}

	c.stashTestCase(physicalID)

	return c.syncTags(physicalID)
}

func deleteConnectTestCase(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteTestCase(inst, id)

	return err
}

// ---- Metric ----

func (c *connectCall) stashMetric(metricARN string) {
	inst, id := splitConnectARN(metricARN)

	m, err := c.be.DescribeMetric(inst, id)
	if err != nil {
		return
	}

	c.stash(map[string]string{
		"Category":                        m.Category,
		connectCreatedTime:                connectTime(m.CreatedTime),
		"Type":                            m.Type,
		connectLastModified:               connectTime(m.LastModifiedTime),
		connectLastModRegion:              m.LastModifiedRegion,
		"SupportsCustomCalculation":       strconv.FormatBool(m.SupportsCustomCalc),
		"SupportsPreaggregateCalculation": strconv.FormatBool(m.SupportsPreaggregate),
	})
}

// createConnectMetric creates a customer-managed metric. Ref and GetAtt MetricArn return the metric ARN.
func createConnectMetric(c *connectCall) (string, error) {
	var in connectbackend.CreateMetricInput
	if err := c.decode(&in); err != nil {
		return "", err
	}

	in.InstanceID = c.instance()

	out, err := c.be.CreateMetric(c.ctx, &in)
	if err != nil {
		return "", err
	}

	c.stashMetric(out.MetricArn)

	return out.MetricArn, nil
}

func updateConnectMetric(c *connectCall, physicalID string) error {
	var content connectbackend.UpdateMetricContentInput
	if err := c.decode(&content, connectTagsProp, connectStatusProp, "Name", "Description"); err != nil {
		return err
	}

	content.InstanceID, content.MetricID = splitConnectARN(physicalID)

	if _, err := c.be.UpdateMetricContent(c.ctx, &content); err != nil {
		return err
	}

	meta := connectbackend.UpdateMetricMetadataInput{Name: new(c.str("Name")), Description: new(c.str("Description"))}
	meta.InstanceID, meta.MetricID = content.InstanceID, content.MetricID

	if _, err := c.be.UpdateMetricMetadata(c.ctx, &meta); err != nil {
		return err
	}

	c.stashMetric(physicalID)

	return c.syncTags(physicalID)
}

func deleteConnectMetric(c *connectCall, physicalID string) error {
	inst, id := splitConnectARN(physicalID)
	_, err := c.be.DeleteMetric(inst, id)

	return err
}

// ---- UserHierarchyStructure ----

func (c *connectCall) hierarchyUpdate() (*connectbackend.UpdateHierarchyStructureInput, error) {
	var in connectbackend.UpdateHierarchyStructureInput

	raw, _ := c.wire["UserHierarchyStructure"].(map[string]any)
	if err := decodeCFNProps(&in.HierarchyStructure, raw, nil, nil); err != nil {
		return nil, err
	}

	in.InstanceID = c.instance()

	return &in, nil
}

// createConnectHierarchyStructure names the hierarchy levels. Ref and GetAtt UserHierarchyStructureArn return the
// instance ARN: the API has no identifier of its own for the structure.
func createConnectHierarchyStructure(c *connectCall) (string, error) {
	in, err := c.hierarchyUpdate()
	if err != nil {
		return "", err
	}

	if _, err = c.be.UpdateUserHierarchyStructure(c.ctx, in); err != nil {
		return "", err
	}

	return c.instance(), nil
}

func updateConnectHierarchyStructure(c *connectCall, _ string) error {
	in, err := c.hierarchyUpdate()
	if err != nil {
		return err
	}

	_, err = c.be.UpdateUserHierarchyStructure(c.ctx, in)

	return err
}
