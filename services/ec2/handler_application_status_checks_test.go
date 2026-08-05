package ec2_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApplicationStatusChecksHandler_CheckLifecycle(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := postForm(t, h, "Action=CreateApplicationStatusCheck&Version=2016-11-15"+
		"&Protocol=http&Port=80&Path=%2Fhealth&FailureThreshold=3"+
		"&TagSpecification.1.ResourceType=application-status-check"+
		"&TagSpecification.1.Tag.1.Key=team&TagSpecification.1.Tag.1.Value=infra")
	require.Equal(t, http.StatusOK, createRec.Code)
	createBody := createRec.Body.String()
	assert.Contains(t, createBody, "<CreateApplicationStatusCheckResponse")
	assert.Contains(t, createBody, "<protocol>http</protocol>")
	assert.Contains(t, createBody, "<port>80</port>")
	assert.Contains(t, createBody, "<path>/health</path>")
	assert.Contains(t, createBody, "<failureThreshold>3</failureThreshold>")
	// Real documented defaults for fields not specified.
	assert.Contains(t, createBody, "<aggregation>included</aggregation>")
	assert.Contains(t, createBody, "<interval>60</interval>")
	assert.Contains(t, createBody, "<timeout>6</timeout>")
	assert.Contains(t, createBody, "<successThreshold>5</successThreshold>")
	assert.Contains(t, createBody, "<statusCodeMatcher>200</statusCodeMatcher>")
	assert.Contains(t, createBody, "<key>team</key>")
	assert.Contains(t, createBody, "<value>infra</value>")
	checkID := extractTag(t, createBody, "applicationStatusCheckId")
	assert.Contains(t, checkID, "asc-")

	modifyRec := postForm(t, h, fmt.Sprintf(
		"Action=ModifyApplicationStatusCheck&Version=2016-11-15"+
			"&ApplicationStatusCheckId=%s&Port=8080",
		checkID,
	))
	require.Equal(t, http.StatusOK, modifyRec.Code)
	modifyBody := modifyRec.Body.String()
	assert.Contains(t, modifyBody, "<port>8080</port>")
	// Unset fields retain their value.
	assert.Contains(t, modifyBody, "<path>/health</path>")

	descRec := postForm(t, h, "Action=DescribeApplicationStatusChecks&Version=2016-11-15")
	require.Equal(t, http.StatusOK, descRec.Code)
	assert.Contains(t, descRec.Body.String(), checkID)

	deleteRec := postForm(t, h, fmt.Sprintf(
		"Action=DeleteApplicationStatusCheck&Version=2016-11-15&ApplicationStatusCheckId=%s",
		checkID,
	))
	require.Equal(t, http.StatusOK, deleteRec.Code)

	// Excluded from a default Describe...
	descAfterDeleteRec := postForm(t, h, "Action=DescribeApplicationStatusChecks&Version=2016-11-15")
	require.Equal(t, http.StatusOK, descAfterDeleteRec.Code)
	assert.NotContains(t, descAfterDeleteRec.Body.String(), checkID)

	// ...but still visible with IncludeAll=true.
	descIncludeAllRec := postForm(t, h, "Action=DescribeApplicationStatusChecks&Version=2016-11-15&IncludeAll=true")
	require.Equal(t, http.StatusOK, descIncludeAllRec.Code)
	assert.Contains(t, descIncludeAllRec.Body.String(), checkID)

	notFoundRec := postForm(t, h, fmt.Sprintf(
		"Action=ModifyApplicationStatusCheck&Version=2016-11-15&ApplicationStatusCheckId=%s&Port=1",
		checkID,
	))
	require.Equal(t, http.StatusBadRequest, notFoundRec.Code)
	assert.Contains(t, notFoundRec.Body.String(), "InvalidApplicationStatusCheckId.NotFound")
}

func TestApplicationStatusChecksHandler_CreateValidationFailures(t *testing.T) {
	t.Parallel()

	h := newHandler()

	tests := []struct {
		name       string
		body       string
		wantErrMsg string
	}{
		{
			name:       "missing Protocol",
			body:       "Action=CreateApplicationStatusCheck&Version=2016-11-15&Port=80",
			wantErrMsg: "InvalidParameterValue",
		},
		{
			name:       "missing Port",
			body:       "Action=CreateApplicationStatusCheck&Version=2016-11-15&Protocol=http",
			wantErrMsg: "InvalidParameterValue",
		},
		{
			name:       "invalid Protocol",
			body:       "Action=CreateApplicationStatusCheck&Version=2016-11-15&Protocol=ftp&Port=80",
			wantErrMsg: "InvalidParameterValue",
		},
		{
			name: "Timeout not less than Interval",
			body: "Action=CreateApplicationStatusCheck&Version=2016-11-15" +
				"&Protocol=http&Port=80&Timeout=60&Interval=60",
			wantErrMsg: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := postForm(t, h, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErrMsg)
		})
	}
}

func TestApplicationStatusChecksHandler_AssociateDisassociateAndSuppression(t *testing.T) {
	t.Parallel()

	h := newHandler()

	runRec := postForm(t, h, "Action=RunInstances&Version=2016-11-15"+
		"&ImageId=ami-123&InstanceType=t2.micro&MinCount=1&MaxCount=1")
	require.Equal(t, http.StatusOK, runRec.Code)
	instID := extractTag(t, runRec.Body.String(), "instanceId")

	createRec := postForm(t, h, "Action=CreateApplicationStatusCheck&Version=2016-11-15&Protocol=http&Port=80")
	checkID := extractTag(t, createRec.Body.String(), "applicationStatusCheckId")

	// Both InstanceId and TargetTagAssociation specified -> InvalidParameterCombination.
	comboRec := postForm(t, h, fmt.Sprintf(
		"Action=AssociateApplicationStatusCheck&Version=2016-11-15"+
			"&ApplicationStatusCheckId=%s&InstanceId.1=%s"+
			"&TargetTagAssociation.1.Key=env&TargetTagAssociation.1.Value=prod",
		checkID, instID,
	))
	require.Equal(t, http.StatusBadRequest, comboRec.Code)
	assert.Contains(t, comboRec.Body.String(), "InvalidParameterCombination")

	assocRec := postForm(t, h, fmt.Sprintf(
		"Action=AssociateApplicationStatusCheck&Version=2016-11-15"+
			"&ApplicationStatusCheckId=%s&InstanceId.1=%s",
		checkID, instID,
	))
	require.Equal(t, http.StatusOK, assocRec.Code)
	assocBody := assocRec.Body.String()
	assert.Contains(t, assocBody, "<AssociateApplicationStatusCheckResponse")
	assert.Contains(t, assocBody, "<associationType>INSTANCE_ID</associationType>")
	assert.Contains(t, assocBody, fmt.Sprintf("<associationValue>%s</associationValue>", instID))

	descAssocRec := postForm(t, h, fmt.Sprintf(
		"Action=DescribeApplicationStatusCheckAssociations&Version=2016-11-15&ApplicationStatusCheckId.1=%s",
		checkID,
	))
	require.Equal(t, http.StatusOK, descAssocRec.Code)
	descAssocBody := descAssocRec.Body.String()
	assert.Contains(t, descAssocBody, "<associationType>instance-id</associationType>")
	assert.Contains(t, descAssocBody, fmt.Sprintf("<value>%s</value>", instID))

	// Before suppression: real, non-fabricated status.
	statusRec := postForm(t, h, fmt.Sprintf(
		"Action=DescribeApplicationStatus&Version=2016-11-15&InstanceId.1=%s", instID,
	))
	require.Equal(t, http.StatusOK, statusRec.Code)
	assert.Contains(t, statusRec.Body.String(), "<status>insufficient-data</status>")
	assert.NotContains(t, statusRec.Body.String(), "<status>ok</status>")
	assert.NotContains(t, statusRec.Body.String(), "<status>impaired</status>")

	suppressRec := postForm(t, h, fmt.Sprintf(
		"Action=EnableApplicationStatusCheckSuppression&Version=2016-11-15"+
			"&InstanceId.1=%s&DurationSeconds=300",
		instID,
	))
	require.Equal(t, http.StatusOK, suppressRec.Code)
	suppressBody := suppressRec.Body.String()
	assert.Contains(t, suppressBody, "<EnableApplicationStatusCheckSuppressionResponse")
	assert.Contains(t, suppressBody, fmt.Sprintf("<instanceId>%s</instanceId>", instID))
	assert.Contains(t, suppressBody, "<resumeAt>")

	statusAfterSuppressRec := postForm(t, h, fmt.Sprintf(
		"Action=DescribeApplicationStatus&Version=2016-11-15&InstanceId.1=%s", instID,
	))
	require.Equal(t, http.StatusOK, statusAfterSuppressRec.Code)
	assert.Contains(t, statusAfterSuppressRec.Body.String(), "<status>suppressed</status>")

	disableRec := postForm(t, h, fmt.Sprintf(
		"Action=DisableApplicationStatusCheckSuppression&Version=2016-11-15&InstanceId.1=%s", instID,
	))
	require.Equal(t, http.StatusOK, disableRec.Code)
	assert.Contains(t, disableRec.Body.String(), "<DisableApplicationStatusCheckSuppressionResponse")

	statusAfterDisableRec := postForm(t, h, fmt.Sprintf(
		"Action=DescribeApplicationStatus&Version=2016-11-15&InstanceId.1=%s", instID,
	))
	require.Equal(t, http.StatusOK, statusAfterDisableRec.Code)
	assert.Contains(t, statusAfterDisableRec.Body.String(), "<status>insufficient-data</status>")

	disassocRec := postForm(t, h, fmt.Sprintf(
		"Action=DisassociateApplicationStatusCheck&Version=2016-11-15"+
			"&ApplicationStatusCheckId=%s&InstanceId.1=%s",
		checkID, instID,
	))
	require.Equal(t, http.StatusOK, disassocRec.Code)
	assert.Contains(t, disassocRec.Body.String(), "<DisassociateApplicationStatusCheckResponse")

	statusAfterDisassocRec := postForm(t, h, fmt.Sprintf(
		"Action=DescribeApplicationStatus&Version=2016-11-15&InstanceId.1=%s", instID,
	))
	require.Equal(t, http.StatusOK, statusAfterDisassocRec.Code)
	assert.Contains(t, statusAfterDisassocRec.Body.String(), "<status>not-applicable</status>")
}

func TestApplicationStatusChecksHandler_TagAssociation(t *testing.T) {
	t.Parallel()

	h := newHandler()

	runRec := postForm(t, h, "Action=RunInstances&Version=2016-11-15"+
		"&ImageId=ami-123&InstanceType=t2.micro&MinCount=1&MaxCount=1")
	instID := extractTag(t, runRec.Body.String(), "instanceId")

	tagRec := postForm(t, h, fmt.Sprintf(
		"Action=CreateTags&Version=2016-11-15&ResourceId.1=%s&Tag.1.Key=env&Tag.1.Value=prod",
		instID,
	))
	require.Equal(t, http.StatusOK, tagRec.Code)

	createRec := postForm(t, h, "Action=CreateApplicationStatusCheck&Version=2016-11-15&Protocol=http&Port=80")
	checkID := extractTag(t, createRec.Body.String(), "applicationStatusCheckId")

	assocRec := postForm(t, h, fmt.Sprintf(
		"Action=AssociateApplicationStatusCheck&Version=2016-11-15"+
			"&ApplicationStatusCheckId=%s&TargetTagAssociation.1.Key=env&TargetTagAssociation.1.Value=prod",
		checkID,
	))
	require.Equal(t, http.StatusOK, assocRec.Code)
	assocBody := assocRec.Body.String()
	assert.Contains(t, assocBody, "<associationType>EC2TAG</associationType>")
	assert.Contains(t, assocBody, "<associationValue>env=prod</associationValue>")

	// The check's own DescribeApplicationStatusChecks response reflects the
	// tag-based TargetTagAssociations (a real ApplicationStatusCheckResponseObject field).
	descChecksRec := postForm(t, h, fmt.Sprintf(
		"Action=DescribeApplicationStatusChecks&Version=2016-11-15&ApplicationStatusCheckId.1=%s",
		checkID,
	))
	require.Equal(t, http.StatusOK, descChecksRec.Code)
	descChecksBody := descChecksRec.Body.String()
	assert.Contains(t, descChecksBody, "<targetTagAssociationSet>")
	assert.Contains(t, descChecksBody, "<key>env</key>")
	assert.Contains(t, descChecksBody, "<value>prod</value>")

	// The tagged instance is picked up via the tag association.
	statusRec := postForm(t, h, fmt.Sprintf(
		"Action=DescribeApplicationStatus&Version=2016-11-15&InstanceId.1=%s", instID,
	))
	require.Equal(t, http.StatusOK, statusRec.Code)
	assert.Contains(t, statusRec.Body.String(), "<status>insufficient-data</status>")
}
