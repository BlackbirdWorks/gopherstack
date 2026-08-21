package quicksight_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/quicksight"
)

// TestCreateAccountSubscription_UserLoginNameCasing_RealClient covers
// gopherstack-y1zn. handleCreateAccountSubscription emitted "UserLoginName"
// inside SignupResponse; types.SignupResponse (quicksight@v1.123.1
// deserializers.go's awsRestjson1_deserializeDocumentSignupResponse) reads
// the lowercase-first "userLoginName" -- the only member in that object with
// non-PascalCase casing. A typed client's UserLoginName field stays nil
// against the wrong casing, so the proof is the raw body.
func TestCreateAccountSubscription_UserLoginNameCasing_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, subscriptionPath(), map[string]any{
		"AccountName":       "cas-account",
		"NotificationEmail": "test@example.com",
		"Edition":           "STANDARD",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"UserLoginName"`,
		"types.SignupResponse has no UserLoginName member; the real key is lowercase-first")
	assert.Contains(t, body, `"userLoginName"`,
		"types.SignupResponse's real member is userLoginName")
}

// TestDescribeKeyRegistration_KeyRegistrationKey_RealClient covers
// gopherstack-y1zn. handleDescribeKeyRegistration emitted
// "RegisteredCustomerManagedKeys" -- the name of the array's own item type
// (types.RegisteredCustomerManagedKey), not a real wire key.
// DescribeKeyRegistrationOutput (quicksight@v1.123.1 deserializers.go's
// awsRestjson1_deserializeOpDocumentDescribeKeyRegistrationOutput) wraps the
// list under "KeyRegistration".
func TestDescribeKeyRegistration_KeyRegistrationKey_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, accountPath("/key-registration"), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"RegisteredCustomerManagedKeys"`,
		"DescribeKeyRegistrationOutput has no RegisteredCustomerManagedKeys member")
	assert.Contains(t, body, `"KeyRegistration"`,
		"DescribeKeyRegistrationOutput's real member is KeyRegistration")
}

// TestDescribeDefaultQBiz_FlatShape_RealClient covers gopherstack-y1zn.
// handleDescribeDefaultQBiz wrapped its result under a fabricated
// "DefaultQBusinessApplication" key with a Namespace echo.
// DescribeDefaultQBusinessApplicationOutput (quicksight@v1.123.1
// deserializers.go's
// awsRestjson1_deserializeOpDocumentDescribeDefaultQBusinessApplicationOutput)
// is flat: ApplicationId/RequestId only, no wrapper and no Namespace member.
func TestDescribeDefaultQBiz_FlatShape_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, accountPath("/default-qbusiness-application"), nil)
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"DefaultQBusinessApplication"`,
		"DescribeDefaultQBusinessApplicationOutput has no such wrapper member")
	assert.NotContains(t, body, `"Namespace"`,
		"DescribeDefaultQBusinessApplicationOutput has no Namespace member; it is Input-side only")
	assert.Contains(t, body, `"ApplicationId"`,
		"DescribeDefaultQBusinessApplicationOutput's real member is the flat ApplicationId")
}

// TestUpdateSelfUpgrade_LastUpdateAttemptTimeCasing_RealClient covers
// gopherstack-y1zn. selfUpgradeRequestDetailToMap emitted
// "LastUpdateAttemptTime"/"LastUpdateFailureReason" (PascalCase, matching
// every sibling member); types.SelfUpgradeRequestDetail (quicksight@v1.123.1
// deserializers.go's
// awsRestjson1_deserializeDocumentSelfUpgradeRequestDetail) reads these two
// specific members lowercase-first ("lastUpdateAttemptTime"/
// "lastUpdateFailureReason") while every other member on the same type stays
// PascalCase.
func TestUpdateSelfUpgrade_LastUpdateAttemptTimeCasing_RealClient(t *testing.T) {
	t.Parallel()

	backend := newTestBackend(t)
	h := quicksight.NewHandler(backend)

	quicksight.SeedSelfUpgradeRequest(backend, testAccountID, testNamespace, &quicksight.SelfUpgradeRequestDetail{
		CreationTime:     time.Now().UTC().Unix(),
		UpgradeRequestID: "y1zn-req1",
		OriginalRole:     "READER",
		RequestedRole:    "AUTHOR",
		RequestStatus:    "PENDING",
	})

	rec := doRequest(t, h, http.MethodPost, nsPath("/update-self-upgrade-request"), map[string]any{
		"Action":           "APPROVE",
		"UpgradeRequestId": "y1zn-req1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	assert.NotContains(t, body, `"LastUpdateAttemptTime"`,
		"the real member is lowercase-first")
	assert.Contains(t, body, `"lastUpdateAttemptTime"`,
		"types.SelfUpgradeRequestDetail's real member is lastUpdateAttemptTime")
}

// TestTopicReviewedAnswers_WireFixes_RealClient covers gopherstack-y1zn.
// BatchCreate/BatchDeleteTopicReviewedAnswerOutput wrapped their succeeded
// list under "SucceededAnswer" (singular); the real member (quicksight@v1.123.1
// deserializers.go) is "SucceededAnswers" (plural). ListTopicReviewedAnswers'
// items also emitted a fabricated "Mode" key: types.TopicReviewedAnswer has no
// such member (AnswerId/Arn/DatasetArn/Mir/PrimaryVisual/Question/Template
// only).
func TestTopicReviewedAnswers_WireFixes_RealClient(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, accountPath("/topics"), map[string]any{"TopicId": "y1zn-tp", "Name": "T1"})

	createRec := doRequest(
		t, h, http.MethodPost, accountPath("/topics/y1zn-tp/batch-create-reviewed-answers"),
		map[string]any{
			"Answers": []any{
				map[string]any{
					"AnswerId":   "y1zn-ans1",
					"DatasetArn": "arn:aws:quicksight:us-east-1:000000000000:dataset/ds1",
					"Question":   "How many sales?",
				},
			},
		},
	)
	require.Equal(t, http.StatusOK, createRec.Code)

	createBody := createRec.Body.String()
	assert.NotContains(t, createBody, `"SucceededAnswer":`,
		"BatchCreateTopicReviewedAnswerOutput has no singular SucceededAnswer member")
	assert.Contains(t, createBody, `"SucceededAnswers"`,
		"BatchCreateTopicReviewedAnswerOutput's real member is SucceededAnswers")

	listRec := doRequest(t, h, http.MethodGet, accountPath("/topics/y1zn-tp/reviewed-answers"), nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	listBody := listRec.Body.String()
	assert.NotContains(t, listBody, `"Mode"`,
		"types.TopicReviewedAnswer has no Mode member")
}
