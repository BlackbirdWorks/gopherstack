package connect

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const (
	labelTestCase      = "TestCase"
	testStatusSaved    = "SAVED"
	testStatusPub      = "PUBLISHED"
	entryVoiceCall     = "VOICE_CALL"
	entryChat          = "CHAT"
	maxTestCaseContent = 256 * 1024
	maxTestCasesPage   = 1000
)

// TestCaseEntryPoint is where a test starts.
type TestCaseEntryPoint struct {
	VoiceCallEntryPointParameters *struct {
		SourcePhoneNumber      string `json:"SourcePhoneNumber,omitempty"`
		DestinationPhoneNumber string `json:"DestinationPhoneNumber,omitempty"`
		FlowID                 string `json:"FlowId,omitempty"`
	} `json:"VoiceCallEntryPointParameters,omitempty"`
	ChatEntryPointParameters *struct {
		FlowID string `json:"FlowId,omitempty"`
	} `json:"ChatEntryPointParameters,omitempty"`
	Type string `json:"Type"`
}

type testCaseVersion struct {
	EntryPoint         *TestCaseEntryPoint `json:"entryPoint,omitempty"`
	LastModified       *awstime.EpochTime  `json:"lastModified"`
	Description        string              `json:"description,omitempty"`
	Content            string              `json:"content"`
	InitializationData string              `json:"initializationData,omitempty"`
	LastModifiedRegion string              `json:"lastModifiedRegion"`
}

type testCaseRec struct {
	Saved      *testCaseVersion  `json:"saved,omitempty"`
	Published  *testCaseVersion  `json:"published,omitempty"`
	Tags       map[string]string `json:"tags,omitempty"`
	InstanceID string            `json:"instanceId"`
	ID         string            `json:"id"`
	Arn        string            `json:"arn"`
	Name       string            `json:"name"`
}

// TestCase is a stored test case version.
type TestCase struct {
	LastModifiedTime   *awstime.EpochTime  `json:"LastModifiedTime"`
	EntryPoint         *TestCaseEntryPoint `json:"EntryPoint,omitempty"`
	Tags               map[string]string   `json:"Tags,omitempty"`
	Arn                string              `json:"Arn"`
	Content            string              `json:"Content"`
	Description        string              `json:"Description,omitempty"`
	ID                 string              `json:"Id"`
	InitializationData string              `json:"InitializationData,omitempty"`
	LastModifiedRegion string              `json:"LastModifiedRegion"`
	Name               string              `json:"Name"`
	Status             string              `json:"Status"`
	TestCaseSha256     string              `json:"TestCaseSha256"`
}

// TestCaseSummary is the list shape of a test case.
type TestCaseSummary struct {
	LastModifiedTime   *awstime.EpochTime `json:"LastModifiedTime"`
	Arn                string             `json:"Arn"`
	ID                 string             `json:"Id"`
	LastModifiedRegion string             `json:"LastModifiedRegion"`
	Name               string             `json:"Name"`
	Status             string             `json:"Status"`
}

// CreateTestCaseInput is the CreateTestCase request.
type CreateTestCaseInput struct {
	EntryPoint         *TestCaseEntryPoint
	Tags               map[string]string
	Name               string
	Description        string
	Content            string
	InitializationData string
	Status             string
	instPath
}

// UpdateTestCaseInput is the UpdateTestCase request.
type UpdateTestCaseInput struct {
	EntryPoint         *TestCaseEntryPoint
	Name               *string
	Description        *string
	Content            *string
	InitializationData *string
	Status             string
	testCaseRef
}

type testCaseRef struct {
	TestCaseID string `json:"-" path:"TestCaseId"`
	instPath
}

// TestCaseCreated is the CreateTestCase response.
type TestCaseCreated struct {
	TestCaseID  string `json:"TestCaseId"`
	TestCaseArn string `json:"TestCaseArn"`
}

type testCaseQuery struct {
	Status string `json:"-" query:"status"`
	testCaseRef
}

func (v *testCaseVersion) clone() *testCaseVersion {
	if v == nil {
		return &testCaseVersion{}
	}

	c := *v

	return &c
}

func (r *testCaseRec) effective() (*testCaseVersion, string) {
	if r.Published != nil {
		return r.Published, testStatusPub
	}

	return r.Saved, testStatusSaved
}

// updateTarget is the version an update starts from: the named one, else the other version.
func (r *testCaseRec) updateTarget(status string) *testCaseVersion {
	cur := r.Saved
	if status == testStatusPub {
		cur = r.Published
	}

	if cur == nil {
		cur, _ = r.effective()
	}

	return cur
}

func (r *testCaseRec) version(status string) (*testCaseVersion, string) {
	switch status {
	case testStatusPub:
		return r.Published, testStatusPub
	case testStatusSaved:
		return r.Saved, testStatusSaved
	default:
		return r.effective()
	}
}

func (r *testCaseRec) view(v *testCaseVersion, status string) TestCase {
	sum := sha256.Sum256([]byte(v.Content))

	return TestCase{
		LastModifiedTime:   v.LastModified,
		EntryPoint:         v.EntryPoint,
		Tags:               cloneTags(r.Tags),
		Arn:                r.Arn,
		Content:            v.Content,
		Description:        v.Description,
		ID:                 r.ID,
		InitializationData: v.InitializationData,
		LastModifiedRegion: v.LastModifiedRegion,
		Name:               r.Name,
		Status:             status,
		TestCaseSha256:     hex.EncodeToString(sum[:]),
	}
}

func (b *Backend) checkTestCaseFields(name, desc, status string) error {
	if name != "" {
		if err := checkName("Name", name, maxNameLen); err != nil {
			return err
		}
	}

	return firstError(checkDescription(desc), checkOptional(status, "Status", testStatusSaved, testStatusPub))
}

func (b *Backend) validateTestCaseVersion(inst string, v *testCaseVersion, published bool) error {
	if v.Content == "" || len(v.Content) > maxTestCaseContent {
		return invalidParamErr("Content must be 1-%d characters", maxTestCaseContent)
	}

	if v.InitializationData != "" && !jsontext.Value([]byte(v.InitializationData)).IsValid(jsonx.V1()) {
		return invalidParamErr("InitializationData must be a valid JSON string")
	}

	if !published {
		return b.checkEntryPoint(inst, v.EntryPoint, false)
	}

	var doc map[string]any
	if err := json.Unmarshal([]byte(v.Content), &doc, jsonx.V1()); err != nil {
		return invalidParamErr("Content must be a JSON object to be published")
	}

	return b.checkEntryPoint(inst, v.EntryPoint, true)
}

func (b *Backend) checkEntryPoint(inst string, ep *TestCaseEntryPoint, required bool) error {
	if ep == nil {
		if required {
			return invalidParamErr("EntryPoint is required to publish a test case")
		}

		return nil
	}

	var flowID string

	switch ep.Type {
	case entryVoiceCall:
		if ep.VoiceCallEntryPointParameters == nil {
			return invalidParamErr("VoiceCallEntryPointParameters is required for VOICE_CALL entry points")
		}

		flowID = ep.VoiceCallEntryPointParameters.FlowID
	case entryChat:
		if ep.ChatEntryPointParameters == nil {
			return invalidParamErr("ChatEntryPointParameters is required for CHAT entry points")
		}

		flowID = ep.ChatEntryPointParameters.FlowID
	default:
		return invalidParamErr("EntryPoint.Type must be one of %s, %s", entryVoiceCall, entryChat)
	}

	if flowID != "" && !b.flows.has(inst, flowID) {
		return notFoundErr("ContactFlow", flowID)
	}

	return nil
}

// CreateTestCase stores a test case as a saved or published version.
func (b *Backend) CreateTestCase(ctx context.Context, in *CreateTestCaseInput) (*TestCaseCreated, error) {
	if err := firstError(
		b.checkTestCaseFields(in.Name, in.Description, in.Status), checkTags(in.Tags),
	); err != nil {
		return nil, err
	}

	if strings.TrimSpace(in.Name) == "" {
		return nil, invalidParamErr("Name is required")
	}

	return writeInstance(b, "CreateTestCase", in.InstanceID, func(inst string) (*TestCaseCreated, error) {
		if nameExists(b.testCases, inst, func(r *testCaseRec) string { return r.Name }, in.Name, "") {
			return nil, duplicateErr(labelTestCase, in.Name)
		}

		_, region := b.scope(ctx)
		now := awstime.NewEpochTime(b.now())
		v := &testCaseVersion{
			EntryPoint: in.EntryPoint, LastModified: &now, Description: in.Description, Content: in.Content,
			InitializationData: in.InitializationData, LastModifiedRegion: region,
		}

		status := orDefault(in.Status, testStatusSaved)
		if err := b.validateTestCaseVersion(inst, v, status == testStatusPub); err != nil {
			return nil, err
		}

		id := newID()
		rec := &testCaseRec{
			InstanceID: inst, ID: id, Arn: b.buildARN(ctx, "instance/"+inst+"/test-case/"+id), Name: in.Name,
			Tags: cloneTags(in.Tags),
		}

		if status == testStatusPub {
			rec.Published = v
		} else {
			rec.Saved = v
		}

		b.testCases.put(rec)

		return &TestCaseCreated{TestCaseID: id, TestCaseArn: rec.Arn}, nil
	})
}

// DescribeTestCase returns the requested version of a test case, the published one when available by default.
func (b *Backend) DescribeTestCase(in *testCaseQuery) (*struct {
	TestCase TestCase `json:"TestCase"`
}, error,
) {
	if err := checkOptional(in.Status, "status", testStatusSaved, testStatusPub); err != nil {
		return nil, err
	}

	return readInstance(b, "DescribeTestCase", in.InstanceID, func(inst string) (*struct {
		TestCase TestCase `json:"TestCase"`
	}, error,
	) {
		r, err := b.testCases.get(inst, in.TestCaseID)
		if err != nil {
			return nil, err
		}

		v, status := r.version(in.Status)
		if v == nil {
			return nil, notFoundErr(labelTestCase+" "+status+" version", in.TestCaseID)
		}

		return &struct {
			TestCase TestCase `json:"TestCase"`
		}{TestCase: r.view(v, status)}, nil
	})
}

func applyTestCaseUpdate(v *testCaseVersion, in *UpdateTestCaseInput) {
	if in.Content != nil {
		v.Content = *in.Content
	}

	if in.Description != nil {
		v.Description = *in.Description
	}

	if in.InitializationData != nil {
		v.InitializationData = *in.InitializationData
	}

	if in.EntryPoint != nil {
		v.EntryPoint = in.EntryPoint
	}
}

// UpdateTestCase changes the saved or published version of a test case.
func (b *Backend) UpdateTestCase(ctx context.Context, in *UpdateTestCaseInput) (*EmptyOutput, error) {
	name, desc := "", ""
	if in.Name != nil {
		name = *in.Name
		if strings.TrimSpace(name) == "" {
			return nil, invalidParamErr("Name must not be empty")
		}
	}

	if in.Description != nil {
		desc = *in.Description
	}

	if err := b.checkTestCaseFields(name, desc, in.Status); err != nil {
		return nil, err
	}

	return writeInstance(b, "UpdateTestCase", in.InstanceID, func(inst string) (*EmptyOutput, error) {
		r, err := b.testCases.get(inst, in.TestCaseID)
		if err != nil {
			return nil, err
		}

		if in.Name != nil && nameExists(b.testCases, inst, func(x *testCaseRec) string { return x.Name }, name, r.ID) {
			return nil, duplicateErr(labelTestCase, name)
		}

		status := orDefault(in.Status, testStatusSaved)
		v := r.updateTarget(status).clone()
		applyTestCaseUpdate(v, in)

		_, region := b.scope(ctx)
		now := awstime.NewEpochTime(b.now())
		v.LastModified, v.LastModifiedRegion = &now, region

		if err = b.validateTestCaseVersion(inst, v, status == testStatusPub); err != nil {
			return nil, err
		}

		if status == testStatusPub {
			r.Published = v
		} else {
			r.Saved = v
		}

		if in.Name != nil {
			r.Name = name
		}

		return &EmptyOutput{}, nil
	})
}

// DeleteTestCase removes a test case with both of its versions.
func (b *Backend) DeleteTestCase(inst, id string) (*EmptyOutput, error) {
	return writeInstance(b, "DeleteTestCase", inst, func(inst string) (*EmptyOutput, error) {
		return nil, b.testCases.del(inst, id)
	})
}

// ListTestCases pages through test case summaries sorted by name.
func (b *Backend) ListTestCases(inst, token string, maxResults int32) ([]TestCaseSummary, string, error) {
	if maxResults > maxTestCasesPage {
		return nil, "", invalidParamErr("maxResults must be 1-%d", maxTestCasesPage)
	}

	all, err := readInstance(b, "ListTestCases", inst, func(inst string) ([]TestCaseSummary, error) {
		recs := b.testCases.in(inst)
		slices.SortFunc(recs, func(a, c *testCaseRec) int {
			return strings.Compare(strings.ToLower(a.Name), strings.ToLower(c.Name))
		})

		out := make([]TestCaseSummary, 0, len(recs))

		for _, r := range recs {
			v, status := r.effective()
			out = append(out, TestCaseSummary{
				LastModifiedTime: v.LastModified, Arn: r.Arn, ID: r.ID, LastModifiedRegion: v.LastModifiedRegion,
				Name: r.Name, Status: status,
			})
		}

		return out, nil
	})
	if err != nil {
		return nil, "", err
	}

	return paged(all, token, maxResults)
}

// SearchTestCases searches test cases by name, description, status and tags.
func (b *Backend) SearchTestCases(in *SearchRequest) (*SearchResult, error) {
	if in.MaxResults > maxNotifListPage {
		return nil, invalidParamErr("MaxResults must be 1-%d", maxNotifListPage)
	}

	return runSearch(b, "SearchTestCases", "TestCases", in, b.testCases, nameDescSpec(),
		func(r *testCaseRec) searchSubject {
			v, status := r.effective()

			return searchSubject{
				fields: nameDesc(r.Name, v.Description), tags: r.Tags, conds: map[string]string{fieldStatus: status},
			}
		},
		func(r *testCaseRec) TestCase {
			v, status := r.effective()

			return r.view(v, status)
		})
}

// DescribeTestCaseVersion returns the saved or published version of a test case; an empty status picks the default.
func (b *Backend) DescribeTestCaseVersion(inst, id, status string) (*TestCase, error) {
	out, err := b.DescribeTestCase(&testCaseQuery{
		Status: status, TestCaseID: id, InstanceID: inst,
	})
	if err != nil {
		return nil, err
	}

	return &out.TestCase, nil
}
