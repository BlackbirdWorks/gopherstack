package servicecatalog_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sdk "github.com/aws/aws-sdk-go-v2/service/servicecatalog"
	sctypes "github.com/aws/aws-sdk-go-v2/service/servicecatalog/types"
	"github.com/aws/smithy-go"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/sdktest"
	"github.com/blackbirdworks/gopherstack/services/servicecatalog"
)

const testAccount = awsmeta.DefaultAccount

const (
	stackSetParams = `{"Version":"1","Properties":{"AccountList":["1"],"RegionList":["us-east-1"],` +
		`"AdminRole":"a","ExecutionRole":"b"}}`
	noProdRule = `{"Rules":{"NoProd":{"Assertions":[{"Assert":{"Fn::Not":[{"Fn::Equals":[{"Ref":"Env"},"prod"]}]},` +
		`"AssertDescription":"prod is not allowed"}]}}}`
)

var (
	errTemplateMissing  = errors.New("S3 error: template not found")
	errChangeSetMissing = errors.New("change set not found")
)

const (
	simpleTemplate = `{"Parameters":{"Env":{"Type":"String","Default":"dev","AllowedValues":["dev","prod"]},` +
		`"Size":{"Type":"String"}},"Resources":{"Q":{"Type":"AWS::SQS::Queue"}},` +
		`"Outputs":{"QueueName":{"Description":"the queue","Value":"q-1"}}}`
	failingTemplate = `{"Resources":{"FAIL":{"Type":"AWS::SQS::Queue"}}}`
	templateURL     = "https://s3.amazonaws.com/templates/simple.json"
	failURL         = "https://s3.amazonaws.com/templates/fail.json"
)

// fakeEngine is an in-memory StackEngine: templates by URL, stacks that succeed unless the template says FAIL.
type fakeEngine struct {
	templates map[string]string
	stacks    map[string]*servicecatalog.StackInfo
	changeSet map[string]servicecatalog.ChangeSetInfo
	csReq     map[string]servicecatalog.StackRequest
	requests  []servicecatalog.StackRequest
	mu        sync.Mutex
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{
		templates: map[string]string{templateURL: simpleTemplate, failURL: failingTemplate},
		stacks:    map[string]*servicecatalog.StackInfo{},
		changeSet: map[string]servicecatalog.ChangeSetInfo{},
		csReq:     map[string]servicecatalog.StackRequest{},
	}
}

func (f *fakeEngine) FetchTemplate(_ context.Context, url string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if t, ok := f.templates[url]; ok {
		return t, nil
	}

	return "", fmt.Errorf("%w: %s", errTemplateMissing, url)
}

func (f *fakeEngine) StackTemplate(_ context.Context, name string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.stacks[name]; ok {
		return simpleTemplate, nil
	}

	return "", servicecatalog.ErrStackNotFound
}

type fakeTemplate struct {
	Parameters map[string]struct {
		Type          string   `json:"Type"`
		Default       string   `json:"Default"`
		AllowedValues []string `json:"AllowedValues"`
	} `json:"Parameters"`
	Outputs map[string]struct {
		Description string `json:"Description"`
		Value       string `json:"Value"`
	} `json:"Outputs"`
}

func (f *fakeEngine) InspectTemplate(_ context.Context, body string) (servicecatalog.TemplateInfo, error) {
	var t fakeTemplate
	if err := json.Unmarshal([]byte(body), &t); err != nil {
		return servicecatalog.TemplateInfo{}, err
	}

	info := servicecatalog.TemplateInfo{}

	for _, k := range sortedMapKeys(t.Parameters) {
		p := t.Parameters[k]
		info.Parameters = append(info.Parameters, servicecatalog.TemplateParameter{
			Key: k, Type: p.Type, Default: p.Default, AllowedValues: p.AllowedValues,
		})
	}

	for _, k := range sortedMapKeys(t.Outputs) {
		info.Outputs = append(
			info.Outputs,
			servicecatalog.TemplateOutput{Key: k, Description: t.Outputs[k].Description},
		)
	}

	return info, nil
}

func sortedMapKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}

	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	return keys
}

func (f *fakeEngine) outputsOf(template string) []servicecatalog.Output {
	var t fakeTemplate
	_ = json.Unmarshal([]byte(template), &t)

	out := make([]servicecatalog.Output, 0, len(t.Outputs))
	for _, k := range sortedMapKeys(t.Outputs) {
		out = append(
			out,
			servicecatalog.Output{Key: k, Value: t.Outputs[k].Value, Description: t.Outputs[k].Description},
		)
	}

	return out
}

func (f *fakeEngine) CreateStack(_ context.Context, req servicecatalog.StackRequest) (servicecatalog.StackInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.requests = append(f.requests, req)

	if strings.Contains(req.Template, "FAIL") {
		return servicecatalog.StackInfo{
			Name:   req.Name,
			Status: "ROLLBACK_COMPLETE",
			Reason: "resource FAIL could not be created",
		}, nil
	}

	st := &servicecatalog.StackInfo{
		ID: "arn:aws:cloudformation:us-east-1:" + testAccount + ":stack/" + req.Name + "/1", Name: req.Name,
		Status: "CREATE_COMPLETE", Outputs: f.outputsOf(req.Template),
	}
	f.stacks[req.Name] = st

	return *st, nil
}

func (f *fakeEngine) UpdateStack(_ context.Context, req servicecatalog.StackRequest) (servicecatalog.StackInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.requests = append(f.requests, req)

	st, ok := f.stacks[req.Name]
	if !ok {
		return servicecatalog.StackInfo{}, servicecatalog.ErrStackNotFound
	}

	if strings.Contains(req.Template, "FAIL") {
		return servicecatalog.StackInfo{
			Name:   req.Name,
			Status: "UPDATE_ROLLBACK_COMPLETE",
			Reason: "update failed",
		}, nil
	}

	st.Status, st.Outputs = "UPDATE_COMPLETE", f.outputsOf(req.Template)

	return *st, nil
}

func (f *fakeEngine) DeleteStack(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.stacks[name]; !ok {
		return servicecatalog.ErrStackNotFound
	}

	delete(f.stacks, name)

	return nil
}

func (f *fakeEngine) DescribeStack(_ context.Context, name string) (servicecatalog.StackInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if st, ok := f.stacks[name]; ok {
		return *st, nil
	}

	for _, st := range f.stacks {
		if st.ID == name {
			return *st, nil
		}
	}

	return servicecatalog.StackInfo{}, servicecatalog.ErrStackNotFound
}

func (f *fakeEngine) StackResources(_ context.Context, name string) ([]servicecatalog.StackResource, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, ok := f.stacks[name]; !ok {
		return nil, servicecatalog.ErrStackNotFound
	}

	return []servicecatalog.StackResource{{LogicalID: "Q", PhysicalID: "i-0abc", Type: "AWS::EC2::Instance"}}, nil
}

func (f *fakeEngine) CreateChangeSet(
	_ context.Context,
	req servicecatalog.StackRequest,
) (servicecatalog.ChangeSetInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	action := "ADD"
	if req.Update {
		action = "MODIFY"
	}

	cs := servicecatalog.ChangeSetInfo{
		Status: "CREATE_COMPLETE",
		Changes: []servicecatalog.PlanChange{
			{Action: action, LogicalResourceID: "Q", ResourceType: "AWS::SQS::Queue", Replacement: "FALSE"},
		},
	}
	f.changeSet[req.Name+"/"+req.ChangeSetName] = cs
	f.csReq[req.Name+"/"+req.ChangeSetName] = req

	return cs, nil
}

func (f *fakeEngine) ExecuteChangeSet(ctx context.Context, stack, csName string) error {
	f.mu.Lock()
	req, ok := f.csReq[stack+"/"+csName]
	f.mu.Unlock()

	if !ok {
		return errChangeSetMissing
	}

	if req.Update {
		_, err := f.UpdateStack(ctx, req)

		return err
	}

	_, err := f.CreateStack(ctx, req)

	return err
}

func (f *fakeEngine) DeleteChangeSet(_ context.Context, stack, csName string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	delete(f.changeSet, stack+"/"+csName)
	delete(f.csReq, stack+"/"+csName)

	return nil
}

func (f *fakeEngine) lastRequest(t *testing.T) servicecatalog.StackRequest {
	t.Helper()

	f.mu.Lock()
	defer f.mu.Unlock()

	require.NotEmpty(t, f.requests)

	return f.requests[len(f.requests)-1]
}

type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

type fixture struct {
	backend *servicecatalog.InMemoryBackend
	engine  *fakeEngine
	clock   *fakeClock
	url     string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()

	b := servicecatalog.NewInMemoryBackend(testAccount, "us-east-1")
	clock := &fakeClock{t: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
	eng := newFakeEngine()
	b.SetClock(clock.now)
	b.SetStackEngine(eng)

	return &fixture{backend: b, engine: eng, clock: clock, url: sdktest.Serve(t, servicecatalog.NewHandler(b))}
}

func (f *fixture) clientIn(t *testing.T, region, accessKey string) *sdk.Client {
	t.Helper()

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion(region),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, "test", "")),
	)
	require.NoError(t, err)

	return sdk.NewFromConfig(cfg, func(o *sdk.Options) { o.BaseEndpoint = aws.String(f.url) })
}

func (f *fixture) client(t *testing.T) *sdk.Client {
	t.Helper()

	return f.clientIn(t, "us-east-1", "test")
}

func apiCode(err error) string {
	if ae, ok := errors.AsType[smithy.APIError](err); ok {
		return ae.ErrorCode()
	}

	return ""
}

// catalog is a portfolio with one CloudFormation product and the association between them.
type catalog struct {
	portfolioID string
	productID   string
	artifactID  string
}

func createCatalog(t *testing.T, c *sdk.Client, url string) catalog {
	t.Helper()

	ctx := t.Context()

	pf, err := c.CreatePortfolio(ctx, &sdk.CreatePortfolioInput{
		DisplayName: aws.String("platform"), ProviderName: aws.String("it"), IdempotencyToken: aws.String(token(t)),
	})
	require.NoError(t, err)

	pr, err := c.CreateProduct(ctx, &sdk.CreateProductInput{
		Name: aws.String("queue"), Owner: aws.String("team-a"), ProductType: sctypes.ProductTypeCloudFormationTemplate,
		IdempotencyToken: aws.String(token(t)),
		ProvisioningArtifactParameters: &sctypes.ProvisioningArtifactProperties{
			Name: aws.String("v1"), Type: sctypes.ProvisioningArtifactTypeCloudFormationTemplate,
			Info: map[string]string{"LoadTemplateFromURL": url},
		},
	})
	require.NoError(t, err)

	_, err = c.AssociateProductWithPortfolio(ctx, &sdk.AssociateProductWithPortfolioInput{
		PortfolioId: pf.PortfolioDetail.Id, ProductId: pr.ProductViewDetail.ProductViewSummary.ProductId,
	})
	require.NoError(t, err)

	return catalog{
		portfolioID: aws.ToString(pf.PortfolioDetail.Id),
		productID:   aws.ToString(pr.ProductViewDetail.ProductViewSummary.ProductId),
		artifactID:  aws.ToString(pr.ProvisioningArtifactDetail.Id),
	}
}

func token(t *testing.T) string {
	t.Helper()

	return t.Name() + "-" + uuid.NewString()
}

func serve(t *testing.T, b *servicecatalog.InMemoryBackend) string {
	t.Helper()

	return sdktest.Serve(t, servicecatalog.NewHandler(b))
}
