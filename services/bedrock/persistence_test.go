package bedrock_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/bedrock"
)

// Compile-time assertion that Handler satisfies persistence.Persistable --
// this is what makes cli.go's setupPersistence auto-register it (see
// persistence.go).
var _ persistence.Persistable = (*bedrock.Handler)(nil)

const (
	testAccountID = "123456789012"
	testRegion    = "us-east-1"
)

// fixtureIDs carries every identifier (and the unexported-field timestamp
// captured via export_test.go's *ForTest bridge) that newPersistenceFixture
// creates, so the round-trip assertions in
// TestInMemoryBackend_SnapshotRestore_FullState can look each resource back
// up after Restore without re-deriving names/IDs.
type fixtureIDs struct {
	inferenceProfileARN       string
	promptRouterARN           string
	pmtARN                    string
	evalJobARN                string
	arpARN                    string
	arpWorkflowID             string
	arpTestCaseID             string
	arpVersion                string
	customModelARN            string
	customModelDeployARN      string
	customizationJobARN       string
	copyJobARN                string
	importJobARN              string
	guardrailID               string
	marketplaceEndpointARN    string
	guardrailVersion          string
	invocationJobARN          string
	enforcedGuardrailConfigID string
	advancedPromptOptJobARN   string
	resourcePolicyTargetARN   string
	guardrailVersionCount     int
}

// newPersistenceFixture builds a backend with one populated entry in every
// store.Table registered on b.registry, every raw map/counter left
// un-converted, and Guardrail.versionCounter (the unexported field
// persistence.go carries out-of-band), so a Snapshot from it exercises the
// entire persisted surface of the backend.
func newPersistenceFixture(t *testing.T) (*bedrock.InMemoryBackend, fixtureIDs) {
	t.Helper()

	b := bedrock.NewInMemoryBackend(testAccountID, testRegion)
	tags := []bedrock.Tag{{Key: "env", Value: "test"}}

	ids := seedGuardrailAndModelResources(t, b, tags)
	seedJobResources(t, b, tags, ids.customModelARN, &ids)

	// Misc raw state not tied to a single resource above.
	b.PutModelInvocationLoggingConfiguration(&bedrock.ModelInvocationLoggingConfiguration{
		S3Config: &bedrock.S3LoggingConfig{BucketName: "test-bucket"},
	})
	b.PutUseCaseForModelAccess([]byte("test use case form data"))
	_, err := b.UpdateAutomatedReasoningPolicyAnnotations(
		ids.arpARN, ids.arpWorkflowID, []any{map[string]any{"seed": true}}, "seed-hash",
	)
	require.NoError(t, err)

	seedParity4Resources(t, b, &ids)

	return b, ids
}

// seedParity4Resources seeds the parity-4 tables/state (advancedPromptOptimizationJobs,
// resourcePolicies, accountDataRetention) and writes the resulting IDs into
// ids in place. resourcePolicies attaches to the guardrail ARN seeded by
// seedGuardrailAndModelResources, proving a real ARN-validated target
// round-trips.
func seedParity4Resources(t *testing.T, b *bedrock.InMemoryBackend, ids *fixtureIDs) {
	t.Helper()

	job, err := b.CreateAdvancedPromptOptimizationJob(
		bedrock.CreateAdvancedPromptOptimizationJobInput{
			JobName:      "test-apo-job",
			InputConfig:  bedrock.AdvancedPromptOptimizationInputConfig{S3URI: "s3://bucket/in"},
			OutputConfig: bedrock.AdvancedPromptOptimizationOutputConfig{S3URI: "s3://bucket/out"},
			ModelConfigurations: []bedrock.ModelConfiguration{
				{ModelID: "amazon.titan-text-express-v1"},
			},
		},
	)
	require.NoError(t, err)

	g, err := b.GetGuardrail(ids.guardrailID)
	require.NoError(t, err)

	_, err = b.PutResourcePolicy(g.GuardrailArn, `{"Version":"2012-10-17","Statement":[]}`)
	require.NoError(t, err)

	_, err = b.PutAccountDataRetention("none")
	require.NoError(t, err)

	ids.advancedPromptOptJobARN = job.JobArn
	ids.resourcePolicyTargetARN = g.GuardrailArn
}

// seedGuardrailAndModelResources seeds the guardrail/model-family tables
// (guardrails, guardrailVersions, provisionedModelThroughputs, customModels,
// customModelDeployments, foundationModelAgreements,
// enforcedGuardrailConfigs) and returns the IDs fixtureIDs needs from them,
// plus GuardrailVersionCounterForTest's pre-snapshot value.
func seedGuardrailAndModelResources(
	t *testing.T,
	b *bedrock.InMemoryBackend,
	tags []bedrock.Tag,
) fixtureIDs {
	t.Helper()

	policies := &bedrock.GuardrailPolicies{
		ContentPolicy: &bedrock.GuardrailContentPolicyConfig{
			FiltersConfig: []bedrock.GuardrailContentFilter{
				{Type: "HATE", InputStrength: "HIGH", OutputStrength: "HIGH"},
			},
		},
	}

	g, err := b.CreateGuardrail(
		"test-guardrail",
		"desc",
		"blocked-in",
		"blocked-out",
		tags,
		policies,
	)
	require.NoError(t, err)

	// CreateGuardrailVersion snapshots the DRAFT's current policies immutably; this
	// snapshot (not the live DRAFT) must survive Snapshot/Restore intact.
	gv, err := b.CreateGuardrailVersion(g.GuardrailID, "v1 snapshot")
	require.NoError(t, err)

	egc, err := b.PutEnforcedGuardrailConfiguration(
		"", g.GuardrailID, gv.Version, "HONOR", []string{"model-a"}, []string{"model-b"},
	)
	require.NoError(t, err)

	pmt, err := b.CreateProvisionedModelThroughput(
		"test-pmt", "amazon.titan-text-express-v1", 1, "NoCommitment", tags,
	)
	require.NoError(t, err)

	cm, err := b.CreateCustomModel("test-model", tags)
	require.NoError(t, err)

	cmd, err := b.CreateCustomModelDeployment(cm.ModelArn, "test-deploy", tags)
	require.NoError(t, err)

	fma, err := b.CreateFoundationModelAgreement("amazon.titan-text-express-v1")
	require.NoError(t, err)
	require.NotEmpty(t, fma.ModelID)

	return fixtureIDs{
		guardrailID:               g.GuardrailID,
		guardrailVersion:          gv.Version,
		guardrailVersionCount:     b.GuardrailVersionCounterForTest(g.GuardrailID),
		pmtARN:                    pmt.ProvisionedModelArn,
		customModelARN:            cm.ModelArn,
		customModelDeployARN:      cmd.CustomModelDeploymentArn,
		enforcedGuardrailConfigID: egc.ConfigID,
	}
}

// seedJobResources seeds the job/policy-family tables (evaluationJobs,
// automatedReasoningPolicies, arpBuildWorkflows, arpTestCases, arpVersions,
// modelCustomizationJobs, modelCopyJobs, modelImportJobs, inferenceProfiles,
// marketplaceEndpoints, modelInvocationJobs, promptRouters), writing their
// IDs into ids in place.
func seedJobResources(
	t *testing.T,
	b *bedrock.InMemoryBackend,
	tags []bedrock.Tag,
	customModelARN string,
	ids *fixtureIDs,
) {
	t.Helper()

	evalJob, err := b.CreateEvaluationJob("test-eval-job", tags)
	require.NoError(t, err)

	arp, err := b.CreateAutomatedReasoningPolicy("test-arp", "desc", tags)
	require.NoError(t, err)

	wf, err := b.StartAutomatedReasoningPolicyBuildWorkflow(
		arp.PolicyArn, "INGEST_CONTENT", json.RawMessage(`{}`),
	)
	require.NoError(t, err)

	tc, err := b.CreateAutomatedReasoningPolicyTestCase(arp.PolicyArn)
	require.NoError(t, err)

	arpv, err := b.CreateAutomatedReasoningPolicyVersion(arp.PolicyArn, "definition-hash-123", nil)
	require.NoError(t, err)

	mcj, err := b.CreateModelCustomizationJob(
		"test-cust-job", "test-cust-model", "amazon.titan-text-express-v1", "FINE_TUNING",
		"arn:aws:iam::000000000000:role/cust-role",
		bedrock.OutputDataConfig{S3Uri: "s3://my-bucket/output/"},
		bedrock.TrainingDataConfig{S3Uri: "s3://my-bucket/training/"},
		nil,
		tags,
	)
	require.NoError(t, err)

	mcpj, err := b.CreateModelCopyJob(customModelARN, "test-copy-target", tags)
	require.NoError(t, err)

	mij, err := b.CreateModelImportJob(
		"test-import-job", "test-import-job-model", "arn:aws:iam::000000000000:role/import-role",
		"s3://my-bucket/model-data/", tags,
	)
	require.NoError(t, err)

	ip, err := b.CreateInferenceProfile(
		"test-inference-profile", "desc",
		"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2", tags,
	)
	require.NoError(t, err)

	mme, err := b.CreateMarketplaceModelEndpoint(
		"test-mp-endpoint",
		"test-model-source-id",
		nil,
		tags,
	)
	require.NoError(t, err)

	invJob, err := b.CreateModelInvocationJob("test-invocation-job", tags)
	require.NoError(t, err)

	pr, err := b.CreatePromptRouter(
		"test-prompt-router", "test router desc",
		"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2",
		[]string{"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2"},
		0.5, tags,
	)
	require.NoError(t, err)

	ids.evalJobARN = evalJob.JobArn
	ids.arpARN = arp.PolicyArn
	ids.arpWorkflowID = wf.BuildWorkflowID
	ids.arpTestCaseID = tc.TestCaseID
	ids.arpVersion = arpv.Version
	ids.customizationJobARN = mcj.JobArn
	ids.copyJobARN = mcpj.JobArn
	ids.importJobARN = mij.JobArn
	ids.inferenceProfileARN = ip.InferenceProfileArn
	ids.marketplaceEndpointARN = mme.EndpointArn
	ids.invocationJobARN = invJob.JobArn
	ids.promptRouterARN = pr.PromptRouterArn
}

// TestInMemoryBackend_SnapshotRestore_FullState round-trips every
// store.Table registered on b.registry, every raw map/counter left
// un-converted, and every unexported field persistence.go carries
// out-of-band, through Snapshot -> Restore into a fresh backend.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	original, ids := newPersistenceFixture(t)

	snap := original.Snapshot(ctx)
	require.NotNil(t, snap)

	// Byte-identical re-snapshot: the automated completeness proof that
	// Restore drops NOTHING Snapshot captured. Restore into a pristine
	// backend, immediately re-Snapshot, and require byte equality -- done
	// FIRST, before the assertion helpers below (which create post-restore
	// resources that would legitimately mutate the backend). If any field
	// persistence.go's Snapshot writes is not faithfully reloaded by Restore,
	// the re-snapshot diverges here, so a future field added to a store.Table
	// or to backendSnapshot that is snapshotted but not restored fails this
	// test without anyone having to remember to add a bespoke assertion for
	// it. Snapshot output is deterministic (store.Table.Snapshot is key-sorted
	// and encoding/json sorts map keys), so equal state always yields equal
	// bytes.
	clean := bedrock.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, clean.Restore(ctx, snap))
	require.Equal(t, string(snap), string(clean.Snapshot(ctx)))

	fresh := bedrock.NewInMemoryBackend(testAccountID, testRegion)
	require.NoError(t, fresh.Restore(ctx, snap))

	assertGuardrailAndModelState(t, fresh, ids)
	assertJobState(t, fresh, ids)
	assertMiscRawState(t, fresh, ids)
	assertParity4State(t, fresh, ids)
}

// assertParity4State verifies the parity-4 tables/state
// (advancedPromptOptimizationJobs, resourcePolicies, accountDataRetention)
// round-tripped, including their ID/revision counters.
func assertParity4State(t *testing.T, fresh *bedrock.InMemoryBackend, ids fixtureIDs) {
	t.Helper()

	job, err := fresh.GetAdvancedPromptOptimizationJob(ids.advancedPromptOptJobARN)
	require.NoError(t, err)
	assert.Equal(t, "test-apo-job", job.JobName)
	assert.Equal(t, "InProgress", job.JobStatus)

	rp, err := fresh.GetResourcePolicy(ids.resourcePolicyTargetARN)
	require.NoError(t, err)
	assert.Contains(t, rp.PolicyDocument, "Statement")

	retention := fresh.GetAccountDataRetention()
	assert.Equal(t, "none", retention.Mode)

	// ID/revision counters: a newly created job or policy after restore must
	// not collide with (or otherwise depend on) the state created before the
	// snapshot.
	job2, err := fresh.CreateAdvancedPromptOptimizationJob(
		bedrock.CreateAdvancedPromptOptimizationJobInput{
			JobName:      "post-restore-apo-job",
			InputConfig:  bedrock.AdvancedPromptOptimizationInputConfig{S3URI: "s3://bucket/in"},
			OutputConfig: bedrock.AdvancedPromptOptimizationOutputConfig{S3URI: "s3://bucket/out"},
			ModelConfigurations: []bedrock.ModelConfiguration{
				{ModelID: "amazon.titan-text-express-v1"},
			},
		},
	)
	require.NoError(t, err)
	assert.NotEqual(t, ids.advancedPromptOptJobARN, job2.JobArn)
}

// assertGuardrailAndModelState verifies the guardrail/model-family tables
// (including EnforcedGuardrailConfig and the out-of-band
// GuardrailVersionCounters field) round-tripped.
func assertGuardrailAndModelState(t *testing.T, fresh *bedrock.InMemoryBackend, ids fixtureIDs) {
	t.Helper()

	g, err := fresh.GetGuardrail(ids.guardrailID)
	require.NoError(t, err)
	assert.Equal(t, "test-guardrail", g.Name)

	// guardrailsByName raw map: a duplicate CreateGuardrail by the same name
	// is allowed by AWS (guardrails are ID-keyed, not name-unique) but the
	// name->ID index must still resolve to the original ID.
	assert.Equal(t, ids.guardrailID, g.GuardrailID)

	// Guardrail.versionCounter (unexported): a restored guardrail must not
	// restart version numbering from 1 -- see persistence.go's
	// backendSnapshot doc comment for the data-corruption risk this avoids.
	assert.Equal(
		t,
		ids.guardrailVersionCount,
		fresh.GuardrailVersionCounterForTest(ids.guardrailID),
	)

	// The numbered version's immutable policy snapshot (a GuardrailVersion field added
	// alongside GetGuardrailVersion) must also round-trip.
	gv, err := fresh.GetGuardrailVersion(ids.guardrailID, ids.guardrailVersion)
	require.NoError(t, err)
	require.NotNil(t, gv.Policies)
	require.NotNil(t, gv.Policies.ContentPolicy)
	assert.Equal(t, "HATE", gv.Policies.ContentPolicy.FiltersConfig[0].Type)

	cfgs, _ := fresh.ListEnforcedGuardrailsConfiguration("")
	require.Len(t, cfgs, 1)
	assert.Equal(t, ids.enforcedGuardrailConfigID, cfgs[0].ConfigID)
	assert.Equal(t, ids.guardrailID, cfgs[0].GuardrailID)
	assert.Equal(t, ids.guardrailVersion, cfgs[0].GuardrailVersion)
	assert.Equal(t, "HONOR", cfgs[0].InputTags)
	assert.Equal(t, []string{"model-a"}, cfgs[0].IncludedModels)
	assert.Equal(t, []string{"model-b"}, cfgs[0].ExcludedModels)

	pmt, err := fresh.GetProvisionedModelThroughput(ids.pmtARN)
	require.NoError(t, err)
	assert.Equal(t, "test-pmt", pmt.ProvisionedModelName)

	cm, err := fresh.GetCustomModel(ids.customModelARN)
	require.NoError(t, err)
	assert.Equal(t, "test-model", cm.ModelName)

	cmd, err := fresh.GetCustomModelDeployment(ids.customModelDeployARN)
	require.NoError(t, err)
	assert.Equal(t, ids.customModelARN, cmd.ModelArn)

	// ListFoundationModelAgreementOffers is a stateless catalog lookup (see its
	// doc comment), so it can't itself prove the foundationModelAgreements table
	// round-tripped. DeleteFoundationModelAgreement succeeding proves the row
	// created pre-snapshot ("amazon.titan-text-express-v1", see
	// seedGuardrailAndModelResources) survived Restore.
	require.NoError(t, fresh.DeleteFoundationModelAgreement("amazon.titan-text-express-v1"))
}

// assertJobState verifies the job/policy-family tables round-tripped.
func assertJobState(t *testing.T, fresh *bedrock.InMemoryBackend, ids fixtureIDs) {
	t.Helper()

	evalJob, err := fresh.GetEvaluationJob(ids.evalJobARN)
	require.NoError(t, err)
	assert.Equal(t, "test-eval-job", evalJob.JobName)

	arp, err := fresh.GetAutomatedReasoningPolicy(ids.arpARN)
	require.NoError(t, err)
	assert.Equal(t, "test-arp", arp.Name)

	wf, err := fresh.GetAutomatedReasoningPolicyBuildWorkflow(ids.arpARN, ids.arpWorkflowID)
	require.NoError(t, err)
	assert.Equal(t, ids.arpWorkflowID, wf.BuildWorkflowID)

	tc, err := fresh.GetAutomatedReasoningPolicyTestCase(ids.arpARN, ids.arpTestCaseID)
	require.NoError(t, err)
	assert.Equal(t, ids.arpTestCaseID, tc.TestCaseID)

	arpv, err := fresh.ExportAutomatedReasoningPolicyVersion(
		ids.arpARN + "/version/" + ids.arpVersion,
	)
	require.NoError(t, err)
	assert.Equal(t, "definition-hash-123", arpv["definitionHash"])

	mcj, err := fresh.GetModelCustomizationJob(ids.customizationJobARN)
	require.NoError(t, err)
	assert.Equal(t, "test-cust-job", mcj.JobName)
	assert.Equal(t, "arn:aws:iam::000000000000:role/cust-role", mcj.RoleArn)
	assert.Equal(t, "s3://my-bucket/output/", mcj.OutputDataConfig.S3Uri)
	assert.Equal(t, "s3://my-bucket/training/", mcj.TrainingDataConfig.S3Uri)

	mcpj, err := fresh.GetModelCopyJob(ids.copyJobARN)
	require.NoError(t, err)
	assert.Equal(t, ids.copyJobARN, mcpj.JobArn)

	mij, err := fresh.GetModelImportJob(ids.importJobARN)
	require.NoError(t, err)
	assert.Equal(t, "test-import-job", mij.JobName)

	ip, err := fresh.GetInferenceProfile(ids.inferenceProfileARN)
	require.NoError(t, err)
	assert.Equal(t, "test-inference-profile", ip.InferenceProfileName)
	assert.Equal(
		t,
		"arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-v2",
		ip.ModelSource,
	)

	mme, err := fresh.GetMarketplaceModelEndpoint(ids.marketplaceEndpointARN)
	require.NoError(t, err)
	assert.Equal(t, "test-mp-endpoint", mme.EndpointName)

	invJob, err := fresh.GetModelInvocationJob(ids.invocationJobARN)
	require.NoError(t, err)
	assert.Equal(t, "test-invocation-job", invJob.JobName)

	pr, err := fresh.GetPromptRouter(ids.promptRouterARN)
	require.NoError(t, err)
	assert.Equal(t, "test-prompt-router", pr.PromptRouterName)
}

// assertMiscRawState verifies the remaining raw (non-store.Table) state:
// loggingConfig, useCaseFormData, arpAnnotations, and that the ID counters
// continue from where they left off instead of colliding with pre-snapshot
// IDs.
func assertMiscRawState(t *testing.T, fresh *bedrock.InMemoryBackend, ids fixtureIDs) {
	t.Helper()

	cfg := fresh.GetModelInvocationLoggingConfiguration()
	require.NotNil(t, cfg.S3Config)
	assert.Equal(t, "test-bucket", cfg.S3Config.BucketName)

	uc := fresh.GetUseCaseForModelAccess()
	assert.Equal(t, []byte("test use case form data"), uc)

	anns, err := fresh.GetAutomatedReasoningPolicyAnnotations(ids.arpARN, ids.arpWorkflowID)
	require.NoError(t, err)
	assert.NotNil(t, anns["annotations"])

	// ID counters: a newly created guardrail after restore must not collide
	// with the guardrail created before the snapshot.
	g2, err := fresh.CreateGuardrail("post-restore-guardrail", "d", "in", "out", nil)
	require.NoError(t, err)
	assert.NotEqual(t, ids.guardrailID, g2.GuardrailID)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including a snapshot with no
// version field at all, which decodes as Version == 0) is discarded cleanly
// rather than partially decoded: the backend resets to empty state and
// Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	b, ids := newPersistenceFixture(t)

	err := b.Restore(ctx, []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	_, err = b.GetGuardrail(ids.guardrailID)
	require.ErrorIs(t, err, bedrock.ErrNotFound)

	uc := b.GetUseCaseForModelAccess()
	assert.Empty(t, uc)

	assert.Equal(t, 0, b.GuardrailVersionCounterForTest(ids.guardrailID))

	// Reset ID counters too: the next guardrail minted after a discarded
	// restore must reuse the very first ID, exactly like a brand-new backend.
	g, err := b.CreateGuardrail("post-reset-guardrail", "d", "in", "out", nil)
	require.NoError(t, err)
	assert.Equal(t, "bedrock-guardrail-0000001", g.GuardrailID)
}

// TestInMemoryBackend_RestoreInvalidData verifies malformed JSON surfaces as
// an error rather than being silently discarded (that path is reserved for a
// syntactically valid but version-mismatched snapshot; see
// TestInMemoryBackend_RestoreVersionMismatch).
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := bedrock.NewInMemoryBackend(testAccountID, testRegion)

	err := b.Restore(context.Background(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestHandler_SnapshotRestoreDelegate verifies Handler.Snapshot/Restore
// delegate to its own InMemoryBackend. Before this file, bedrock had no
// persistence at all -- dead wiring in cli.go's setupPersistence (it never
// even attempted a type assertion, since neither method existed), fixed for
// the first time by persistence.go.
func TestHandler_SnapshotRestoreDelegate(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	backend, ids := newPersistenceFixture(t)
	h := bedrock.NewHandler(backend)

	snap := h.Snapshot(ctx)
	require.NotNil(t, snap)

	h2 := bedrock.NewHandler(bedrock.NewInMemoryBackend(testAccountID, testRegion))
	require.NoError(t, h2.Restore(ctx, snap))

	g, err := h2.Backend.GetGuardrail(ids.guardrailID)
	require.NoError(t, err)
	assert.Equal(t, ids.guardrailID, g.GuardrailID)
}
