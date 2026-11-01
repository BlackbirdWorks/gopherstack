package codepipeline

// runBuiltinAction runs an AWS-owned action none of the seam-specific cases in runOneAction took. Providers the
// emulator implements are dispatched; the legacy ones whose seam is unwired keep completing instantly; anything
// else fails rather than reporting work that never happened.
func (b *InMemoryBackend) runBuiltinAction(
	region string,
	p *Pipeline,
	execID string,
	action Action,
	ae *ActionExecution,
) {
	ctx := b.regionContext(region)
	id := action.ActionTypeID

	switch {
	case id.Category == actionCategoryDeploy && id.Provider == actionProviderCloudFormation:
		b.runCloudFormationAction(ctx, p, execID, action, ae)
	case id.Category == actionCategoryDeploy && id.Provider == actionProviderECS:
		b.runECSAction(ctx, p, execID, action, ae)
	case id.Category == actionCategorySource && id.Provider == actionProviderECR:
		b.runECRSourceAction(ctx, p, execID, action, ae)
	case id.Category == actionCategorySource && id.Provider == actionProviderCodeCommit:
		b.runCodeCommitSourceAction(ctx, p, execID, action, ae)
	case id.Category == actionCategorySource && id.Provider == actionProviderCodeStar:
		runCodeStarSourceAction(action, ae)
	case legacyUnwiredProvider(id.Provider):
	default:
		if awsActionKnown(id.Category, id.Provider) {
			failActionf(ae, failureConfigurationError,
				"Action type %s/%s/%s is not supported by this emulator", id.Category, id.Provider, id.Version)
		} else {
			failActionf(ae, failureConfigurationError,
				"Action type %s/%s/%s/%s is not a valid CodePipeline action type",
				id.Category, id.Owner, id.Provider, id.Version)
		}
	}
}

// isAsyncBuiltin reports whether action is an AWS-owned action that stays InProgress until reconcileAsyncActions
// observes its backend resource settle.
func isAsyncBuiltin(action Action) bool {
	return action.ActionTypeID.Category == actionCategoryDeploy &&
		(isBuiltinAction(action, actionProviderCloudFormation) || isBuiltinAction(action, actionProviderECS))
}
