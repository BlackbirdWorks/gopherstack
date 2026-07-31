<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getFISClient } from '$lib/aws-client';
	import {
		CreateExperimentTemplateCommand,
		GetExperimentTemplateCommand,
		UpdateExperimentTemplateCommand,
		DeleteExperimentTemplateCommand,
		ListExperimentTemplatesCommand,
		StartExperimentCommand,
		GetExperimentCommand,
		StopExperimentCommand,
		ListExperimentsCommand,
		ListExperimentResolvedTargetsCommand,
		ListExperimentTargetAccountConfigurationsCommand,
		CreateTargetAccountConfigurationCommand,
		UpdateTargetAccountConfigurationCommand,
		DeleteTargetAccountConfigurationCommand,
		GetTargetAccountConfigurationCommand,
		ListTargetAccountConfigurationsCommand,
		GetActionCommand,
		ListActionsCommand,
		GetTargetResourceTypeCommand,
		ListTargetResourceTypesCommand,
		GetSafetyLeverCommand,
		UpdateSafetyLeverStateCommand,
		type ExperimentTemplate,
		type ExperimentTemplateSummary,
		type Experiment,
		type ExperimentSummary,
		type ResolvedTarget as ResolvedTargetShape,
		type ExperimentTargetAccountConfigurationSummary,
		type TargetAccountConfigurationSummary,
		type Action,
		type ActionSummary,
		type TargetResourceType,
		type TargetResourceTypeSummary,
		type SafetyLever
	} from '@aws-sdk/client-fis';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import type { Column } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { ZapOff, Plus, Trash2, Eye, Pencil, Play, Square, ShieldAlert } from 'lucide-svelte';

	const client = regionalClient(getFISClient);

	// The SDK puts the AWS error code on err.name and status on
	// err.$metadata.httpStatusCode; err.message alone is usually just the
	// human-readable text. Combine them so both the toast and the inline
	// error banner show the actual code, not just a generic message.
	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	// Replaces the JSON.stringify(x) antipattern: search only over the named
	// fields that are actually meaningful for each resource.
	function matches(q: string, ...vals: (string | undefined)[]): boolean {
		if (!q) return true;
		const needle = q.toLowerCase();
		return vals.some((v) => (v ?? '').toLowerCase().includes(needle));
	}

	function parseCommaList(s: string): string[] {
		return s
			.split(',')
			.map((x) => x.trim())
			.filter((x) => x.length > 0);
	}

	// "key=value,key2=value2" -> {key: value, key2: value2}. Used for tags,
	// resourceTags, and action/target parameters, all of which are plain
	// string maps on the wire (CreateExperimentTemplateRequest.tags,
	// CreateExperimentTemplateTargetInput.resourceTags/parameters,
	// CreateExperimentTemplateActionInput.parameters).
	function parseKeyValueList(s: string): Record<string, string> | undefined {
		const entries = s
			.split(',')
			.map((p) => p.trim())
			.filter((p) => p.length > 0)
			.map((p): [string, string] => {
				const idx = p.indexOf('=');
				return idx === -1 ? [p, ''] : [p.slice(0, idx), p.slice(idx + 1)];
			});
		return entries.length > 0 ? Object.fromEntries(entries) : undefined;
	}

	function formatKeyValueList(m: Record<string, string> | undefined): string {
		return Object.entries(m ?? {})
			.map(([k, v]) => `${k}=${v}`)
			.join(', ');
	}

	// The real ListExperimentResolvedTargetsResponse's ResolvedTarget
	// (confirmed against the installed SDK model) has only
	// resourceType/targetName/targetInformation (a generic string map).
	// gopherstack's resolvedTargetDTO (services/fis/models.go) instead emits
	// resolvedArns/targetResourcesCount -- fields the real shape does not
	// have at all -- and never populates targetInformation. A real SDK
	// client would see none of the count/ARN data at all. Read the wire
	// fields gopherstack actually sends (this backend) while keeping the
	// import typed against the real shape; see report.
	type ResolvedTarget = ResolvedTargetShape & { resolvedArns?: string[]; targetResourcesCount?: number };
	function resolvedTargetCount(rt: ResolvedTarget): number {
		return rt.targetResourcesCount ?? rt.resolvedArns?.length ?? Object.keys(rt.targetInformation ?? {}).length;
	}

	type TabId =
		| 'templates'
		| 'experiments'
		| 'targetAccountConfigs'
		| 'actions'
		| 'targetResourceTypes'
		| 'safetyLever';

	const tabs: TabDef[] = [
		{ id: 'templates', label: 'Experiment Templates' },
		{ id: 'experiments', label: 'Experiments' },
		{ id: 'targetAccountConfigs', label: 'Target Account Configs' },
		{ id: 'actions', label: 'Actions' },
		{ id: 'targetResourceTypes', label: 'Target Resource Types' },
		{ id: 'safetyLever', label: 'Safety Lever' }
	];

	let activeTab = $state<TabId>('templates');
	let searchQuery = $state('');

	// ==================== Experiment Templates: list state ====================

	let templates = $state<ExperimentTemplateSummary[]>([]);
	let templatesNextToken = $state<string | undefined>();
	let loadingMoreTemplates = $state(false);

	async function fetchTemplates(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListExperimentTemplatesCommand({ nextToken: reset ? undefined : templatesNextToken })
		);
		templates = reset ? (resp.experimentTemplates ?? []) : [...templates, ...(resp.experimentTemplates ?? [])];
		templatesNextToken = resp.nextToken;
	}

	async function loadMoreTemplates(): Promise<void> {
		loadingMoreTemplates = true;
		try {
			await fetchTemplates(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTemplates = false;
		}
	}

	// ==================== Experiments: list state ====================

	let experiments = $state<ExperimentSummary[]>([]);
	let experimentsNextToken = $state<string | undefined>();
	let loadingMoreExperiments = $state(false);

	async function fetchExperiments(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListExperimentsCommand({ nextToken: reset ? undefined : experimentsNextToken })
		);
		experiments = reset ? (resp.experiments ?? []) : [...experiments, ...(resp.experiments ?? [])];
		experimentsNextToken = resp.nextToken;
	}

	async function loadMoreExperiments(): Promise<void> {
		loadingMoreExperiments = true;
		try {
			await fetchExperiments(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreExperiments = false;
		}
	}

	// ==================== Target Account Configurations: list state ====================
	//
	// Scoped by experiment template (ListTargetAccountConfigurations takes
	// experimentTemplateId as a required request member), so this tab needs a
	// template picker rather than a flat list.
	//
	// The real ListTargetAccountConfigurationsResponse (and
	// ListExperimentTargetAccountConfigurationsResponse /
	// ListExperimentResolvedTargetsResponse, used below) all carry a
	// nextToken for pagination -- confirmed in the installed SDK model.
	// gopherstack's handlers for these three operations
	// (handleListTargetAccountConfigurations,
	// handleListExperimentTargetAccountConfigurations,
	// handleListExperimentResolvedTargets in
	// services/fis/handler_target_account_configurations.go and
	// handler_experiments.go) never call paginateWithToken and never set
	// NextToken on the response DTO -- despite the DTO struct itself having a
	// `NextToken string \`json:"nextToken,omitempty"\`` field -- so they
	// always return the full unpaginated list. There is genuinely nothing to
	// "Load More" against this backend for these three; see report.

	let tacTemplateId = $state('');
	let targetAccountConfigs = $state<TargetAccountConfigurationSummary[]>([]);

	async function fetchTargetAccountConfigs(): Promise<void> {
		if (!tacTemplateId) {
			targetAccountConfigs = [];
			return;
		}
		const resp = await client().send(
			new ListTargetAccountConfigurationsCommand({ experimentTemplateId: tacTemplateId })
		);
		targetAccountConfigs = resp.targetAccountConfigurations ?? [];
	}

	async function initTargetAccountConfigsTab(): Promise<void> {
		await tabLoader.load('templates');
		if (!tacTemplateId) {
			tacTemplateId = templates[0]?.id ?? '';
		}
		await fetchTargetAccountConfigs();
	}

	function handleTacTemplateChange(): void {
		tabLoader.refresh('targetAccountConfigs');
	}

	// ==================== Actions catalog: list state ====================

	let actionsCatalog = $state<ActionSummary[]>([]);
	let actionsNextToken = $state<string | undefined>();
	let loadingMoreActions = $state(false);

	async function fetchActions(reset: boolean): Promise<void> {
		const resp = await client().send(new ListActionsCommand({ nextToken: reset ? undefined : actionsNextToken }));
		actionsCatalog = reset ? (resp.actions ?? []) : [...actionsCatalog, ...(resp.actions ?? [])];
		actionsNextToken = resp.nextToken;
	}

	async function loadMoreActions(): Promise<void> {
		loadingMoreActions = true;
		try {
			await fetchActions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreActions = false;
		}
	}

	// ==================== Target resource types catalog: list state ====================

	let resourceTypes = $state<TargetResourceTypeSummary[]>([]);
	let resourceTypesNextToken = $state<string | undefined>();
	let loadingMoreResourceTypes = $state(false);

	async function fetchResourceTypes(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListTargetResourceTypesCommand({ nextToken: reset ? undefined : resourceTypesNextToken })
		);
		resourceTypes = reset ? (resp.targetResourceTypes ?? []) : [...resourceTypes, ...(resp.targetResourceTypes ?? [])];
		resourceTypesNextToken = resp.nextToken;
	}

	async function loadMoreResourceTypes(): Promise<void> {
		loadingMoreResourceTypes = true;
		try {
			await fetchResourceTypes(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreResourceTypes = false;
		}
	}

	// ==================== Safety lever: singleton state ====================
	//
	// Real AWS FIS has no ListSafetyLevers operation -- confirmed absent from
	// the installed SDK's command list -- because there is exactly one
	// account-level safety lever, not a collection. gopherstack's backend
	// matches this: GetSafetyLever/UpdateSafetyLeverState both resolve the
	// "default" alias to a single per-account SafetyLever
	// (services/fis/safety_levers.go). So this tab is a single-record panel
	// (Get + Update-state), not a table.

	let safetyLever = $state<SafetyLever | null>(null);
	let safetyLeverReason = $state('');
	let updatingSafetyLever = $state(false);

	async function fetchSafetyLever(): Promise<void> {
		const resp = await client().send(new GetSafetyLeverCommand({ id: 'default' }));
		safetyLever = resp.safetyLever ?? null;
	}

	async function toggleSafetyLever(): Promise<void> {
		if (!safetyLever) return;
		const engaging = safetyLever.state?.status !== 'engaged';
		if (engaging) {
			const confirmed = await confirmDestructive({
				title: 'Engage safety lever',
				message: 'Engaging the safety lever blocks every new experiment in this account from starting until it is disengaged.',
				confirmLabel: 'Engage',
				dangerous: true
			});
			if (!confirmed) return;
		}
		updatingSafetyLever = true;
		try {
			const resp = await client().send(
				new UpdateSafetyLeverStateCommand({
					id: 'default',
					state: {
						status: engaging ? 'engaged' : 'disengaged',
						reason: safetyLeverReason || (engaging ? 'Engaged from gopherstack console' : 'Disengaged from gopherstack console')
					}
				})
			);
			safetyLever = resp.safetyLever ?? null;
			safetyLeverReason = '';
			toast.success(engaging ? 'Safety lever engaged' : 'Safety lever disengaged');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			updatingSafetyLever = false;
		}
	}

	// ==================== Tab loader ====================

	const tabLoader = createTabLoader<TabId>({
		templates: () => fetchTemplates(true).catch(rethrowDescribed),
		experiments: () => fetchExperiments(true).catch(rethrowDescribed),
		targetAccountConfigs: () => initTargetAccountConfigsTab().catch(rethrowDescribed),
		actions: () => fetchActions(true).catch(rethrowDescribed),
		targetResourceTypes: () => fetchResourceTypes(true).catch(rethrowDescribed),
		safetyLever: () => fetchSafetyLever().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		tabLoader.refresh(activeTab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredTemplates = $derived(templates.filter((t) => matches(searchQuery, t.id, t.description, t.arn)));
	const filteredExperiments = $derived(
		experiments.filter((e) => matches(searchQuery, e.id, e.experimentTemplateId, e.state?.status))
	);
	const filteredTAC = $derived(
		targetAccountConfigs.filter((c) => matches(searchQuery, c.accountId, c.roleArn, c.description))
	);
	const filteredActions = $derived(
		actionsCatalog.filter((a) => matches(searchQuery, a.id, a.description))
	);
	const filteredResourceTypes = $derived(
		resourceTypes.filter((rt) => matches(searchQuery, rt.resourceType, rt.description))
	);

	// ==================== Experiment Templates: create / update / delete / detail ====================
	//
	// CreateExperimentTemplate takes nested actions/targets/stopConditions
	// maps. Checked against services/fis/experiment_templates.go: unlike
	// several other services audited today, this backend genuinely
	// round-trips every one of these fields end to end -- convertTargetDTOs /
	// convertActionDTOs / convertStopConditionDTOs / cloneTemplate all carry
	// resourceType/selectionMode/resourceArns/resourceTags/parameters/filters
	// (targets), actionId/description/parameters/targets/startAfter
	// (actions), and source/value (stop conditions) through Create, Update,
	// Get, and List with no silent drops. This form intentionally exposes the
	// primary fields (resourceType/selectionMode/resourceArns/resourceTags/
	// parameters on targets; actionId/description/parameters/targets/
	// startAfter on actions) but not per-target `filters` (a nested
	// path+values list) or the template's logConfiguration/
	// experimentReportConfiguration (S3/CloudWatch destinations) -- those are
	// real and are round-tripped by the backend, just out of scope for this
	// pass's form to keep it usable; see report.

	type StopConditionRow = { source: string; value: string };
	type TargetRow = {
		name: string;
		resourceType: string;
		selectionMode: string;
		resourceArns: string;
		resourceTags: string;
		parameters: string;
	};
	type ActionRow = {
		name: string;
		actionId: string;
		description: string;
		parameters: string;
		targetNames: string;
		startAfter: string;
	};

	function newStopConditionRow(): StopConditionRow {
		return { source: 'none', value: '' };
	}
	function newTargetRow(): TargetRow {
		return { name: '', resourceType: '', selectionMode: 'ALL', resourceArns: '', resourceTags: '', parameters: '' };
	}
	function newActionRow(): ActionRow {
		return { name: '', actionId: '', description: '', parameters: '', targetNames: '', startAfter: '' };
	}

	let templateFormModal = $state<Modal | null>(null);
	let templateFormMode = $state<'create' | 'update'>('create');
	let templateFormSaving = $state(false);
	let templateFormError = $state<string | null>(null);
	let editingTemplateId = $state<string | null>(null);
	let tplDescription = $state('');
	let tplRoleArn = $state('');
	let tplTags = $state('');
	let tplAccountTargeting = $state<'' | 'single-account' | 'multi-account'>('');
	let tplEmptyTargetResolutionMode = $state<'' | 'fail' | 'skip'>('');
	let tplStopConditions = $state<StopConditionRow[]>([]);
	let tplTargets = $state<TargetRow[]>([]);
	let tplActions = $state<ActionRow[]>([]);

	function addStopCondition(): void {
		tplStopConditions = [...tplStopConditions, newStopConditionRow()];
	}
	function removeStopCondition(i: number): void {
		tplStopConditions = tplStopConditions.filter((_, idx) => idx !== i);
	}
	function addTarget(): void {
		tplTargets = [...tplTargets, newTargetRow()];
	}
	function removeTarget(i: number): void {
		tplTargets = tplTargets.filter((_, idx) => idx !== i);
	}
	function addAction(): void {
		tplActions = [...tplActions, newActionRow()];
	}
	function removeAction(i: number): void {
		tplActions = tplActions.filter((_, idx) => idx !== i);
	}

	function openCreateTemplateModal(): void {
		templateFormMode = 'create';
		editingTemplateId = null;
		templateFormError = null;
		tplDescription = '';
		tplRoleArn = '';
		tplTags = '';
		tplAccountTargeting = '';
		tplEmptyTargetResolutionMode = '';
		tplStopConditions = [newStopConditionRow()];
		tplTargets = [];
		tplActions = [];
		templateFormModal?.open();
	}

	async function openUpdateTemplateModal(t: ExperimentTemplateSummary): Promise<void> {
		if (!t.id) return;
		try {
			const resp = await client().send(new GetExperimentTemplateCommand({ id: t.id }));
			const tpl = resp.experimentTemplate;
			if (!tpl) return;
			templateFormMode = 'update';
			editingTemplateId = t.id;
			templateFormError = null;
			tplDescription = tpl.description ?? '';
			tplRoleArn = tpl.roleArn ?? '';
			tplTags = formatKeyValueList(tpl.tags);
			tplAccountTargeting = (tpl.experimentOptions?.accountTargeting as typeof tplAccountTargeting) ?? '';
			tplEmptyTargetResolutionMode =
				(tpl.experimentOptions?.emptyTargetResolutionMode as typeof tplEmptyTargetResolutionMode) ?? '';
			tplStopConditions = (tpl.stopConditions ?? []).map((sc) => ({
				source: sc.source ?? '',
				value: sc.value ?? ''
			}));
			tplTargets = Object.entries(tpl.targets ?? {}).map(([name, tgt]) => ({
				name,
				resourceType: tgt.resourceType ?? '',
				selectionMode: tgt.selectionMode ?? 'ALL',
				resourceArns: (tgt.resourceArns ?? []).join(', '),
				resourceTags: formatKeyValueList(tgt.resourceTags),
				parameters: formatKeyValueList(tgt.parameters)
			}));
			tplActions = Object.entries(tpl.actions ?? {}).map(([name, a]) => ({
				name,
				actionId: a.actionId ?? '',
				description: a.description ?? '',
				parameters: formatKeyValueList(a.parameters),
				targetNames: Object.values(a.targets ?? {}).join(', '),
				startAfter: (a.startAfter ?? []).join(', ')
			}));
			templateFormModal?.open();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	function buildTemplateTargets() {
		const out: Record<
			string,
			{
				resourceType: string;
				selectionMode: string;
				resourceArns?: string[];
				resourceTags?: Record<string, string>;
				parameters?: Record<string, string>;
			}
		> = {};
		for (const row of tplTargets) {
			const name = row.name.trim();
			if (!name) continue;
			const arns = parseCommaList(row.resourceArns);
			out[name] = {
				resourceType: row.resourceType,
				selectionMode: row.selectionMode,
				resourceArns: arns.length > 0 ? arns : undefined,
				resourceTags: parseKeyValueList(row.resourceTags),
				parameters: parseKeyValueList(row.parameters)
			};
		}
		return out;
	}

	function buildTemplateActions() {
		const out: Record<
			string,
			{
				actionId: string;
				description?: string;
				parameters?: Record<string, string>;
				targets?: Record<string, string>;
				startAfter?: string[];
			}
		> = {};
		for (const row of tplActions) {
			const name = row.name.trim();
			if (!name || !row.actionId.trim()) continue;
			const targetNames = parseCommaList(row.targetNames);
			const startAfter = parseCommaList(row.startAfter);
			out[name] = {
				actionId: row.actionId.trim(),
				description: row.description || undefined,
				parameters: parseKeyValueList(row.parameters),
				// gopherstack's validateActionReferences (services/fis/experiment_templates.go)
				// only checks the map VALUES of an action's `targets` against defined
				// target names, not the keys -- so the target's own name doubles as
				// its own map key here rather than guessing the action-type-specific
				// parameter name (e.g. "Instances") real AWS documents per built-in
				// action.
				targets: targetNames.length > 0 ? Object.fromEntries(targetNames.map((n) => [n, n])) : undefined,
				startAfter: startAfter.length > 0 ? startAfter : undefined
			};
		}
		return out;
	}

	async function submitTemplateForm(): Promise<void> {
		if (!tplRoleArn.trim()) {
			templateFormError = 'IAM role ARN is required.';
			return;
		}
		if (templateFormMode === 'create' && tplStopConditions.filter((r) => r.source.trim()).length === 0) {
			templateFormError = 'At least one stop condition is required.';
			return;
		}
		templateFormSaving = true;
		templateFormError = null;
		const stopConditions = tplStopConditions
			.filter((r) => r.source.trim())
			.map((r) => ({ source: r.source.trim(), value: r.value.trim() || undefined }));
		const experimentOptions =
			tplAccountTargeting || tplEmptyTargetResolutionMode
				? {
						accountTargeting: tplAccountTargeting || undefined,
						emptyTargetResolutionMode: tplEmptyTargetResolutionMode || undefined
					}
				: undefined;
		try {
			if (templateFormMode === 'create') {
				await client().send(
					new CreateExperimentTemplateCommand({
						description: tplDescription,
						roleArn: tplRoleArn.trim(),
						tags: parseKeyValueList(tplTags),
						stopConditions,
						targets: buildTemplateTargets(),
						actions: buildTemplateActions(),
						experimentOptions
					})
				);
				toast.success('Experiment template created');
			} else if (editingTemplateId) {
				await client().send(
					new UpdateExperimentTemplateCommand({
						id: editingTemplateId,
						description: tplDescription || undefined,
						roleArn: tplRoleArn.trim() || undefined,
						stopConditions: stopConditions.length > 0 ? stopConditions : undefined,
						targets: buildTemplateTargets(),
						actions: buildTemplateActions(),
						experimentOptions
					})
				);
				toast.success('Experiment template updated');
			}
			templateFormModal?.close();
			await tabLoader.refresh('templates');
		} catch (e) {
			const msg = describeError(e);
			templateFormError = msg;
			toast.error(msg);
		} finally {
			templateFormSaving = false;
		}
	}

	async function handleDeleteTemplate(t: ExperimentTemplateSummary): Promise<void> {
		if (!t.id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete experiment template',
			message: `Delete experiment template ${t.id}? This also removes its target account configurations.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteExperimentTemplateCommand({ id: t.id }));
			toast.success('Experiment template deleted');
			await tabLoader.refresh('templates');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let templateDetailModal = $state<Modal | null>(null);
	let viewedTemplate = $state<ExperimentTemplate | null>(null);

	async function openTemplateDetail(t: ExperimentTemplateSummary): Promise<void> {
		if (!t.id) return;
		viewedTemplate = null;
		templateDetailModal?.open();
		try {
			const resp = await client().send(new GetExperimentTemplateCommand({ id: t.id }));
			viewedTemplate = resp.experimentTemplate ?? null;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Experiments: start / stop / detail ====================
	//
	// Real AWS FIS has no CreateExperiment or DeleteExperiment -- an
	// experiment is started FROM a template (StartExperiment) and halted
	// (StopExperiment); it is never independently created or deleted.
	// Confirmed absent from both the installed SDK's command list and
	// services/fis's GetSupportedOperations(). So this tab offers Start (in
	// place of Create) and Stop (in place of Delete) using the real verbs
	// rather than forcing CRUD names onto an API that doesn't have them.

	let startExperimentModal = $state<Modal | null>(null);
	let startingExperiment = $state(false);
	let startExperimentError = $state<string | null>(null);
	let startTemplateId = $state('');
	let startActionsMode = $state<'run-all' | 'skip-all'>('run-all');

	async function openStartExperimentModal(): Promise<void> {
		startExperimentError = null;
		startActionsMode = 'run-all';
		await tabLoader.load('templates');
		startTemplateId = templates[0]?.id ?? '';
		startExperimentModal?.open();
	}

	async function submitStartExperiment(): Promise<void> {
		if (!startTemplateId) {
			startExperimentError = 'Select an experiment template.';
			return;
		}
		startingExperiment = true;
		startExperimentError = null;
		try {
			await client().send(
				new StartExperimentCommand({
					experimentTemplateId: startTemplateId,
					experimentOptions: { actionsMode: startActionsMode }
				})
			);
			toast.success('Experiment started');
			startExperimentModal?.close();
			await tabLoader.refresh('experiments');
		} catch (e) {
			const msg = describeError(e);
			startExperimentError = msg;
			toast.error(msg);
		} finally {
			startingExperiment = false;
		}
	}

	async function handleStopExperiment(e: ExperimentSummary): Promise<void> {
		if (!e.id) return;
		const confirmed = await confirmDestructive({
			title: 'Stop experiment',
			message: `Stop experiment ${e.id}? Fault injection halts and any in-flight rollback runs.`,
			confirmLabel: 'Stop',
			dangerous: false
		});
		if (!confirmed) return;
		try {
			await client().send(new StopExperimentCommand({ id: e.id }));
			toast.success('Experiment stopped');
			await tabLoader.refresh('experiments');
		} catch (err) {
			toast.error(describeError(err));
		}
	}

	let experimentDetailModal = $state<Modal | null>(null);
	let viewedExperiment = $state<Experiment | null>(null);
	let viewedResolvedTargets = $state<ResolvedTarget[]>([]);
	let viewedExperimentTAC = $state<ExperimentTargetAccountConfigurationSummary[]>([]);

	async function openExperimentDetail(e: ExperimentSummary): Promise<void> {
		if (!e.id) return;
		viewedExperiment = null;
		viewedResolvedTargets = [];
		viewedExperimentTAC = [];
		experimentDetailModal?.open();
		try {
			const resp = await client().send(new GetExperimentCommand({ id: e.id }));
			viewedExperiment = resp.experiment ?? null;
			// Neither of these two nested reads is paginated by this backend
			// today (see the Target Account Configurations section above for
			// why) -- one unpaged call each is the whole story.
			const [resolvedResp, tacResp] = await Promise.all([
				client().send(new ListExperimentResolvedTargetsCommand({ experimentId: e.id })),
				client().send(new ListExperimentTargetAccountConfigurationsCommand({ experimentId: e.id }))
			]);
			viewedResolvedTargets = resolvedResp.resolvedTargets ?? [];
			viewedExperimentTAC = tacResp.targetAccountConfigurations ?? [];
		} catch (err) {
			toast.error(describeError(err));
		}
	}

	// ==================== Target Account Configurations: create / update / delete / detail ====================

	let tacFormModal = $state<Modal | null>(null);
	let tacFormMode = $state<'create' | 'update'>('create');
	let tacSaving = $state(false);
	let tacFormError = $state<string | null>(null);
	let tacEditingAccountId = $state<string | null>(null);
	let tacFormAccountId = $state('');
	let tacFormRoleArn = $state('');
	let tacFormDescription = $state('');

	function openCreateTAC(): void {
		if (!tacTemplateId) {
			toast.error('Select an experiment template first.');
			return;
		}
		tacFormMode = 'create';
		tacEditingAccountId = null;
		tacFormError = null;
		tacFormAccountId = '';
		tacFormRoleArn = '';
		tacFormDescription = '';
		tacFormModal?.open();
	}

	function openUpdateTAC(cfg: TargetAccountConfigurationSummary): void {
		tacFormMode = 'update';
		tacEditingAccountId = cfg.accountId ?? null;
		tacFormError = null;
		tacFormAccountId = cfg.accountId ?? '';
		tacFormRoleArn = cfg.roleArn ?? '';
		tacFormDescription = cfg.description ?? '';
		tacFormModal?.open();
	}

	async function submitTAC(): Promise<void> {
		if (!tacTemplateId) return;
		if (tacFormMode === 'create' && (!tacFormAccountId.trim() || !tacFormRoleArn.trim())) {
			tacFormError = 'Account ID and IAM role ARN are required.';
			return;
		}
		tacSaving = true;
		tacFormError = null;
		try {
			if (tacFormMode === 'create') {
				await client().send(
					new CreateTargetAccountConfigurationCommand({
						experimentTemplateId: tacTemplateId,
						accountId: tacFormAccountId.trim(),
						roleArn: tacFormRoleArn.trim(),
						description: tacFormDescription || undefined
					})
				);
				toast.success('Target account configuration created');
			} else if (tacEditingAccountId) {
				await client().send(
					new UpdateTargetAccountConfigurationCommand({
						experimentTemplateId: tacTemplateId,
						accountId: tacEditingAccountId,
						roleArn: tacFormRoleArn.trim() || undefined,
						description: tacFormDescription || undefined
					})
				);
				toast.success('Target account configuration updated');
			}
			tacFormModal?.close();
			await tabLoader.refresh('targetAccountConfigs');
		} catch (e) {
			const msg = describeError(e);
			tacFormError = msg;
			toast.error(msg);
		} finally {
			tacSaving = false;
		}
	}

	async function handleDeleteTAC(cfg: TargetAccountConfigurationSummary): Promise<void> {
		if (!tacTemplateId || !cfg.accountId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete target account configuration',
			message: `Delete the target account configuration for account ${cfg.accountId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteTargetAccountConfigurationCommand({ experimentTemplateId: tacTemplateId, accountId: cfg.accountId })
			);
			toast.success('Target account configuration deleted');
			await tabLoader.refresh('targetAccountConfigs');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let tacDetailModal = $state<Modal | null>(null);
	let viewedTAC = $state<TargetAccountConfigurationSummary | null>(null);

	async function openTACDetail(cfg: TargetAccountConfigurationSummary): Promise<void> {
		if (!tacTemplateId || !cfg.accountId) return;
		viewedTAC = null;
		tacDetailModal?.open();
		try {
			const resp = await client().send(
				new GetTargetAccountConfigurationCommand({ experimentTemplateId: tacTemplateId, accountId: cfg.accountId })
			);
			viewedTAC = resp.targetAccountConfiguration ?? null;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Actions catalog: detail ====================
	//
	// GetAction/ListActions are a read-only built-in catalog (fault action
	// types like aws:ec2:stop-instances) -- there is no
	// Create/Update/DeleteAction in real AWS FIS, confirmed absent from the
	// installed SDK's command list.

	let actionDetailModal = $state<Modal | null>(null);
	let viewedAction = $state<Action | null>(null);

	async function openActionDetail(a: ActionSummary): Promise<void> {
		if (!a.id) return;
		viewedAction = null;
		actionDetailModal?.open();
		try {
			const resp = await client().send(new GetActionCommand({ id: a.id }));
			viewedAction = resp.action ?? null;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Target resource types catalog: detail ====================
	//
	// Same story as Actions: GetTargetResourceType/ListTargetResourceTypes
	// are a read-only catalog of resource types targets can select
	// (ec2:instance, ecs:cluster, ...), with no create/update/delete verbs in
	// real AWS FIS.

	let resourceTypeDetailModal = $state<Modal | null>(null);
	let viewedResourceType = $state<TargetResourceType | null>(null);

	async function openResourceTypeDetail(rt: TargetResourceTypeSummary): Promise<void> {
		if (!rt.resourceType) return;
		viewedResourceType = null;
		resourceTypeDetailModal?.open();
		try {
			const resp = await client().send(new GetTargetResourceTypeCommand({ resourceType: rt.resourceType }));
			viewedResourceType = resp.targetResourceType ?? null;
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<!-- Experiment Template modals -->
<Modal bind:this={templateFormModal} title={templateFormMode === 'create' ? 'Create Experiment Template' : 'Update Experiment Template'}>
	{#snippet children()}
		<div class="space-y-5 max-h-[65vh] overflow-y-auto pr-1">
			{#if templateFormError}
				<div class="text-sm text-red-600 dark:text-red-400">{templateFormError}</div>
			{/if}
			<div>
				<label for="tpl-description" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Description</label>
				<input id="tpl-description" bind:value={tplDescription} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="Terminate one EC2 instance" />
			</div>
			<div>
				<label for="tpl-role-arn" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">IAM Role ARN</label>
				<input id="tpl-role-arn" bind:value={tplRoleArn} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" placeholder="arn:aws:iam::123456789012:role/fis-role" />
			</div>
			<div>
				<label for="tpl-tags" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Tags (key=value, comma-separated)</label>
				<input id="tpl-tags" bind:value={tplTags} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="env=staging, owner=sre" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="tpl-account-targeting" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Account Targeting</label>
					<select id="tpl-account-targeting" bind:value={tplAccountTargeting} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="">(unset)</option>
						<option value="single-account">single-account</option>
						<option value="multi-account">multi-account</option>
					</select>
				</div>
				<div>
					<label for="tpl-empty-resolution" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Empty Target Resolution</label>
					<select id="tpl-empty-resolution" bind:value={tplEmptyTargetResolutionMode} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="">(unset)</option>
						<option value="fail">fail</option>
						<option value="skip">skip</option>
					</select>
				</div>
			</div>

			<div class="border-t border-gray-100 dark:border-gray-800 pt-4">
				<div class="flex items-center justify-between mb-2">
					<h3 class="text-xs font-semibold uppercase tracking-wide text-gray-500">Stop Conditions</h3>
					<button type="button" onclick={addStopCondition} class="text-xs text-blue-600 dark:text-blue-400 hover:underline">+ Add</button>
				</div>
				<div class="space-y-2">
					{#each tplStopConditions as row, i (i)}
						<div class="flex gap-2 items-center">
							<input bind:value={row.source} type="text" placeholder="none or aws:cloudwatch:alarm:..." class="flex-1 px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs font-mono" aria-label="Stop condition {i + 1} source" />
							<input bind:value={row.value} type="text" placeholder="value (alarm ARN)" class="flex-1 px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs font-mono" aria-label="Stop condition {i + 1} value" />
							<button type="button" onclick={() => removeStopCondition(i)} aria-label="Remove stop condition {i + 1}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/each}
					{#if tplStopConditions.length === 0}
						<p class="text-xs text-gray-400 italic">No stop conditions yet.</p>
					{/if}
				</div>
			</div>

			<div class="border-t border-gray-100 dark:border-gray-800 pt-4">
				<div class="flex items-center justify-between mb-2">
					<h3 class="text-xs font-semibold uppercase tracking-wide text-gray-500">Targets</h3>
					<button type="button" onclick={addTarget} class="text-xs text-blue-600 dark:text-blue-400 hover:underline">+ Add</button>
				</div>
				<div class="space-y-3">
					{#each tplTargets as row, i (i)}
						<div class="p-3 rounded-lg border border-gray-200 dark:border-gray-700 space-y-2">
							<div class="flex justify-between items-center">
								<span class="text-[10px] uppercase tracking-wide text-gray-400">Target {i + 1}</span>
								<button type="button" onclick={() => removeTarget(i)} aria-label="Remove target {i + 1}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
							</div>
							<div class="grid grid-cols-2 gap-2">
								<input bind:value={row.name} type="text" placeholder="Name (e.g. myInstances)" class="px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs" aria-label="Target {i + 1} name" />
								<input bind:value={row.resourceType} type="text" placeholder="resourceType (e.g. aws:ec2:instance)" class="px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs font-mono" aria-label="Target {i + 1} resource type" />
							</div>
							<input bind:value={row.selectionMode} type="text" placeholder="selectionMode: ALL, COUNT(1), PERCENT(50)" class="w-full px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs font-mono" aria-label="Target {i + 1} selection mode" />
							<input bind:value={row.resourceArns} type="text" placeholder="resourceArns (comma-separated)" class="w-full px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs font-mono" aria-label="Target {i + 1} resource ARNs" />
							<input bind:value={row.resourceTags} type="text" placeholder="resourceTags (key=value, comma-separated)" class="w-full px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs" aria-label="Target {i + 1} resource tags" />
							<input bind:value={row.parameters} type="text" placeholder="parameters (key=value, comma-separated)" class="w-full px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs" aria-label="Target {i + 1} parameters" />
						</div>
					{/each}
					{#if tplTargets.length === 0}
						<p class="text-xs text-gray-400 italic">No targets yet.</p>
					{/if}
				</div>
			</div>

			<div class="border-t border-gray-100 dark:border-gray-800 pt-4">
				<div class="flex items-center justify-between mb-2">
					<h3 class="text-xs font-semibold uppercase tracking-wide text-gray-500">Actions</h3>
					<button type="button" onclick={addAction} class="text-xs text-blue-600 dark:text-blue-400 hover:underline">+ Add</button>
				</div>
				<div class="space-y-3">
					{#each tplActions as row, i (i)}
						<div class="p-3 rounded-lg border border-gray-200 dark:border-gray-700 space-y-2">
							<div class="flex justify-between items-center">
								<span class="text-[10px] uppercase tracking-wide text-gray-400">Action {i + 1}</span>
								<button type="button" onclick={() => removeAction(i)} aria-label="Remove action {i + 1}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
							</div>
							<div class="grid grid-cols-2 gap-2">
								<input bind:value={row.name} type="text" placeholder="Name (e.g. terminateInstances)" class="px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs" aria-label="Action {i + 1} name" />
								<input bind:value={row.actionId} type="text" placeholder="actionId (e.g. aws:ec2:terminate-instances)" class="px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs font-mono" aria-label="Action {i + 1} action ID" />
							</div>
							<input bind:value={row.description} type="text" placeholder="Description (optional)" class="w-full px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs" aria-label="Action {i + 1} description" />
							<input bind:value={row.parameters} type="text" placeholder="parameters (key=value, comma-separated; e.g. duration=PT2M)" class="w-full px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs" aria-label="Action {i + 1} parameters" />
							<input bind:value={row.targetNames} type="text" placeholder="target names referenced (comma-separated)" class="w-full px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs" aria-label="Action {i + 1} target references" />
							<input bind:value={row.startAfter} type="text" placeholder="startAfter action names (comma-separated)" class="w-full px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-xs" aria-label="Action {i + 1} start-after dependencies" />
						</div>
					{/each}
					{#if tplActions.length === 0}
						<p class="text-xs text-gray-400 italic">No actions yet.</p>
					{/if}
				</div>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => templateFormModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitTemplateForm} disabled={templateFormSaving} class="px-4 py-2 text-sm rounded-lg bg-rose-600 text-white hover:bg-rose-700 disabled:opacity-50">{templateFormSaving ? 'Saving…' : templateFormMode === 'create' ? 'Create' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={templateDetailModal} title="Experiment Template Details">
	{#snippet children()}
		{#if viewedTemplate}
			<div class="space-y-4 text-sm max-h-[65vh] overflow-y-auto pr-1">
				{#each [
					['ID', viewedTemplate.id],
					['ARN', viewedTemplate.arn],
					['Description', viewedTemplate.description],
					['Role ARN', viewedTemplate.roleArn],
					['Created', formatDate(viewedTemplate.creationTime)],
					['Updated', formatDate(viewedTemplate.lastUpdateTime)],
					['Tags', formatKeyValueList(viewedTemplate.tags) || '—']
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}
				<div>
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Stop Conditions ({viewedTemplate.stopConditions?.length ?? 0})</h4>
					{#each viewedTemplate.stopConditions ?? [] as sc, i (i)}
						<div class="text-xs font-mono text-gray-700 dark:text-gray-300">{sc.source}{sc.value ? `: ${sc.value}` : ''}</div>
					{/each}
				</div>
				<div>
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Targets ({Object.keys(viewedTemplate.targets ?? {}).length})</h4>
					{#each Object.entries(viewedTemplate.targets ?? {}) as [name, tgt] (name)}
						<div class="text-xs font-mono text-gray-700 dark:text-gray-300 mb-1">{name}: {tgt.resourceType} / {tgt.selectionMode} {tgt.resourceArns?.length ? `(${tgt.resourceArns.length} ARNs)` : ''}</div>
					{/each}
				</div>
				<div>
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Actions ({Object.keys(viewedTemplate.actions ?? {}).length})</h4>
					{#each Object.entries(viewedTemplate.actions ?? {}) as [name, a] (name)}
						<div class="text-xs font-mono text-gray-700 dark:text-gray-300 mb-1">{name}: {a.actionId} {Object.keys(a.targets ?? {}).length ? `→ ${Object.values(a.targets ?? {}).join(', ')}` : ''}</div>
					{/each}
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => templateDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Start Experiment modal -->
<Modal bind:this={startExperimentModal} title="Start Experiment">
	{#snippet children()}
		<div class="space-y-4">
			{#if startExperimentError}
				<div class="text-sm text-red-600 dark:text-red-400">{startExperimentError}</div>
			{/if}
			<div>
				<label for="start-template" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Experiment Template</label>
				<select id="start-template" bind:value={startTemplateId} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					{#each templates as t (t.id)}
						<option value={t.id}>{t.id} — {t.description ?? 'no description'}</option>
					{/each}
				</select>
				{#if templates.length === 0}
					<p class="text-xs text-gray-400 mt-1">No experiment templates exist yet. Create one first.</p>
				{/if}
			</div>
			<div>
				<label for="start-actions-mode" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Actions Mode</label>
				<select id="start-actions-mode" bind:value={startActionsMode} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="run-all">run-all — inject faults</option>
					<option value="skip-all">skip-all — dry run, validates targets/permissions only</option>
				</select>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => startExperimentModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitStartExperiment} disabled={startingExperiment || !startTemplateId} class="px-4 py-2 text-sm rounded-lg bg-rose-600 text-white hover:bg-rose-700 disabled:opacity-50">{startingExperiment ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<Modal bind:this={experimentDetailModal} title="Experiment Details">
	{#snippet children()}
		{#if viewedExperiment}
			<div class="space-y-4 text-sm max-h-[65vh] overflow-y-auto pr-1">
				{#each [
					['ID', viewedExperiment.id],
					['ARN', viewedExperiment.arn],
					['Template', viewedExperiment.experimentTemplateId],
					['Status', viewedExperiment.state?.status],
					['Reason', viewedExperiment.state?.reason || '—'],
					['Actions Mode', viewedExperiment.experimentOptions?.actionsMode],
					['Started', formatDate(viewedExperiment.startTime)],
					['Ended', formatDate(viewedExperiment.endTime)]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}
				{#if viewedExperiment.state?.error}
					<div class="text-xs text-red-600 dark:text-red-400 font-mono">
						Error: {viewedExperiment.state.error.code} @ {viewedExperiment.state.error.location} ({viewedExperiment.state.error.accountId})
					</div>
				{/if}
				<div>
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Actions ({Object.keys(viewedExperiment.actions ?? {}).length})</h4>
					{#each Object.entries(viewedExperiment.actions ?? {}) as [name, a] (name)}
						<div class="text-xs font-mono text-gray-700 dark:text-gray-300 mb-1">{name}: {a.actionId} — {a.state?.status ?? 'pending'}</div>
					{/each}
				</div>
				<div>
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Targets ({Object.keys(viewedExperiment.targets ?? {}).length})</h4>
					{#each Object.entries(viewedExperiment.targets ?? {}) as [name, tgt] (name)}
						<div class="text-xs font-mono text-gray-700 dark:text-gray-300 mb-1">{name}: {tgt.resourceType} / {tgt.selectionMode}</div>
					{/each}
				</div>
				<div>
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Resolved Targets ({viewedResolvedTargets.length})</h4>
					{#each viewedResolvedTargets as rt, i (i)}
						<div class="text-xs font-mono text-gray-700 dark:text-gray-300 mb-1">{rt.targetName}: {resolvedTargetCount(rt)} resource(s) ({rt.resourceType})</div>
					{:else}
						<p class="text-xs text-gray-400 italic">None resolved.</p>
					{/each}
				</div>
				{#if viewedExperiment.targetAccountConfigurationsCount}
					<div>
						<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Target Account Configs ({viewedExperimentTAC.length})</h4>
						{#each viewedExperimentTAC as cfg, i (i)}
							<div class="text-xs font-mono text-gray-700 dark:text-gray-300 mb-1">{cfg.accountId}: {cfg.roleArn}</div>
						{/each}
					</div>
				{/if}
				{#if viewedExperiment.experimentReport}
					<div>
						<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Report</h4>
						<div class="text-xs font-mono text-gray-700 dark:text-gray-300">{viewedExperiment.experimentReport.state?.status ?? '—'}</div>
					</div>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => experimentDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Target Account Configuration modals -->
<Modal bind:this={tacFormModal} title={tacFormMode === 'create' ? 'Create Target Account Configuration' : 'Update Target Account Configuration'}>
	{#snippet children()}
		<div class="space-y-4">
			{#if tacFormError}
				<div class="text-sm text-red-600 dark:text-red-400">{tacFormError}</div>
			{/if}
			<div>
				<label for="tac-account-id" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Target Account ID</label>
				<input id="tac-account-id" bind:value={tacFormAccountId} type="text" disabled={tacFormMode === 'update'} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono disabled:opacity-60" placeholder="123456789012" />
			</div>
			<div>
				<label for="tac-role-arn" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Target Account IAM Role ARN</label>
				<input id="tac-role-arn" bind:value={tacFormRoleArn} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" placeholder="arn:aws:iam::123456789012:role/fis-role" />
			</div>
			<div>
				<label for="tac-description" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Target Account Description</label>
				<input id="tac-description" bind:value={tacFormDescription} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => tacFormModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitTAC} disabled={tacSaving} class="px-4 py-2 text-sm rounded-lg bg-rose-600 text-white hover:bg-rose-700 disabled:opacity-50">{tacSaving ? 'Saving…' : tacFormMode === 'create' ? 'Create' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={tacDetailModal} title="Target Account Configuration Details">
	{#snippet children()}
		{#if viewedTAC}
			<div class="space-y-2 text-sm">
				{#each [
					['Account ID', viewedTAC.accountId],
					['Role ARN', viewedTAC.roleArn],
					['Description', viewedTAC.description]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => tacDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Action catalog detail -->
<Modal bind:this={actionDetailModal} title="Action Details">
	{#snippet children()}
		{#if viewedAction}
			<div class="space-y-4 text-sm">
				{#each [
					['ID', viewedAction.id],
					['ARN', viewedAction.arn],
					['Description', viewedAction.description]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}
				<div>
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Parameters</h4>
					{#each Object.entries(viewedAction.parameters ?? {}) as [name, p] (name)}
						<div class="text-xs font-mono text-gray-700 dark:text-gray-300 mb-1">{name}{p.required ? ' (required)' : ''}: {p.description ?? '—'}</div>
					{:else}
						<p class="text-xs text-gray-400 italic">None.</p>
					{/each}
				</div>
				<div>
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Targets</h4>
					{#each Object.entries(viewedAction.targets ?? {}) as [name, t] (name)}
						<div class="text-xs font-mono text-gray-700 dark:text-gray-300 mb-1">{name}: {t.resourceType}</div>
					{:else}
						<p class="text-xs text-gray-400 italic">None.</p>
					{/each}
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => actionDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Target resource type catalog detail -->
<Modal bind:this={resourceTypeDetailModal} title="Target Resource Type Details">
	{#snippet children()}
		{#if viewedResourceType}
			<div class="space-y-4 text-sm">
				{#each [
					['Resource Type', viewedResourceType.resourceType],
					['Description', viewedResourceType.description]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}
				<div>
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Parameters</h4>
					{#each Object.entries(viewedResourceType.parameters ?? {}) as [name, p] (name)}
						<div class="text-xs font-mono text-gray-700 dark:text-gray-300 mb-1">{name}{p.required ? ' (required)' : ''}: {p.description ?? '—'}</div>
					{:else}
						<p class="text-xs text-gray-400 italic">None.</p>
					{/each}
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => resourceTypeDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<div class="p-6 space-y-6">
	<PageHeader
		icon={ZapOff}
		title="AWS Fault Injection Simulator"
		description="Design and run controlled fault-injection experiments against your AWS resources"
		onRefresh={handleRefresh}
		color="rose"
	>
		{#snippet actions()}
			{#if activeTab === 'templates'}
				<button onclick={openCreateTemplateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 text-sm">
					<Plus class="w-4 h-4" /> Create template
				</button>
			{:else if activeTab === 'experiments'}
				<button onclick={openStartExperimentModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 text-sm">
					<Play class="w-4 h-4" /> Start experiment
				</button>
			{:else if activeTab === 'targetAccountConfigs'}
				<button onclick={openCreateTAC} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 text-sm">
					<Plus class="w-4 h-4" /> Create config
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="rose" />
			{#if activeTab !== 'safetyLever'}
				<SearchInput bind:value={searchQuery} />
			{/if}
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'templates'}
				{#snippet tplActionsCell(t: ExperimentTemplateSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openTemplateDetail(t)} title="View" aria-label="View template {t.id}" class="text-gray-400 hover:text-rose-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateTemplateModal(t)} title="Update" aria-label="Update template {t.id}" class="text-gray-400 hover:text-rose-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteTemplate(t)} title="Delete" aria-label="Delete template {t.id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const tplColumns = [
					{ key: 'id', label: 'ID' },
					{ key: 'description', label: 'Description' },
					{ key: 'creationTime', label: 'Created', render: (t: ExperimentTemplateSummary) => formatDate(t.creationTime) },
					{ key: 'lastUpdateTime', label: 'Updated', render: (t: ExperimentTemplateSummary) => formatDate(t.lastUpdateTime) },
					{ key: 'actions', label: '', render: tplActionsCell }
				] as Column<ExperimentTemplateSummary>[]}
				<DataTable
					rows={filteredTemplates}
					rowKey={(t) => t.id ?? ''}
					columns={tplColumns}
					loading={tabLoader.isLoading('templates')}
					emptyMessage="No experiment templates found"
				/>
				<LoadMore hasMore={!!templatesNextToken} loading={loadingMoreTemplates} onLoadMore={loadMoreTemplates} />
			{:else if activeTab === 'experiments'}
				{#snippet expStatusCell(e: ExperimentSummary)}
					<span class="text-xs px-2 py-1 rounded-full {e.state?.status === 'running' ? 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400' : e.state?.status === 'failed' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400' : 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'}">{e.state?.status ?? '—'}</span>
				{/snippet}
				{#snippet expActionsCell(e: ExperimentSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openExperimentDetail(e)} title="View" aria-label="View experiment {e.id}" class="text-gray-400 hover:text-rose-500"><Eye class="w-4 h-4" /></button>
						{#if e.state?.status === 'running' || e.state?.status === 'initiating' || e.state?.status === 'pending'}
							<button onclick={() => handleStopExperiment(e)} title="Stop" aria-label="Stop experiment {e.id}" class="text-gray-400 hover:text-red-500"><Square class="w-4 h-4" /></button>
						{/if}
					</div>
				{/snippet}
				{@const expColumns = [
					{ key: 'id', label: 'ID' },
					{ key: 'experimentTemplateId', label: 'Template' },
					{ key: 'state', label: 'Status', render: expStatusCell },
					{ key: 'actions', label: '', render: expActionsCell }
				] as Column<ExperimentSummary>[]}
				<DataTable
					rows={filteredExperiments}
					rowKey={(e) => e.id ?? ''}
					columns={expColumns}
					loading={tabLoader.isLoading('experiments')}
					emptyMessage="No experiments found"
				/>
				<LoadMore hasMore={!!experimentsNextToken} loading={loadingMoreExperiments} onLoadMore={loadMoreExperiments} />
			{:else if activeTab === 'targetAccountConfigs'}
				<div class="flex items-center gap-2 mb-2">
					<label for="tac-template-select" class="text-xs font-medium text-gray-600 dark:text-gray-400">Experiment Template</label>
					<select id="tac-template-select" bind:value={tacTemplateId} onchange={handleTacTemplateChange} class="px-3 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						{#each templates as t (t.id)}
							<option value={t.id}>{t.id} — {t.description ?? 'no description'}</option>
						{/each}
					</select>
				</div>
				{#if !tacTemplateId}
					<p class="text-sm text-gray-400 italic">No experiment templates exist yet. Create one on the Experiment Templates tab first.</p>
				{:else}
					{#snippet tacActionsCell(c: TargetAccountConfigurationSummary)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openTACDetail(c)} title="View" aria-label="View target account config {c.accountId}" class="text-gray-400 hover:text-rose-500"><Eye class="w-4 h-4" /></button>
							<button onclick={() => openUpdateTAC(c)} title="Update" aria-label="Update target account config {c.accountId}" class="text-gray-400 hover:text-rose-500"><Pencil class="w-4 h-4" /></button>
							<button onclick={() => handleDeleteTAC(c)} title="Delete" aria-label="Delete target account config {c.accountId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const tacColumns = [
						{ key: 'accountId', label: 'Account ID' },
						{ key: 'roleArn', label: 'Role ARN' },
						{ key: 'description', label: 'Description' },
						{ key: 'actions', label: '', render: tacActionsCell }
					] as Column<TargetAccountConfigurationSummary>[]}
					<DataTable
						rows={filteredTAC}
						rowKey={(c) => c.accountId ?? ''}
						columns={tacColumns}
						loading={tabLoader.isLoading('targetAccountConfigs')}
						emptyMessage="No target account configurations found"
					/>
				{/if}
			{:else if activeTab === 'actions'}
				{#snippet catalogActionsCell(a: ActionSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openActionDetail(a)} title="View" aria-label="View action {a.id}" class="text-gray-400 hover:text-rose-500"><Eye class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const actionColumns = [
					{ key: 'id', label: 'Action ID' },
					{ key: 'description', label: 'Description' },
					{ key: 'actions', label: '', render: catalogActionsCell }
				] as Column<ActionSummary>[]}
				<DataTable
					rows={filteredActions}
					rowKey={(a) => a.id ?? ''}
					columns={actionColumns}
					loading={tabLoader.isLoading('actions')}
					emptyMessage="No actions found"
				/>
				<LoadMore hasMore={!!actionsNextToken} loading={loadingMoreActions} onLoadMore={loadMoreActions} />
			{:else if activeTab === 'targetResourceTypes'}
				{#snippet rtActionsCell(rt: TargetResourceTypeSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openResourceTypeDetail(rt)} title="View" aria-label="View resource type {rt.resourceType}" class="text-gray-400 hover:text-rose-500"><Eye class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const rtColumns = [
					{ key: 'resourceType', label: 'Resource Type' },
					{ key: 'description', label: 'Description' },
					{ key: 'actions', label: '', render: rtActionsCell }
				] as Column<TargetResourceTypeSummary>[]}
				<DataTable
					rows={filteredResourceTypes}
					rowKey={(rt) => rt.resourceType ?? ''}
					columns={rtColumns}
					loading={tabLoader.isLoading('targetResourceTypes')}
					emptyMessage="No target resource types found"
				/>
				<LoadMore hasMore={!!resourceTypesNextToken} loading={loadingMoreResourceTypes} onLoadMore={loadMoreResourceTypes} />
			{:else if activeTab === 'safetyLever'}
				{#if tabLoader.isLoading('safetyLever')}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
				{:else if safetyLever}
					<div class="max-w-lg space-y-4 p-6 rounded-lg border border-slate-200 dark:border-slate-700">
						<div class="flex items-center gap-3">
							<ShieldAlert class="w-6 h-6 {safetyLever.state?.status === 'engaged' ? 'text-red-500' : 'text-green-500'}" />
							<div>
								<div class="text-sm font-semibold text-gray-900 dark:text-white">Account Safety Lever</div>
								<div class="text-xs text-gray-500 dark:text-gray-400 font-mono">{safetyLever.id}</div>
							</div>
						</div>
						<div class="flex justify-between text-sm border-t border-gray-100 dark:border-gray-800 pt-3">
							<span class="text-gray-500">Status</span>
							<span class="font-mono {safetyLever.state?.status === 'engaged' ? 'text-red-600 dark:text-red-400' : 'text-green-600 dark:text-green-400'}">{safetyLever.state?.status}</span>
						</div>
						{#if safetyLever.state?.reason}
							<div class="flex justify-between text-sm">
								<span class="text-gray-500">Reason</span>
								<span class="font-mono text-right break-all">{safetyLever.state.reason}</span>
							</div>
						{/if}
						<div>
							<label for="safety-lever-reason" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Reason for next change (optional)</label>
							<input id="safety-lever-reason" bind:value={safetyLeverReason} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="Blocking experiments during incident response" />
						</div>
						<button
							onclick={toggleSafetyLever}
							disabled={updatingSafetyLever}
							class="w-full px-4 py-2 text-sm rounded-lg text-white disabled:opacity-50 {safetyLever.state?.status === 'engaged' ? 'bg-green-600 hover:bg-green-700' : 'bg-red-600 hover:bg-red-700'}"
						>
							{updatingSafetyLever ? 'Updating…' : safetyLever.state?.status === 'engaged' ? 'Disengage' : 'Engage'}
						</button>
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
