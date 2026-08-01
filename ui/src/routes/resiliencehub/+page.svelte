<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { urlState } from '$lib/url-state.svelte';
	import { getResiliencehubClient } from '$lib/aws-client';
	import {
		CreateAppCommand,
		DescribeAppCommand,
		UpdateAppCommand,
		DeleteAppCommand,
		ListAppsCommand,
		ListAppVersionsCommand,
		PublishAppVersionCommand,
		DescribeAppVersionTemplateCommand,
		PutDraftAppVersionTemplateCommand,
		CreateAppVersionAppComponentCommand,
		DescribeAppVersionAppComponentCommand,
		UpdateAppVersionAppComponentCommand,
		DeleteAppVersionAppComponentCommand,
		ListAppVersionAppComponentsCommand,
		CreateAppVersionResourceCommand,
		DescribeAppVersionResourceCommand,
		UpdateAppVersionResourceCommand,
		DeleteAppVersionResourceCommand,
		ListAppVersionResourcesCommand,
		ListUnsupportedAppVersionResourcesCommand,
		ResolveAppVersionResourcesCommand,
		DescribeAppVersionResourcesResolutionStatusCommand,
		AddDraftAppVersionResourceMappingsCommand,
		RemoveDraftAppVersionResourceMappingsCommand,
		ListAppVersionResourceMappingsCommand,
		ImportResourcesToDraftAppVersionCommand,
		DescribeDraftAppVersionResourcesImportStatusCommand,
		ListAppInputSourcesCommand,
		DeleteAppInputSourceCommand,
		CreateResiliencyPolicyCommand,
		DescribeResiliencyPolicyCommand,
		UpdateResiliencyPolicyCommand,
		DeleteResiliencyPolicyCommand,
		ListResiliencyPoliciesCommand,
		ListSuggestedResiliencyPoliciesCommand,
		StartAppAssessmentCommand,
		DescribeAppAssessmentCommand,
		DeleteAppAssessmentCommand,
		ListAppAssessmentsCommand,
		ListAppComponentCompliancesCommand,
		CreateRecommendationTemplateCommand,
		DeleteRecommendationTemplateCommand,
		ListRecommendationTemplatesCommand,
		ListTagsForResourceCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type AppSummary,
		type App,
		type AppVersionSummary,
		type AppComponent,
		type PhysicalResource,
		type ResourceMapping,
		type AppInputSource,
		type ResiliencyPolicy,
		type AppAssessmentSummary,
		type AppAssessment,
		type AppComponentCompliance,
		type RecommendationTemplate
	} from '@aws-sdk/client-resiliencehub';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import {
		ShieldCheck,
		Plus,
		Trash2,
		Eye,
		Pencil,
		PlayCircle,
		Link2,
		UploadCloud
	} from 'lucide-svelte';

	const client = regionalClient(getResiliencehubClient);

	// This service has only 7 modeled exception shapes shared across all 63
	// operations (AccessDeniedException, ConflictException,
	// InternalServerException, ResourceNotFoundException,
	// ServiceQuotaExceededException, ThrottlingException,
	// ValidationException -- see services/resiliencehub/PARITY.md), so the
	// generic name+message+status rendering below already shows exactly what
	// the API returns -- there is no richer per-op error detail to surface.
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

	// Every *AppVersion* mutation op in this service (components, resources,
	// mappings) operates on the draft version only -- publishing snapshots it
	// into an immutable numbered version. See PARITY.md's "AppVersion draft
	// sentinel" gap: the literal string "draft" is asserted from general
	// product knowledge, not confirmed against any SDK enum/pattern trait.
	const DRAFT_VERSION = 'draft';

	const DISRUPTION_TYPES = ['Software', 'Hardware', 'AZ', 'Region'] as const;
	type DisruptionTypeKey = (typeof DISRUPTION_TYPES)[number];

	function emptyFailurePolicyMap(): Record<DisruptionTypeKey, { rtoInSecs: number; rpoInSecs: number }> {
		return {
			Software: { rtoInSecs: 3600, rpoInSecs: 3600 },
			Hardware: { rtoInSecs: 3600, rpoInSecs: 3600 },
			AZ: { rtoInSecs: 3600, rpoInSecs: 3600 },
			Region: { rtoInSecs: 86400, rpoInSecs: 86400 }
		};
	}

	type TabId =
		| 'apps'
		| 'components'
		| 'resources'
		| 'mappings'
		| 'inputSources'
		| 'policies'
		| 'assessments'
		| 'templates';

	const tabs: TabDef[] = [
		{ id: 'apps', label: 'Apps' },
		{ id: 'components', label: 'App Components' },
		{ id: 'resources', label: 'Resources' },
		{ id: 'mappings', label: 'Resource Mappings' },
		{ id: 'inputSources', label: 'Input Sources' },
		{ id: 'policies', label: 'Resiliency Policies' },
		{ id: 'assessments', label: 'Assessments' },
		{ id: 'templates', label: 'Recommendation Templates' }
	];

	// URL-backed (?tab=...). Read via untrack() inside the onRegionChange
	// effect below (switchTab() also writes it): without untrack, every tab
	// switch would re-trigger the region effect and double-fetch.
	const pageTabParam = urlState<TabId>('tab', 'apps');
	let activeTab = $derived(pageTabParam.get());
	let searchQuery = $state('');

	// Components/Resources/Mappings/Input Sources/Assessments are all scoped
	// to one selected App -- the same shared-selector pattern grafana uses
	// for its workspace-scoped tabs. All draft-version mutations target
	// DRAFT_VERSION; assessments/policies are independent of app version.
	let selectedAppArn = $state('');
	const appScopedTabs: TabId[] = [
		'components',
		'resources',
		'mappings',
		'inputSources',
		'assessments'
	];

	let apps = $state<AppSummary[]>([]);
	let appsNextToken = $state<string | undefined>();
	let loadingMoreApps = $state(false);

	let components = $state<AppComponent[]>([]);
	let componentsNextToken = $state<string | undefined>();
	let loadingMoreComponents = $state(false);

	let resources = $state<PhysicalResource[]>([]);
	let resourcesNextToken = $state<string | undefined>();
	let loadingMoreResources = $state(false);
	let resourcesResolutionId = $state('');

	let mappings = $state<ResourceMapping[]>([]);
	let mappingsNextToken = $state<string | undefined>();
	let loadingMoreMappings = $state(false);

	let inputSources = $state<AppInputSource[]>([]);
	let inputSourcesNextToken = $state<string | undefined>();
	let loadingMoreInputSources = $state(false);

	let policies = $state<ResiliencyPolicy[]>([]);
	let policiesNextToken = $state<string | undefined>();
	let loadingMorePolicies = $state(false);

	let assessments = $state<AppAssessmentSummary[]>([]);
	let assessmentsNextToken = $state<string | undefined>();
	let loadingMoreAssessments = $state(false);

	let templatesAssessmentFilter = $state('');
	let templates = $state<RecommendationTemplate[]>([]);
	let templatesNextToken = $state<string | undefined>();
	let loadingMoreTemplates = $state(false);

	async function fetchApps(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListAppsCommand({ nextToken: reset ? undefined : appsNextToken })
		);
		apps = reset ? (resp.appSummaries ?? []) : [...apps, ...(resp.appSummaries ?? [])];
		appsNextToken = resp.nextToken;
		if (!selectedAppArn && apps.length > 0) {
			selectedAppArn = apps[0].appArn ?? '';
		}
	}

	async function fetchComponents(reset: boolean): Promise<void> {
		if (!selectedAppArn) {
			components = [];
			componentsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListAppVersionAppComponentsCommand({
				appArn: selectedAppArn,
				appVersion: DRAFT_VERSION,
				nextToken: reset ? undefined : componentsNextToken
			})
		);
		components = reset
			? (resp.appComponents ?? [])
			: [...components, ...(resp.appComponents ?? [])];
		componentsNextToken = resp.nextToken;
	}

	async function fetchResources(reset: boolean): Promise<void> {
		if (!selectedAppArn) {
			resources = [];
			resourcesNextToken = undefined;
			resourcesResolutionId = '';
			return;
		}
		const resp = await client().send(
			new ListAppVersionResourcesCommand({
				appArn: selectedAppArn,
				appVersion: DRAFT_VERSION,
				nextToken: reset ? undefined : resourcesNextToken
			})
		);
		resources = reset
			? (resp.physicalResources ?? [])
			: [...resources, ...(resp.physicalResources ?? [])];
		resourcesNextToken = resp.nextToken;
		resourcesResolutionId = resp.resolutionId ?? '';
	}

	async function fetchMappings(reset: boolean): Promise<void> {
		if (!selectedAppArn) {
			mappings = [];
			mappingsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListAppVersionResourceMappingsCommand({
				appArn: selectedAppArn,
				appVersion: DRAFT_VERSION,
				nextToken: reset ? undefined : mappingsNextToken
			})
		);
		mappings = reset
			? (resp.resourceMappings ?? [])
			: [...mappings, ...(resp.resourceMappings ?? [])];
		mappingsNextToken = resp.nextToken;
	}

	async function fetchInputSources(reset: boolean): Promise<void> {
		if (!selectedAppArn) {
			inputSources = [];
			inputSourcesNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListAppInputSourcesCommand({
				appArn: selectedAppArn,
				appVersion: DRAFT_VERSION,
				nextToken: reset ? undefined : inputSourcesNextToken
			})
		);
		inputSources = reset
			? (resp.appInputSources ?? [])
			: [...inputSources, ...(resp.appInputSources ?? [])];
		inputSourcesNextToken = resp.nextToken;
	}

	async function fetchPolicies(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListResiliencyPoliciesCommand({ nextToken: reset ? undefined : policiesNextToken })
		);
		policies = reset
			? (resp.resiliencyPolicies ?? [])
			: [...policies, ...(resp.resiliencyPolicies ?? [])];
		policiesNextToken = resp.nextToken;
	}

	async function fetchAssessments(reset: boolean): Promise<void> {
		if (!selectedAppArn) {
			assessments = [];
			assessmentsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListAppAssessmentsCommand({
				appArn: selectedAppArn,
				nextToken: reset ? undefined : assessmentsNextToken
			})
		);
		assessments = reset
			? (resp.assessmentSummaries ?? [])
			: [...assessments, ...(resp.assessmentSummaries ?? [])];
		assessmentsNextToken = resp.nextToken;
	}

	async function fetchTemplates(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListRecommendationTemplatesCommand({
				assessmentArn: templatesAssessmentFilter.trim() || undefined,
				nextToken: reset ? undefined : templatesNextToken
			})
		);
		templates = reset
			? (resp.recommendationTemplates ?? [])
			: [...templates, ...(resp.recommendationTemplates ?? [])];
		templatesNextToken = resp.nextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		apps: () => fetchApps(true).catch(rethrowDescribed),
		components: () => fetchComponents(true).catch(rethrowDescribed),
		resources: () => fetchResources(true).catch(rethrowDescribed),
		mappings: () => fetchMappings(true).catch(rethrowDescribed),
		inputSources: () => fetchInputSources(true).catch(rethrowDescribed),
		policies: () => fetchPolicies(true).catch(rethrowDescribed),
		assessments: () => fetchAssessments(true).catch(rethrowDescribed),
		templates: () => fetchTemplates(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		pageTabParam.set(id as TabId);
		searchQuery = '';
		tabLoader.load(id as TabId);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	function onAppSelect(arn: string): void {
		selectedAppArn = arn;
		if (appScopedTabs.includes(activeTab)) {
			tabLoader.refresh(activeTab);
		}
	}

	// Apps is the parent resource for the five app-scoped tabs: on a region
	// change the previously selected app ARN belongs to the old region and
	// must not be reused, so reload apps first (which re-selects an app for
	// the new region) before reloading whichever tab is active. `activeTab`
	// is read via untrack() because switchTab() also writes it (via
	// pageTabParam): without untrack, every tab switch would re-trigger this
	// region effect and double-fetch.
	onRegionChange(() => {
		selectedAppArn = '';
		apps = [];
		appsNextToken = undefined;
		void tabLoader.refresh('apps').then(() => {
			const tab = untrack(() => activeTab);
			if (tab !== 'apps') {
				tabLoader.refresh(tab);
			}
		});
	});

	const filteredApps = $derived(
		apps.filter((a) => {
			const q = searchQuery.toLowerCase();
			return (
				(a.name ?? '').toLowerCase().includes(q) ||
				(a.appArn ?? '').toLowerCase().includes(q) ||
				(a.status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredComponents = $derived(
		components.filter((c) => (c.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredResources = $derived(
		resources.filter(
			(r) =>
				(r.resourceName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(r.resourceType ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredMappings = $derived(
		mappings.filter((m) => (m.mappingType ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredInputSources = $derived(
		inputSources.filter((s) =>
			(s.sourceName ?? s.sourceArn ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredPolicies = $derived(
		policies.filter((p) => (p.policyName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredAssessments = $derived(
		assessments.filter((a) =>
			(a.assessmentName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredTemplates = $derived(
		templates.filter((t) => (t.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreApps(): Promise<void> {
		loadingMoreApps = true;
		try {
			await fetchApps(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreApps = false;
		}
	}
	async function loadMoreComponents(): Promise<void> {
		loadingMoreComponents = true;
		try {
			await fetchComponents(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreComponents = false;
		}
	}
	async function loadMoreResources(): Promise<void> {
		loadingMoreResources = true;
		try {
			await fetchResources(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreResources = false;
		}
	}
	async function loadMoreMappings(): Promise<void> {
		loadingMoreMappings = true;
		try {
			await fetchMappings(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreMappings = false;
		}
	}
	async function loadMoreInputSources(): Promise<void> {
		loadingMoreInputSources = true;
		try {
			await fetchInputSources(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreInputSources = false;
		}
	}
	async function loadMorePolicies(): Promise<void> {
		loadingMorePolicies = true;
		try {
			await fetchPolicies(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMorePolicies = false;
		}
	}
	async function loadMoreAssessments(): Promise<void> {
		loadingMoreAssessments = true;
		try {
			await fetchAssessments(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAssessments = false;
		}
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

	function complianceClass(status: string | undefined): string {
		if (status === 'PolicyMet') {
			return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		}
		if (status === 'PolicyBreached') {
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		}
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}
	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// --- Apps: create / delete / edit / detail ---

	let createAppModal = $state<Modal | null>(null);
	let creatingApp = $state(false);
	let createAppError = $state<string | null>(null);
	let newAppName = $state('');
	let newAppDescription = $state('');
	let newAppPolicyArn = $state('');
	let newAppAssessmentSchedule = $state<'Disabled' | 'Daily'>('Disabled');
	let newAppAwsApplicationArn = $state('');

	function openCreateAppModal(): void {
		createAppError = null;
		newAppName = '';
		newAppDescription = '';
		newAppPolicyArn = '';
		newAppAssessmentSchedule = 'Disabled';
		newAppAwsApplicationArn = '';
		createAppModal?.open();
	}

	async function submitCreateApp(): Promise<void> {
		if (!newAppName.trim()) {
			createAppError = 'Name is required.';
			return;
		}
		creatingApp = true;
		createAppError = null;
		try {
			await client().send(
				new CreateAppCommand({
					name: newAppName.trim(),
					description: newAppDescription.trim() || undefined,
					policyArn: newAppPolicyArn.trim() || undefined,
					assessmentSchedule: newAppAssessmentSchedule,
					awsApplicationArn: newAppAwsApplicationArn.trim() || undefined
				})
			);
			toast.success('Application created');
			createAppModal?.close();
			await tabLoader.refresh('apps');
		} catch (e) {
			const msg = describeError(e);
			createAppError = msg;
			toast.error(msg);
		} finally {
			creatingApp = false;
		}
	}

	async function handleDeleteApp(a: AppSummary): Promise<void> {
		if (!a.appArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete application',
			message: `Delete application "${a.name ?? a.appArn}"? This also deletes its versions, components, resources, mappings, assessments, and tags.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAppCommand({ appArn: a.appArn }));
			toast.success('Application deleted');
			if (selectedAppArn === a.appArn) selectedAppArn = '';
			await tabLoader.refresh('apps');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let editAppModal = $state<Modal | null>(null);
	let editingApp = $state(false);
	let editAppError = $state<string | null>(null);
	let editAppArn = $state('');
	let editAppDescription = $state('');
	let editAppPolicyArn = $state('');
	let editAppClearPolicy = $state(false);
	let editAppAssessmentSchedule = $state<'Disabled' | 'Daily'>('Disabled');

	// AppSummary (the list row shape) has no policyArn field -- only the full
	// App shape (DescribeApp) carries it -- so the edit modal re-describes
	// before prefilling, the same re-fetch-to-prefill pattern
	// accessanalyzer's edit-analyzer modal uses for its own list/describe gap.
	async function openEditAppModal(a: AppSummary): Promise<void> {
		if (!a.appArn) return;
		editAppError = null;
		editAppArn = a.appArn;
		editAppDescription = a.description ?? '';
		editAppPolicyArn = '';
		editAppClearPolicy = false;
		editAppAssessmentSchedule = (a.assessmentSchedule as 'Disabled' | 'Daily') ?? 'Disabled';
		editAppModal?.open();
		try {
			const resp = await client().send(new DescribeAppCommand({ appArn: a.appArn }));
			editAppPolicyArn = resp.app?.policyArn ?? '';
		} catch (e) {
			editAppError = describeError(e);
		}
	}

	async function submitEditApp(): Promise<void> {
		if (!editAppArn) return;
		editingApp = true;
		editAppError = null;
		try {
			await client().send(
				new UpdateAppCommand({
					appArn: editAppArn,
					description: editAppDescription.trim() || undefined,
					policyArn: editAppClearPolicy ? undefined : editAppPolicyArn.trim() || undefined,
					clearResiliencyPolicyArn: editAppClearPolicy || undefined,
					assessmentSchedule: editAppAssessmentSchedule
				})
			);
			toast.success('Application updated');
			editAppModal?.close();
			await tabLoader.refresh('apps');
		} catch (e) {
			const msg = describeError(e);
			editAppError = msg;
			toast.error(msg);
		} finally {
			editingApp = false;
		}
	}

	// App detail: base description (ResiliencyScore/ComplianceStatus honestly
	// annotated -- see PARITY.md) plus the non-listable sub-resources this
	// service scopes to a single app: versions/publish, the draft template
	// body, and tags.
	let appDetailModal = $state<Modal | null>(null);
	let viewedApp = $state<App | null>(null);
	let appDetailLoading = $state(false);
	let appDetailError = $state<string | null>(null);
	let appDetailArn = $state('');
	let appTags = $state<Record<string, string>>({});
	let appVersions = $state<AppVersionSummary[]>([]);

	async function openAppDetail(a: AppSummary): Promise<void> {
		if (!a.appArn) return;
		appDetailArn = a.appArn;
		viewedApp = null;
		appDetailError = null;
		appTags = {};
		appVersions = [];
		appDetailModal?.open();
		appDetailLoading = true;
		try {
			const resp = await client().send(new DescribeAppCommand({ appArn: a.appArn }));
			viewedApp = resp.app ?? null;
			await Promise.all([refreshAppTags(), refreshAppVersions()]);
		} catch (e) {
			appDetailError = describeError(e);
		} finally {
			appDetailLoading = false;
		}
	}

	async function refreshAppTags(): Promise<void> {
		if (!appDetailArn) return;
		try {
			const resp = await client().send(new ListTagsForResourceCommand({ resourceArn: appDetailArn }));
			appTags = resp.tags ?? {};
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function refreshAppVersions(): Promise<void> {
		if (!appDetailArn) return;
		try {
			const resp = await client().send(new ListAppVersionsCommand({ appArn: appDetailArn }));
			appVersions = resp.appVersions ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let addAppTagKey = $state('');
	let addAppTagValue = $state('');

	async function submitAddAppTag(): Promise<void> {
		if (!appDetailArn || !addAppTagKey.trim()) return;
		try {
			await client().send(
				new TagResourceCommand({
					resourceArn: appDetailArn,
					tags: { [addAppTagKey.trim()]: addAppTagValue }
				})
			);
			toast.success('Tag added');
			addAppTagKey = '';
			addAppTagValue = '';
			await refreshAppTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removeAppTag(key: string): Promise<void> {
		if (!appDetailArn) return;
		try {
			await client().send(new UntagResourceCommand({ resourceArn: appDetailArn, tagKeys: [key] }));
			toast.success('Tag removed');
			await refreshAppTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let newVersionName = $state('');
	let publishingVersion = $state(false);

	async function submitPublishVersion(): Promise<void> {
		if (!appDetailArn) return;
		publishingVersion = true;
		try {
			await client().send(
				new PublishAppVersionCommand({
					appArn: appDetailArn,
					versionName: newVersionName.trim() || undefined
				})
			);
			toast.success('Version published');
			newVersionName = '';
			await refreshAppVersions();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			publishingVersion = false;
		}
	}

	let templateModal = $state<Modal | null>(null);
	let templateBody = $state('{}');
	let templateLoading = $state(false);
	let templateSaving = $state(false);
	let templateError = $state<string | null>(null);

	async function openTemplateModal(): Promise<void> {
		if (!appDetailArn) return;
		templateError = null;
		templateModal?.open();
		templateLoading = true;
		try {
			const resp = await client().send(
				new DescribeAppVersionTemplateCommand({ appArn: appDetailArn, appVersion: DRAFT_VERSION })
			);
			templateBody = resp.appTemplateBody ?? '{}';
		} catch (e) {
			templateError = describeError(e);
		} finally {
			templateLoading = false;
		}
	}

	async function submitTemplate(): Promise<void> {
		if (!appDetailArn) return;
		templateSaving = true;
		templateError = null;
		try {
			await client().send(
				new PutDraftAppVersionTemplateCommand({
					appArn: appDetailArn,
					appTemplateBody: templateBody
				})
			);
			toast.success('Draft template saved');
			templateModal?.close();
		} catch (e) {
			const msg = describeError(e);
			templateError = msg;
			toast.error(msg);
		} finally {
			templateSaving = false;
		}
	}

	// --- App Components: create / edit / delete / detail (draft-only) ---

	let createComponentModal = $state<Modal | null>(null);
	let creatingComponent = $state(false);
	let createComponentError = $state<string | null>(null);
	let newComponentName = $state('');
	let newComponentType = $state('');

	function openCreateComponentModal(): void {
		createComponentError = selectedAppArn ? null : 'Select an app first.';
		newComponentName = '';
		newComponentType = '';
		createComponentModal?.open();
	}

	async function submitCreateComponent(): Promise<void> {
		if (!selectedAppArn) {
			createComponentError = 'Select an app first.';
			return;
		}
		if (!newComponentName.trim() || !newComponentType.trim()) {
			createComponentError = 'Name and type are required.';
			return;
		}
		creatingComponent = true;
		createComponentError = null;
		try {
			await client().send(
				new CreateAppVersionAppComponentCommand({
					appArn: selectedAppArn,
					name: newComponentName.trim(),
					type: newComponentType.trim()
				})
			);
			toast.success('Component created');
			createComponentModal?.close();
			await tabLoader.refresh('components');
		} catch (e) {
			const msg = describeError(e);
			createComponentError = msg;
			toast.error(msg);
		} finally {
			creatingComponent = false;
		}
	}

	let componentDetailModal = $state<Modal | null>(null);
	let viewedComponent = $state<AppComponent | null>(null);
	let componentDetailLoading = $state(false);
	let componentDetailError = $state<string | null>(null);

	async function openComponentDetail(c: AppComponent): Promise<void> {
		if (!c.id || !selectedAppArn) return;
		viewedComponent = null;
		componentDetailError = null;
		componentDetailModal?.open();
		componentDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeAppVersionAppComponentCommand({
					appArn: selectedAppArn,
					appVersion: DRAFT_VERSION,
					id: c.id
				})
			);
			viewedComponent = resp.appComponent ?? null;
		} catch (e) {
			componentDetailError = describeError(e);
		} finally {
			componentDetailLoading = false;
		}
	}

	let editComponentModal = $state<Modal | null>(null);
	let editingComponent = $state(false);
	let editComponentError = $state<string | null>(null);
	let editComponentId = $state('');
	let editComponentName = $state('');
	let editComponentType = $state('');

	function openEditComponentModal(c: AppComponent): void {
		if (!c.id) return;
		editComponentError = null;
		editComponentId = c.id;
		editComponentName = c.name ?? '';
		editComponentType = c.type ?? '';
		editComponentModal?.open();
	}

	async function submitEditComponent(): Promise<void> {
		if (!selectedAppArn || !editComponentId) return;
		editingComponent = true;
		editComponentError = null;
		try {
			await client().send(
				new UpdateAppVersionAppComponentCommand({
					appArn: selectedAppArn,
					id: editComponentId,
					name: editComponentName.trim() || undefined,
					type: editComponentType.trim() || undefined
				})
			);
			toast.success('Component updated');
			editComponentModal?.close();
			await tabLoader.refresh('components');
		} catch (e) {
			const msg = describeError(e);
			editComponentError = msg;
			toast.error(msg);
		} finally {
			editingComponent = false;
		}
	}

	async function handleDeleteComponent(c: AppComponent): Promise<void> {
		if (!c.id || !selectedAppArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete component',
			message: `Delete component "${c.name}"? This fails if any resource is still assigned to it.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteAppVersionAppComponentCommand({ appArn: selectedAppArn, id: c.id })
			);
			toast.success('Component deleted');
			await tabLoader.refresh('components');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Resources: create / edit / delete / detail / resolve (draft-only) ---

	let createResourceModal = $state<Modal | null>(null);
	let creatingResource = $state(false);
	let createResourceError = $state<string | null>(null);
	let newResourceName = $state('');
	let newResourcePhysicalId = $state('');
	let newResourceType = $state('');
	let newResourceComponents = $state('');
	let newResourceAwsAccountId = $state('');
	let newResourceAwsRegion = $state('');

	function openCreateResourceModal(): void {
		createResourceError = selectedAppArn ? null : 'Select an app first.';
		newResourceName = '';
		newResourcePhysicalId = '';
		newResourceType = '';
		newResourceComponents = '';
		newResourceAwsAccountId = '';
		newResourceAwsRegion = '';
		createResourceModal?.open();
	}

	async function submitCreateResource(): Promise<void> {
		if (!selectedAppArn) {
			createResourceError = 'Select an app first.';
			return;
		}
		if (!newResourcePhysicalId.trim() || !newResourceType.trim()) {
			createResourceError = 'Physical resource ID and resource type are required.';
			return;
		}
		creatingResource = true;
		createResourceError = null;
		try {
			await client().send(
				new CreateAppVersionResourceCommand({
					appArn: selectedAppArn,
					resourceName: newResourceName.trim() || undefined,
					logicalResourceId: { identifier: newResourceName.trim() || newResourcePhysicalId.trim() },
					physicalResourceId: newResourcePhysicalId.trim(),
					resourceType: newResourceType.trim(),
					awsAccountId: newResourceAwsAccountId.trim() || undefined,
					awsRegion: newResourceAwsRegion.trim() || undefined,
					appComponents: newResourceComponents
						.split(',')
						.map((s) => s.trim())
						.filter(Boolean)
				})
			);
			toast.success('Resource created');
			createResourceModal?.close();
			await tabLoader.refresh('resources');
		} catch (e) {
			const msg = describeError(e);
			createResourceError = msg;
			toast.error(msg);
		} finally {
			creatingResource = false;
		}
	}

	let resourceDetailModal = $state<Modal | null>(null);
	let viewedResource = $state<PhysicalResource | null>(null);
	let resourceDetailLoading = $state(false);
	let resourceDetailError = $state<string | null>(null);

	async function openResourceDetail(r: PhysicalResource): Promise<void> {
		if (!r.resourceName || !selectedAppArn) return;
		viewedResource = null;
		resourceDetailError = null;
		resourceDetailModal?.open();
		resourceDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeAppVersionResourceCommand({
					appArn: selectedAppArn,
					appVersion: DRAFT_VERSION,
					resourceName: r.resourceName
				})
			);
			viewedResource = resp.physicalResource ?? null;
		} catch (e) {
			resourceDetailError = describeError(e);
		} finally {
			resourceDetailLoading = false;
		}
	}

	let editResourceModal = $state<Modal | null>(null);
	let editingResource = $state(false);
	let editResourceError = $state<string | null>(null);
	let editResourceName = $state('');
	let editResourceType = $state('');
	let editResourcePhysicalId = $state('');
	let editResourceExcluded = $state(false);

	function openEditResourceModal(r: PhysicalResource): void {
		if (!r.resourceName) return;
		editResourceError = null;
		editResourceName = r.resourceName;
		editResourceType = r.resourceType ?? '';
		editResourcePhysicalId = r.physicalResourceId?.identifier ?? '';
		editResourceExcluded = r.excluded ?? false;
		editResourceModal?.open();
	}

	async function submitEditResource(): Promise<void> {
		if (!selectedAppArn || !editResourceName) return;
		editingResource = true;
		editResourceError = null;
		try {
			await client().send(
				new UpdateAppVersionResourceCommand({
					appArn: selectedAppArn,
					resourceName: editResourceName,
					resourceType: editResourceType.trim() || undefined,
					physicalResourceId: editResourcePhysicalId.trim() || undefined,
					excluded: editResourceExcluded
				})
			);
			toast.success('Resource updated');
			editResourceModal?.close();
			await tabLoader.refresh('resources');
		} catch (e) {
			const msg = describeError(e);
			editResourceError = msg;
			toast.error(msg);
		} finally {
			editingResource = false;
		}
	}

	async function handleDeleteResource(r: PhysicalResource): Promise<void> {
		if (!r.resourceName || !selectedAppArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete resource',
			message: `Delete resource "${r.resourceName}"? Only manually-added resources can be deleted.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteAppVersionResourceCommand({ appArn: selectedAppArn, resourceName: r.resourceName })
			);
			toast.success('Resource deleted');
			await tabLoader.refresh('resources');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let resolving = $state(false);
	let resolutionStatus = $state<string | null>(null);

	// Resolving only ever materializes the "Resource" mapping type for real
	// -- CfnStack/ResourceGroup/EKS/AppRegistryApp/Terraform mappings are
	// accepted but left unresolved. See PARITY.md's "Resolve
	// AppVersionResources" gap: this is a documented, narrower scope than
	// real cross-service resolution, not a silent stub.
	async function handleResolveResources(): Promise<void> {
		if (!selectedAppArn) return;
		resolving = true;
		resolutionStatus = null;
		try {
			const resp = await client().send(
				new ResolveAppVersionResourcesCommand({ appArn: selectedAppArn, appVersion: DRAFT_VERSION })
			);
			resolutionStatus = resp.status ?? null;
			const statusResp = await client().send(
				new DescribeAppVersionResourcesResolutionStatusCommand({
					appArn: selectedAppArn,
					appVersion: DRAFT_VERSION,
					resolutionId: resp.resolutionId
				})
			);
			resolutionStatus = statusResp.status ?? resolutionStatus;
			toast.success(`Resolution ${resolutionStatus ?? 'started'}`);
			await tabLoader.refresh('resources');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			resolving = false;
		}
	}

	let unsupportedResources = $state<PhysicalResource[] | null>(null);
	let checkingUnsupported = $state(false);

	async function handleCheckUnsupported(): Promise<void> {
		if (!selectedAppArn) return;
		checkingUnsupported = true;
		try {
			const resp = await client().send(
				new ListUnsupportedAppVersionResourcesCommand({
					appArn: selectedAppArn,
					appVersion: DRAFT_VERSION
				})
			);
			unsupportedResources = resp.unsupportedResources ?? [];
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			checkingUnsupported = false;
		}
	}

	// --- Resource Mappings: add / remove (draft-only, no describe op) ---

	let createMappingModal = $state<Modal | null>(null);
	let creatingMapping = $state(false);
	let createMappingError = $state<string | null>(null);
	let newMappingType = $state<
		'Resource' | 'CfnStack' | 'ResourceGroup' | 'Terraform' | 'EKS' | 'AppRegistryApp'
	>('Resource');
	let newMappingName = $state('');
	let newMappingPhysicalId = $state('');
	let newMappingPhysicalIdType = $state<'Arn' | 'Native'>('Native');

	function openCreateMappingModal(): void {
		createMappingError = selectedAppArn ? null : 'Select an app first.';
		newMappingType = 'Resource';
		newMappingName = '';
		newMappingPhysicalId = '';
		newMappingPhysicalIdType = 'Native';
		createMappingModal?.open();
	}

	function mappingNameField(
		type: typeof newMappingType
	): 'resourceName' | 'logicalStackName' | 'resourceGroupName' | 'terraformSourceName' | 'eksSourceName' | 'appRegistryAppName' {
		switch (type) {
			case 'CfnStack':
				return 'logicalStackName';
			case 'ResourceGroup':
				return 'resourceGroupName';
			case 'Terraform':
				return 'terraformSourceName';
			case 'EKS':
				return 'eksSourceName';
			case 'AppRegistryApp':
				return 'appRegistryAppName';
			default:
				return 'resourceName';
		}
	}

	async function submitCreateMapping(): Promise<void> {
		if (!selectedAppArn) {
			createMappingError = 'Select an app first.';
			return;
		}
		if (!newMappingName.trim() || !newMappingPhysicalId.trim()) {
			createMappingError = 'Name and physical resource ID are required.';
			return;
		}
		creatingMapping = true;
		createMappingError = null;
		try {
			await client().send(
				new AddDraftAppVersionResourceMappingsCommand({
					appArn: selectedAppArn,
					resourceMappings: [
						{
							mappingType: newMappingType,
							physicalResourceId: {
								identifier: newMappingPhysicalId.trim(),
								type: newMappingPhysicalIdType
							},
							[mappingNameField(newMappingType)]: newMappingName.trim()
						}
					]
				})
			);
			toast.success('Mapping added');
			createMappingModal?.close();
			await tabLoader.refresh('mappings');
		} catch (e) {
			const msg = describeError(e);
			createMappingError = msg;
			toast.error(msg);
		} finally {
			creatingMapping = false;
		}
	}

	function mappingDisplayName(m: ResourceMapping): string {
		return (
			m.resourceName ??
			m.logicalStackName ??
			m.resourceGroupName ??
			m.terraformSourceName ??
			m.eksSourceName ??
			m.appRegistryAppName ??
			''
		);
	}

	async function handleRemoveMapping(m: ResourceMapping): Promise<void> {
		if (!selectedAppArn) return;
		const name = mappingDisplayName(m);
		const confirmed = await confirmDestructive({
			title: 'Remove mapping',
			message: `Remove mapping "${name}" (${m.mappingType})?`
		});
		if (!confirmed) return;
		const field = mappingNameField(
			(m.mappingType as typeof newMappingType) ?? 'Resource'
		);
		const requestKey =
			field === 'resourceName'
				? 'resourceNames'
				: field === 'logicalStackName'
					? 'logicalStackNames'
					: field === 'resourceGroupName'
						? 'resourceGroupNames'
						: field === 'terraformSourceName'
							? 'terraformSourceNames'
							: field === 'eksSourceName'
								? 'eksSourceNames'
								: 'appRegistryAppNames';
		try {
			await client().send(
				new RemoveDraftAppVersionResourceMappingsCommand({
					appArn: selectedAppArn,
					[requestKey]: [name]
				})
			);
			toast.success('Mapping removed');
			await tabLoader.refresh('mappings');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Input Sources: import / delete (draft-only, no describe op) ---

	let importModal = $state<Modal | null>(null);
	let importing = $state(false);
	let importError = $state<string | null>(null);
	let importSourceArns = $state('');

	function openImportModal(): void {
		importError = selectedAppArn ? null : 'Select an app first.';
		importSourceArns = '';
		importModal?.open();
	}

	async function submitImport(): Promise<void> {
		if (!selectedAppArn) {
			importError = 'Select an app first.';
			return;
		}
		const arns = importSourceArns
			.split(/[\n,]/)
			.map((s) => s.trim())
			.filter(Boolean);
		if (arns.length === 0) {
			importError = 'At least one source ARN is required.';
			return;
		}
		importing = true;
		importError = null;
		try {
			await client().send(
				new ImportResourcesToDraftAppVersionCommand({ appArn: selectedAppArn, sourceArns: arns })
			);
			toast.success('Import started');
			importModal?.close();
			await tabLoader.refresh('inputSources');
			const statusResp = await client().send(
				new DescribeDraftAppVersionResourcesImportStatusCommand({ appArn: selectedAppArn })
			);
			toast.success(`Import status: ${statusResp.status}`);
		} catch (e) {
			const msg = describeError(e);
			importError = msg;
			toast.error(msg);
		} finally {
			importing = false;
		}
	}

	async function handleDeleteInputSource(s: AppInputSource): Promise<void> {
		if (!selectedAppArn || !s.sourceArn) return;
		const confirmed = await confirmDestructive({
			title: 'Remove input source',
			message: `Remove input source "${s.sourceName ?? s.sourceArn}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteAppInputSourceCommand({ appArn: selectedAppArn, sourceArn: s.sourceArn })
			);
			toast.success('Input source removed');
			await tabLoader.refresh('inputSources');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Resiliency Policies: create / edit / delete / detail ---

	let createPolicyModal = $state<Modal | null>(null);
	let creatingPolicy = $state(false);
	let createPolicyError = $state<string | null>(null);
	let newPolicyName = $state('');
	let newPolicyDescription = $state('');
	let newPolicyTier = $state<
		'MissionCritical' | 'Critical' | 'Important' | 'CoreServices' | 'NonCritical' | 'NotApplicable'
	>('Important');
	let newPolicyFailurePolicy = $state(emptyFailurePolicyMap());

	function openCreatePolicyModal(): void {
		createPolicyError = null;
		newPolicyName = '';
		newPolicyDescription = '';
		newPolicyTier = 'Important';
		newPolicyFailurePolicy = emptyFailurePolicyMap();
		createPolicyModal?.open();
	}

	async function submitCreatePolicy(): Promise<void> {
		if (!newPolicyName.trim()) {
			createPolicyError = 'Name is required.';
			return;
		}
		creatingPolicy = true;
		createPolicyError = null;
		try {
			await client().send(
				new CreateResiliencyPolicyCommand({
					policyName: newPolicyName.trim(),
					policyDescription: newPolicyDescription.trim() || undefined,
					tier: newPolicyTier,
					policy: newPolicyFailurePolicy
				})
			);
			toast.success('Policy created');
			createPolicyModal?.close();
			await tabLoader.refresh('policies');
		} catch (e) {
			const msg = describeError(e);
			createPolicyError = msg;
			toast.error(msg);
		} finally {
			creatingPolicy = false;
		}
	}

	let policyDetailModal = $state<Modal | null>(null);
	let viewedPolicy = $state<ResiliencyPolicy | null>(null);
	let policyDetailLoading = $state(false);
	let policyDetailError = $state<string | null>(null);
	let policyTags = $state<Record<string, string>>({});
	let policyDetailArn = $state('');

	async function openPolicyDetail(p: ResiliencyPolicy): Promise<void> {
		if (!p.policyArn) return;
		policyDetailArn = p.policyArn;
		viewedPolicy = null;
		policyDetailError = null;
		policyTags = {};
		policyDetailModal?.open();
		policyDetailLoading = true;
		try {
			const resp = await client().send(new DescribeResiliencyPolicyCommand({ policyArn: p.policyArn }));
			viewedPolicy = resp.policy ?? null;
			const tagsResp = await client().send(
				new ListTagsForResourceCommand({ resourceArn: p.policyArn })
			);
			policyTags = tagsResp.tags ?? {};
		} catch (e) {
			policyDetailError = describeError(e);
		} finally {
			policyDetailLoading = false;
		}
	}

	let editPolicyModal = $state<Modal | null>(null);
	let editingPolicy = $state(false);
	let editPolicyError = $state<string | null>(null);
	let editPolicyArn = $state('');
	let editPolicyDescription = $state('');
	let editPolicyTier = $state<
		'MissionCritical' | 'Critical' | 'Important' | 'CoreServices' | 'NonCritical' | 'NotApplicable'
	>('Important');
	let editPolicyFailurePolicy = $state(emptyFailurePolicyMap());

	function openEditPolicyModal(p: ResiliencyPolicy): void {
		if (!p.policyArn) return;
		editPolicyError = null;
		editPolicyArn = p.policyArn;
		editPolicyDescription = p.policyDescription ?? '';
		editPolicyTier = (p.tier as typeof editPolicyTier) ?? 'Important';
		const merged = emptyFailurePolicyMap();
		for (const dt of DISRUPTION_TYPES) {
			const fp = p.policy?.[dt];
			if (fp) merged[dt] = { rtoInSecs: fp.rtoInSecs ?? 0, rpoInSecs: fp.rpoInSecs ?? 0 };
		}
		editPolicyFailurePolicy = merged;
		editPolicyModal?.open();
	}

	async function submitEditPolicy(): Promise<void> {
		if (!editPolicyArn) return;
		editingPolicy = true;
		editPolicyError = null;
		try {
			await client().send(
				new UpdateResiliencyPolicyCommand({
					policyArn: editPolicyArn,
					policyDescription: editPolicyDescription.trim() || undefined,
					tier: editPolicyTier,
					policy: editPolicyFailurePolicy
				})
			);
			toast.success('Policy updated');
			editPolicyModal?.close();
			await tabLoader.refresh('policies');
		} catch (e) {
			const msg = describeError(e);
			editPolicyError = msg;
			toast.error(msg);
		} finally {
			editingPolicy = false;
		}
	}

	async function handleDeletePolicy(p: ResiliencyPolicy): Promise<void> {
		if (!p.policyArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete policy',
			message: `Delete policy "${p.policyName}"? This fails while any app is still bound to it.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteResiliencyPolicyCommand({ policyArn: p.policyArn }));
			toast.success('Policy deleted');
			await tabLoader.refresh('policies');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let suggestedPolicies = $state<ResiliencyPolicy[] | null>(null);
	let loadingSuggested = $state(false);

	// The tier RTO/RPO table returned here is this emulator's OWN invented
	// 5-tier stand-in progression, NOT AWS's published defaults -- see
	// PARITY.md: "a coarse, self-invented halving progression ... documented
	// stand-in", mirroring services/grafana's ListVersions precedent.
	async function toggleSuggestedPolicies(): Promise<void> {
		if (suggestedPolicies !== null) {
			suggestedPolicies = null;
			return;
		}
		loadingSuggested = true;
		try {
			const resp = await client().send(new ListSuggestedResiliencyPoliciesCommand({}));
			suggestedPolicies = resp.resiliencyPolicies ?? [];
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingSuggested = false;
		}
	}

	// --- Assessments: start / delete / detail (scoped by App) ---

	let createAssessmentModal = $state<Modal | null>(null);
	let creatingAssessment = $state(false);
	let createAssessmentError = $state<string | null>(null);
	let newAssessmentName = $state('');

	function openCreateAssessmentModal(): void {
		createAssessmentError = selectedAppArn ? null : 'Select an app first.';
		newAssessmentName = '';
		createAssessmentModal?.open();
	}

	async function submitCreateAssessment(): Promise<void> {
		if (!selectedAppArn) {
			createAssessmentError = 'Select an app first.';
			return;
		}
		if (!newAssessmentName.trim()) {
			createAssessmentError = 'Name is required.';
			return;
		}
		creatingAssessment = true;
		createAssessmentError = null;
		try {
			await client().send(
				new StartAppAssessmentCommand({
					appArn: selectedAppArn,
					appVersion: DRAFT_VERSION,
					assessmentName: newAssessmentName.trim()
				})
			);
			toast.success('Assessment started');
			createAssessmentModal?.close();
			await tabLoader.refresh('assessments');
		} catch (e) {
			const msg = describeError(e);
			createAssessmentError = msg;
			toast.error(msg);
		} finally {
			creatingAssessment = false;
		}
	}

	let assessmentDetailModal = $state<Modal | null>(null);
	let viewedAssessment = $state<AppAssessment | null>(null);
	let assessmentDetailLoading = $state(false);
	let assessmentDetailError = $state<string | null>(null);
	let componentCompliances = $state<AppComponentCompliance[]>([]);

	async function openAssessmentDetail(a: AppAssessmentSummary): Promise<void> {
		if (!a.assessmentArn) return;
		viewedAssessment = null;
		assessmentDetailError = null;
		componentCompliances = [];
		assessmentDetailModal?.open();
		assessmentDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeAppAssessmentCommand({ assessmentArn: a.assessmentArn })
			);
			viewedAssessment = resp.assessment ?? null;
			const compResp = await client().send(
				new ListAppComponentCompliancesCommand({ assessmentArn: a.assessmentArn })
			);
			componentCompliances = compResp.componentCompliances ?? [];
		} catch (e) {
			assessmentDetailError = describeError(e);
		} finally {
			assessmentDetailLoading = false;
		}
	}

	async function handleDeleteAssessment(a: AppAssessmentSummary): Promise<void> {
		if (!a.assessmentArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete assessment',
			message: `Delete assessment "${a.assessmentName ?? a.assessmentArn}"? This fails while it is still running.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAppAssessmentCommand({ assessmentArn: a.assessmentArn }));
			toast.success('Assessment deleted');
			await tabLoader.refresh('assessments');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Recommendation Templates: create / delete (no describe op) ---

	let createTemplateModal = $state<Modal | null>(null);
	let creatingTemplate = $state(false);
	let createTemplateError = $state<string | null>(null);
	let newTemplateAssessmentArn = $state('');
	let newTemplateName = $state('');
	let newTemplateFormat = $state<'CfnJson' | 'CfnYaml'>('CfnJson');

	function openCreateTemplateModal(): void {
		createTemplateError = null;
		newTemplateAssessmentArn = templatesAssessmentFilter.trim();
		newTemplateName = '';
		newTemplateFormat = 'CfnJson';
		createTemplateModal?.open();
	}

	async function submitCreateTemplate(): Promise<void> {
		if (!newTemplateAssessmentArn.trim() || !newTemplateName.trim()) {
			createTemplateError = 'Assessment ARN and name are required.';
			return;
		}
		creatingTemplate = true;
		createTemplateError = null;
		try {
			await client().send(
				new CreateRecommendationTemplateCommand({
					assessmentArn: newTemplateAssessmentArn.trim(),
					name: newTemplateName.trim(),
					format: newTemplateFormat
				})
			);
			toast.success('Recommendation template created');
			createTemplateModal?.close();
			await tabLoader.refresh('templates');
		} catch (e) {
			const msg = describeError(e);
			createTemplateError = msg;
			toast.error(msg);
		} finally {
			creatingTemplate = false;
		}
	}

	async function handleDeleteTemplate(t: RecommendationTemplate): Promise<void> {
		if (!t.recommendationTemplateArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete template',
			message: `Delete recommendation template "${t.name}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteRecommendationTemplateCommand({
					recommendationTemplateArn: t.recommendationTemplateArn
				})
			);
			toast.success('Template deleted');
			await tabLoader.refresh('templates');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	const SCORE_NOTE = 'Not computed by this emulator (no derivable formula) — always 0.';
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={ShieldCheck}
		title="AWS Resilience Hub"
		description="Assess and improve the resiliency of your applications"
		onRefresh={handleRefresh}
		color="teal"
		service="resiliencehub"
	>
		{#snippet actions()}
			{#if activeTab === 'apps'}
				<button
					onclick={openCreateAppModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create app
				</button>
			{:else if activeTab === 'components'}
				<button
					onclick={openCreateComponentModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create component
				</button>
			{:else if activeTab === 'resources'}
				<button
					onclick={openCreateResourceModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create resource
				</button>
			{:else if activeTab === 'mappings'}
				<button
					onclick={openCreateMappingModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Add mapping
				</button>
			{:else if activeTab === 'inputSources'}
				<button
					onclick={openImportModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<UploadCloud class="w-4 h-4" /> Import resources
				</button>
			{:else if activeTab === 'policies'}
				<button
					onclick={openCreatePolicyModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create policy
				</button>
			{:else if activeTab === 'assessments'}
				<button
					onclick={openCreateAssessmentModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<PlayCircle class="w-4 h-4" /> Start assessment
				</button>
			{:else if activeTab === 'templates'}
				<button
					onclick={openCreateTemplateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create template
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="teal" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if appScopedTabs.includes(activeTab)}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="app-select" class="text-sm text-gray-500 dark:text-gray-400">App</label>
					<select
						id="app-select"
						value={selectedAppArn}
						onchange={(e) => onAppSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if apps.length === 0}
							<option value="">No apps</option>
						{/if}
						{#each apps as a (a.appArn)}
							<option value={a.appArn}>{a.name} ({a.status})</option>
						{/each}
					</select>
					{#if activeTab === 'resources' && selectedAppArn}
						<button
							onclick={handleResolveResources}
							disabled={resolving}
							class="flex items-center gap-2 px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-600 dark:text-gray-300 disabled:opacity-50"
						>
							<Link2 class="w-4 h-4" /> {resolving ? 'Resolving...' : 'Resolve resources'}
						</button>
						<button
							onclick={handleCheckUnsupported}
							disabled={checkingUnsupported}
							class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-600 dark:text-gray-300 disabled:opacity-50"
						>
							{checkingUnsupported ? 'Checking...' : 'Check unsupported resources'}
						</button>
					{/if}
				</div>
				{#if activeTab === 'resources'}
					{#if resourcesResolutionId}
						<p class="text-xs text-gray-500 dark:text-gray-400">
							Latest resolution ID: {resourcesResolutionId}
							{resolutionStatus ? ` — status: ${resolutionStatus}` : ''}
						</p>
					{/if}
					{#if unsupportedResources !== null}
						<div
							class="rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-4 py-3 text-sm text-amber-800 dark:text-amber-300"
						>
							{#if unsupportedResources.length === 0}
								No unsupported resources on this app's draft version.
							{:else}
								{unsupportedResources.length} unsupported resource(s): {unsupportedResources
									.map((r) => r.resourceName ?? r.resourceType)
									.join(', ')}
							{/if}
						</div>
					{/if}
				{/if}
			{/if}

			{#if activeTab === 'templates'}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="templates-assessment-filter" class="text-sm text-gray-500 dark:text-gray-400"
						>Filter by assessment ARN (optional)</label
					>
					<input
						id="templates-assessment-filter"
						bind:value={templatesAssessmentFilter}
						placeholder="arn:aws:resiliencehub:...:app-assessment/..."
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white flex-1 min-w-[16rem]"
					/>
					<button
						onclick={() => tabLoader.refresh('templates')}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-600 dark:text-gray-300"
						>Load</button
					>
				</div>
			{/if}

			{#if activeTab === 'policies'}
				<div>
					<button
						onclick={toggleSuggestedPolicies}
						disabled={loadingSuggested}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-600 dark:text-gray-300 disabled:opacity-50"
					>
						{suggestedPolicies !== null ? 'Hide' : 'View'} suggested policy tiers
					</button>
					{#if suggestedPolicies !== null}
						<div
							class="mt-2 rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-4 py-3 text-sm text-amber-800 dark:text-amber-300 space-y-2"
						>
							<p>
								This table is this emulator's own invented 5-tier stand-in progression, NOT
								AWS's published tier defaults. See PARITY.md.
							</p>
							<ul class="list-disc list-inside">
								{#each suggestedPolicies as sp, i (i)}
									<li>
										{sp.tier}: {DISRUPTION_TYPES.map(
											(dt) => `${dt} RTO ${sp.policy?.[dt]?.rtoInSecs ?? '—'}s / RPO ${sp.policy?.[dt]?.rpoInSecs ?? '—'}s`
										).join(', ')}
									</li>
								{/each}
							</ul>
						</div>
					{/if}
				</div>
			{/if}

			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'apps'}
				{#snippet appComplianceCell(a: AppSummary)}
					<span class="text-xs px-2 py-1 rounded-full {complianceClass(a.complianceStatus)}"
						>{a.complianceStatus ?? '—'}</span
					>
				{/snippet}
				{#snippet appScoreCell(a: AppSummary)}
					<span title={SCORE_NOTE} class="italic text-gray-400 dark:text-gray-500"
						>{a.resiliencyScore ?? 0} (not computed)</span
					>
				{/snippet}
				{#snippet appCreatedCell(a: AppSummary)}
					{formatDate(a.creationTime)}
				{/snippet}
				{#snippet appActionsCell(a: AppSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openAppDetail(a)}
							title="View"
							aria-label="View app {a.name}"
							class="text-gray-400 hover:text-teal-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditAppModal(a)}
							title="Edit"
							aria-label="Edit app {a.name}"
							class="text-gray-400 hover:text-teal-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteApp(a)}
							title="Delete"
							aria-label="Delete app {a.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const appColumns = defineColumns<AppSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'appArn', label: 'ARN' },
					{ key: 'status', label: 'Status' },
					{ key: 'complianceStatus', label: 'Compliance', render: appComplianceCell },
					{ key: 'resiliencyScore', label: 'Resiliency Score', render: appScoreCell },
					{ key: 'creationTime', label: 'Created', render: appCreatedCell },
					{ key: 'actions', label: '', render: appActionsCell }
				])}
				<DataTable
					rows={filteredApps}
					rowKey={(a) => a.appArn ?? ''}
					columns={appColumns}
					loading={tabLoader.isLoading('apps')}
					emptyMessage="No applications found"
				/>
				<LoadMore hasMore={!!appsNextToken} loading={loadingMoreApps} onLoadMore={loadMoreApps} />
			{:else if activeTab === 'components'}
				{#snippet componentActionsCell(c: AppComponent)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openComponentDetail(c)}
							title="View"
							aria-label="View component {c.name}"
							class="text-gray-400 hover:text-teal-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditComponentModal(c)}
							title="Edit"
							aria-label="Edit component {c.name}"
							class="text-gray-400 hover:text-teal-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteComponent(c)}
							title="Delete"
							aria-label="Delete component {c.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const componentColumns = defineColumns<AppComponent>([
					{ key: 'name', label: 'Name' },
					{ key: 'type', label: 'Type' },
					{ key: 'id', label: 'ID' },
					{ key: 'actions', label: '', render: componentActionsCell }
				])}
				<DataTable
					rows={filteredComponents}
					rowKey={(c) => c.id ?? c.name ?? ''}
					columns={componentColumns}
					loading={tabLoader.isLoading('components')}
					emptyMessage={selectedAppArn ? 'No components found' : 'Select an app to see its components'}
				/>
				<LoadMore
					hasMore={!!componentsNextToken}
					loading={loadingMoreComponents}
					onLoadMore={loadMoreComponents}
				/>
			{:else if activeTab === 'resources'}
				{#snippet resourcePhysIdCell(r: PhysicalResource)}
					{r.physicalResourceId?.identifier ?? '—'}
				{/snippet}
				{#snippet resourceExcludedCell(r: PhysicalResource)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(!r.excluded)}"
						>{r.excluded ? 'Excluded' : 'Included'}</span
					>
				{/snippet}
				{#snippet resourceActionsCell(r: PhysicalResource)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openResourceDetail(r)}
							title="View"
							aria-label="View resource {r.resourceName}"
							class="text-gray-400 hover:text-teal-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditResourceModal(r)}
							title="Edit"
							aria-label="Edit resource {r.resourceName}"
							class="text-gray-400 hover:text-teal-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteResource(r)}
							title="Delete"
							aria-label="Delete resource {r.resourceName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const resourceColumns = defineColumns<PhysicalResource>([
					{ key: 'resourceName', label: 'Name' },
					{ key: 'resourceType', label: 'Type' },
					{ key: 'physicalResourceId', label: 'Physical ID', render: resourcePhysIdCell },
					{ key: 'sourceType', label: 'Source' },
					{ key: 'excluded', label: 'Status', render: resourceExcludedCell },
					{ key: 'actions', label: '', render: resourceActionsCell }
				])}
				<DataTable
					rows={filteredResources}
					rowKey={(r) => r.resourceName ?? ''}
					columns={resourceColumns}
					loading={tabLoader.isLoading('resources')}
					emptyMessage={selectedAppArn ? 'No resources found' : 'Select an app to see its resources'}
				/>
				<LoadMore
					hasMore={!!resourcesNextToken}
					loading={loadingMoreResources}
					onLoadMore={loadMoreResources}
				/>
			{:else if activeTab === 'mappings'}
				{#snippet mappingNameCell(m: ResourceMapping)}
					{mappingDisplayName(m)}
				{/snippet}
				{#snippet mappingPhysIdCell(m: ResourceMapping)}
					{m.physicalResourceId?.identifier ?? '—'}
				{/snippet}
				{#snippet mappingActionsCell(m: ResourceMapping)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => handleRemoveMapping(m)}
							title="Remove"
							aria-label="Remove mapping {mappingDisplayName(m)}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const mappingColumns = defineColumns<ResourceMapping>([
					{ key: 'mappingType', label: 'Type' },
					{ key: 'name', label: 'Name', render: mappingNameCell },
					{ key: 'physicalResourceId', label: 'Physical ID', render: mappingPhysIdCell },
					{ key: 'actions', label: '', render: mappingActionsCell }
				])}
				<DataTable
					rows={filteredMappings}
					rowKey={(m) => `${m.mappingType}:${mappingDisplayName(m)}`}
					columns={mappingColumns}
					loading={tabLoader.isLoading('mappings')}
					emptyMessage={selectedAppArn ? 'No resource mappings found' : 'Select an app to see its mappings'}
				/>
				<LoadMore
					hasMore={!!mappingsNextToken}
					loading={loadingMoreMappings}
					onLoadMore={loadMoreMappings}
				/>
			{:else if activeTab === 'inputSources'}
				{#snippet inputSourceActionsCell(s: AppInputSource)}
					<div class="flex items-center justify-end">
						{#if s.sourceArn}
							<button
								onclick={() => handleDeleteInputSource(s)}
								title="Remove"
								aria-label="Remove input source {s.sourceName ?? s.sourceArn}"
								class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
							>
						{/if}
					</div>
				{/snippet}
				{@const inputSourceColumns = defineColumns<AppInputSource>([
					{ key: 'sourceName', label: 'Name' },
					{ key: 'importType', label: 'Type' },
					{ key: 'sourceArn', label: 'Source ARN' },
					{ key: 'resourceCount', label: 'Resource Count' },
					{ key: 'actions', label: '', render: inputSourceActionsCell }
				])}
				<DataTable
					rows={filteredInputSources}
					rowKey={(s) => s.sourceArn ?? s.sourceName ?? ''}
					columns={inputSourceColumns}
					loading={tabLoader.isLoading('inputSources')}
					emptyMessage={selectedAppArn
						? 'No input sources found'
						: 'Select an app to see its input sources'}
				/>
				<LoadMore
					hasMore={!!inputSourcesNextToken}
					loading={loadingMoreInputSources}
					onLoadMore={loadMoreInputSources}
				/>
			{:else if activeTab === 'policies'}
				{#snippet policyRtoRpoCell(p: ResiliencyPolicy)}
					{DISRUPTION_TYPES.map(
						(dt) => `${dt}: ${p.policy?.[dt]?.rtoInSecs ?? '—'}s/${p.policy?.[dt]?.rpoInSecs ?? '—'}s`
					).join(', ')}
				{/snippet}
				{#snippet policyCostCell(p: ResiliencyPolicy)}
					<span title="No cost-estimation model in this emulator — always empty."
						>{p.estimatedCostTier ?? '—'}</span
					>
				{/snippet}
				{#snippet policyActionsCell(p: ResiliencyPolicy)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openPolicyDetail(p)}
							title="View"
							aria-label="View policy {p.policyName}"
							class="text-gray-400 hover:text-teal-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditPolicyModal(p)}
							title="Edit"
							aria-label="Edit policy {p.policyName}"
							class="text-gray-400 hover:text-teal-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeletePolicy(p)}
							title="Delete"
							aria-label="Delete policy {p.policyName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const policyColumns = defineColumns<ResiliencyPolicy>([
					{ key: 'policyName', label: 'Name' },
					{ key: 'tier', label: 'Tier' },
					{ key: 'rtoRpo', label: 'RTO/RPO by disruption type', render: policyRtoRpoCell },
					{ key: 'estimatedCostTier', label: 'Cost Tier', render: policyCostCell },
					{ key: 'actions', label: '', render: policyActionsCell }
				])}
				<DataTable
					rows={filteredPolicies}
					rowKey={(p) => p.policyArn ?? ''}
					columns={policyColumns}
					loading={tabLoader.isLoading('policies')}
					emptyMessage="No resiliency policies found"
				/>
				<LoadMore
					hasMore={!!policiesNextToken}
					loading={loadingMorePolicies}
					onLoadMore={loadMorePolicies}
				/>
			{:else if activeTab === 'assessments'}
				{#snippet assessmentStatusCell(a: AppAssessmentSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(a.assessmentStatus === 'Success')}"
						>{a.assessmentStatus}</span
					>
				{/snippet}
				{#snippet assessmentComplianceCell(a: AppAssessmentSummary)}
					<span class="text-xs px-2 py-1 rounded-full {complianceClass(a.complianceStatus)}"
						>{a.complianceStatus ?? '—'}</span
					>
				{/snippet}
				{#snippet assessmentStartCell(a: AppAssessmentSummary)}
					{formatDate(a.startTime)}
				{/snippet}
				{#snippet assessmentActionsCell(a: AppAssessmentSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openAssessmentDetail(a)}
							title="View"
							aria-label="View assessment {a.assessmentName}"
							class="text-gray-400 hover:text-teal-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteAssessment(a)}
							title="Delete"
							aria-label="Delete assessment {a.assessmentName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const assessmentColumns = defineColumns<AppAssessmentSummary>([
					{ key: 'assessmentName', label: 'Name' },
					{ key: 'assessmentStatus', label: 'Status', render: assessmentStatusCell },
					{ key: 'complianceStatus', label: 'Compliance', render: assessmentComplianceCell },
					{ key: 'invoker', label: 'Invoker' },
					{ key: 'startTime', label: 'Started', render: assessmentStartCell },
					{ key: 'actions', label: '', render: assessmentActionsCell }
				])}
				<DataTable
					rows={filteredAssessments}
					rowKey={(a) => a.assessmentArn ?? ''}
					columns={assessmentColumns}
					loading={tabLoader.isLoading('assessments')}
					emptyMessage={selectedAppArn
						? 'No assessments found'
						: 'Select an app to see its assessments'}
				/>
				<LoadMore
					hasMore={!!assessmentsNextToken}
					loading={loadingMoreAssessments}
					onLoadMore={loadMoreAssessments}
				/>
			{:else if activeTab === 'templates'}
				{#snippet templateLocationCell(t: RecommendationTemplate)}
					<span
						title="Synthetic bucket/prefix — no real S3 object is written by this emulator."
						>{t.templatesLocation?.bucket ?? '—'}/{t.templatesLocation?.prefix ?? '—'}</span
					>
				{/snippet}
				{#snippet templateActionsCell(t: RecommendationTemplate)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => handleDeleteTemplate(t)}
							title="Delete"
							aria-label="Delete template {t.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const templateColumns = defineColumns<RecommendationTemplate>([
					{ key: 'name', label: 'Name' },
					{ key: 'format', label: 'Format' },
					{ key: 'status', label: 'Status' },
					{ key: 'assessmentArn', label: 'Assessment ARN' },
					{ key: 'templatesLocation', label: 'S3 Location', render: templateLocationCell },
					{ key: 'actions', label: '', render: templateActionsCell }
				])}
				<DataTable
					rows={filteredTemplates}
					rowKey={(t) => t.recommendationTemplateArn ?? ''}
					columns={templateColumns}
					loading={tabLoader.isLoading('templates')}
					emptyMessage="No recommendation templates found"
				/>
				<LoadMore
					hasMore={!!templatesNextToken}
					loading={loadingMoreTemplates}
					onLoadMore={loadMoreTemplates}
				/>
			{/if}
		</div>
	</div>
</div>

<!-- Create App -->
<Modal bind:this={createAppModal} title="Create Application">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-app-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-app-name"
					bind:value={newAppName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-app-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="new-app-desc"
					bind:value={newAppDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-app-policy" class="text-sm text-slate-600 dark:text-slate-300"
					>Resiliency Policy ARN (optional)</label
				>
				<input
					id="new-app-policy"
					bind:value={newAppPolicyArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-app-schedule" class="text-sm text-slate-600 dark:text-slate-300"
					>Assessment Schedule</label
				>
				<select
					id="new-app-schedule"
					bind:value={newAppAssessmentSchedule}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="Disabled">Disabled</option>
					<option value="Daily">Daily</option>
				</select>
			</div>
			<div>
				<label for="new-app-registry" class="text-sm text-slate-600 dark:text-slate-300"
					>AppRegistry Application ARN (optional)</label
				>
				<input
					id="new-app-registry"
					bind:value={newAppAwsApplicationArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createAppError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAppError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createAppModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateApp}
			disabled={creatingApp}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{creatingApp ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<!-- Edit App -->
<Modal bind:this={editAppModal} title="Edit Application">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-app-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="edit-app-desc"
					bind:value={editAppDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-app-policy" class="text-sm text-slate-600 dark:text-slate-300"
					>Resiliency Policy ARN</label
				>
				<input
					id="edit-app-policy"
					bind:value={editAppPolicyArn}
					disabled={editAppClearPolicy}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white disabled:opacity-50"
				/>
			</div>
			<label class="flex items-center gap-2 text-sm">
				<input type="checkbox" bind:checked={editAppClearPolicy} /> Clear resiliency policy
			</label>
			<div>
				<label for="edit-app-schedule" class="text-sm text-slate-600 dark:text-slate-300"
					>Assessment Schedule</label
				>
				<select
					id="edit-app-schedule"
					bind:value={editAppAssessmentSchedule}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="Disabled">Disabled</option>
					<option value="Daily">Daily</option>
				</select>
			</div>
			{#if editAppError}
				<p class="text-sm text-red-600 dark:text-red-400">{editAppError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editAppModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditApp}
			disabled={editingApp}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{editingApp ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<!-- App Detail -->
<Modal bind:this={appDetailModal} title="Application Detail">
	{#snippet children()}
		<div class="space-y-4 max-h-[70vh] overflow-y-auto">
			{#if appDetailLoading}
				<p class="text-sm text-gray-500 dark:text-gray-400">Loading...</p>
			{:else if appDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{appDetailError}</p>
			{:else if viewedApp}
				<dl class="grid grid-cols-1 sm:grid-cols-2 gap-2 text-sm">
					<div><dt class="text-gray-500 dark:text-gray-400">Name</dt><dd>{viewedApp.name}</dd></div>
					<div>
						<dt class="text-gray-500 dark:text-gray-400">ARN</dt>
						<dd class="break-all">{viewedApp.appArn}</dd>
					</div>
					<div>
						<dt class="text-gray-500 dark:text-gray-400">Status</dt><dd>{viewedApp.status ?? '—'}</dd>
					</div>
					<div>
						<dt class="text-gray-500 dark:text-gray-400">Compliance</dt>
						<dd>
							<span
								class="text-xs px-2 py-1 rounded-full {complianceClass(viewedApp.complianceStatus)}"
								>{viewedApp.complianceStatus ?? '—'}</span
							>
							{#if viewedApp.complianceStatus === 'PolicyMet'}
								<p class="text-xs text-gray-400 mt-1">
									Stand-in rule: this emulator marks PolicyMet once any policy is bound — it does
									not evaluate whether resources actually meet the policy's RTO/RPO targets.
								</p>
							{/if}
						</dd>
					</div>
					<div>
						<dt class="text-gray-500 dark:text-gray-400">Resiliency Score</dt>
						<dd class="italic text-gray-400" title={SCORE_NOTE}>
							{viewedApp.resiliencyScore ?? 0} (not computed by this emulator)
						</dd>
					</div>
					<div>
						<dt class="text-gray-500 dark:text-gray-400">Drift Status</dt>
						<dd>{viewedApp.driftStatus ?? '—'}</dd>
					</div>
					<div>
						<dt class="text-gray-500 dark:text-gray-400">RTO / RPO</dt>
						<dd>{viewedApp.rtoInSecs ?? '—'}s / {viewedApp.rpoInSecs ?? '—'}s</dd>
					</div>
					<div>
						<dt class="text-gray-500 dark:text-gray-400">Policy ARN</dt>
						<dd class="break-all">{viewedApp.policyArn ?? '—'}</dd>
					</div>
					<div>
						<dt class="text-gray-500 dark:text-gray-400">Assessment Schedule</dt>
						<dd>{viewedApp.assessmentSchedule ?? '—'}</dd>
					</div>
					<div>
						<dt class="text-gray-500 dark:text-gray-400">Created</dt>
						<dd>{formatDate(viewedApp.creationTime)}</dd>
					</div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<h3 class="text-sm font-medium text-slate-700 dark:text-slate-200 mb-2">Tags</h3>
					<ul class="space-y-1">
						{#each Object.entries(appTags) as [k, v] (k)}
							<li class="flex items-center justify-between text-sm">
								<span>{k} = {v}</span>
								<button
									onclick={() => removeAppTag(k)}
									aria-label="Remove tag {k}"
									class="text-gray-400 hover:text-red-500"><Trash2 class="w-3.5 h-3.5" /></button
								>
							</li>
						{/each}
					</ul>
					<div class="flex items-center gap-2 mt-2">
						<input
							bind:value={addAppTagKey}
							placeholder="Key"
							aria-label="New tag key"
							class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-1/3"
						/>
						<input
							bind:value={addAppTagValue}
							placeholder="Value"
							aria-label="New tag value"
							class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-1/3"
						/>
						<button
							onclick={submitAddAppTag}
							class="px-3 py-1 text-sm rounded-lg bg-teal-600 text-white hover:bg-teal-700"
							>Add</button
						>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<h3 class="text-sm font-medium text-slate-700 dark:text-slate-200 mb-2">Versions</h3>
					<ul class="space-y-1 text-sm">
						{#each appVersions as v (v.appVersion)}
							<li>{v.appVersion} {v.versionName ? `(${v.versionName})` : ''} — {formatDate(v.creationTime)}</li>
						{/each}
					</ul>
					<div class="flex items-center gap-2 mt-2">
						<input
							bind:value={newVersionName}
							placeholder="Version name (optional)"
							aria-label="New version name"
							class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white flex-1"
						/>
						<button
							onclick={submitPublishVersion}
							disabled={publishingVersion}
							class="px-3 py-1 text-sm rounded-lg bg-teal-600 text-white hover:bg-teal-700 disabled:opacity-50"
							>{publishingVersion ? 'Publishing...' : 'Publish draft'}</button
						>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<button
						onclick={openTemplateModal}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-600 dark:text-gray-300"
						>Edit draft template</button
					>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => appDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- Draft Template -->
<Modal bind:this={templateModal} title="Draft App Template">
	{#snippet children()}
		<div class="space-y-3">
			{#if templateLoading}
				<p class="text-sm text-gray-500 dark:text-gray-400">Loading...</p>
			{:else}
				<div>
					<label for="template-body" class="text-sm text-slate-600 dark:text-slate-300"
						>Template body</label
					>
					<textarea
						id="template-body"
						bind:value={templateBody}
						rows="10"
						class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					></textarea>
				</div>
			{/if}
			{#if templateError}
				<p class="text-sm text-red-600 dark:text-red-400">{templateError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => templateModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitTemplate}
			disabled={templateSaving}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{templateSaving ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<!-- Create Component -->
<Modal bind:this={createComponentModal} title="Create Component">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-component-name" class="text-sm text-slate-600 dark:text-slate-300"
					>Name</label
				>
				<input
					id="new-component-name"
					bind:value={newComponentName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-component-type" class="text-sm text-slate-600 dark:text-slate-300"
					>Type</label
				>
				<input
					id="new-component-type"
					bind:value={newComponentType}
					placeholder="AWS::ResilienceHub::AppComponent::..."
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createComponentError}
				<p class="text-sm text-red-600 dark:text-red-400">{createComponentError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createComponentModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateComponent}
			disabled={creatingComponent}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{creatingComponent ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<!-- Component Detail -->
<Modal bind:this={componentDetailModal} title="Component Detail">
	{#snippet children()}
		<div class="space-y-2 text-sm">
			{#if componentDetailLoading}
				<p class="text-gray-500 dark:text-gray-400">Loading...</p>
			{:else if componentDetailError}
				<p class="text-red-600 dark:text-red-400">{componentDetailError}</p>
			{:else if viewedComponent}
				<p><span class="text-gray-500 dark:text-gray-400">Name:</span> {viewedComponent.name}</p>
				<p><span class="text-gray-500 dark:text-gray-400">Type:</span> {viewedComponent.type}</p>
				<p><span class="text-gray-500 dark:text-gray-400">ID:</span> {viewedComponent.id}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => componentDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- Edit Component -->
<Modal bind:this={editComponentModal} title="Edit Component">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-component-name" class="text-sm text-slate-600 dark:text-slate-300"
					>Name</label
				>
				<input
					id="edit-component-name"
					bind:value={editComponentName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-component-type" class="text-sm text-slate-600 dark:text-slate-300"
					>Type</label
				>
				<input
					id="edit-component-type"
					bind:value={editComponentType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editComponentError}
				<p class="text-sm text-red-600 dark:text-red-400">{editComponentError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editComponentModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditComponent}
			disabled={editingComponent}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{editingComponent ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<!-- Create Resource -->
<Modal bind:this={createResourceModal} title="Create Resource">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-resource-name" class="text-sm text-slate-600 dark:text-slate-300"
					>Resource name</label
				>
				<input
					id="new-resource-name"
					bind:value={newResourceName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-resource-physid" class="text-sm text-slate-600 dark:text-slate-300"
					>Physical resource ID (ARN or native ID)</label
				>
				<input
					id="new-resource-physid"
					bind:value={newResourcePhysicalId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-resource-type" class="text-sm text-slate-600 dark:text-slate-300"
					>Resource type</label
				>
				<input
					id="new-resource-type"
					bind:value={newResourceType}
					placeholder="AWS::EC2::Instance"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-resource-components" class="text-sm text-slate-600 dark:text-slate-300"
					>Application component names (comma-separated; unknown names are auto-created)</label
				>
				<input
					id="new-resource-components"
					bind:value={newResourceComponents}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="new-resource-account" class="text-sm text-slate-600 dark:text-slate-300"
						>AWS Account ID (optional)</label
					>
					<input
						id="new-resource-account"
						bind:value={newResourceAwsAccountId}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="new-resource-region" class="text-sm text-slate-600 dark:text-slate-300"
						>AWS Region (optional)</label
					>
					<input
						id="new-resource-region"
						bind:value={newResourceAwsRegion}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			{#if createResourceError}
				<p class="text-sm text-red-600 dark:text-red-400">{createResourceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createResourceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateResource}
			disabled={creatingResource}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{creatingResource ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<!-- Resource Detail -->
<Modal bind:this={resourceDetailModal} title="Resource Detail">
	{#snippet children()}
		<div class="space-y-2 text-sm">
			{#if resourceDetailLoading}
				<p class="text-gray-500 dark:text-gray-400">Loading...</p>
			{:else if resourceDetailError}
				<p class="text-red-600 dark:text-red-400">{resourceDetailError}</p>
			{:else if viewedResource}
				<p><span class="text-gray-500 dark:text-gray-400">Name:</span> {viewedResource.resourceName}</p>
				<p><span class="text-gray-500 dark:text-gray-400">Type:</span> {viewedResource.resourceType}</p>
				<p>
					<span class="text-gray-500 dark:text-gray-400">Physical ID:</span>
					{viewedResource.physicalResourceId?.identifier} ({viewedResource.physicalResourceId?.type})
				</p>
				<p><span class="text-gray-500 dark:text-gray-400">Source:</span> {viewedResource.sourceType}</p>
				<p>
					<span class="text-gray-500 dark:text-gray-400">App Components:</span>
					{(viewedResource.appComponents ?? []).map((c) => c.name).join(', ') || '—'}
				</p>
				<p><span class="text-gray-500 dark:text-gray-400">Excluded:</span> {viewedResource.excluded ? 'Yes' : 'No'}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => resourceDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- Edit Resource -->
<Modal bind:this={editResourceModal} title="Edit Resource">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-resource-type" class="text-sm text-slate-600 dark:text-slate-300"
					>Resource type</label
				>
				<input
					id="edit-resource-type"
					bind:value={editResourceType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-resource-physid" class="text-sm text-slate-600 dark:text-slate-300"
					>Physical resource ID</label
				>
				<input
					id="edit-resource-physid"
					bind:value={editResourcePhysicalId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<label class="flex items-center gap-2 text-sm">
				<input type="checkbox" bind:checked={editResourceExcluded} /> Exclude from assessments
			</label>
			{#if editResourceError}
				<p class="text-sm text-red-600 dark:text-red-400">{editResourceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editResourceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditResource}
			disabled={editingResource}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{editingResource ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<!-- Create Mapping -->
<Modal bind:this={createMappingModal} title="Add Resource Mapping">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-mapping-type" class="text-sm text-slate-600 dark:text-slate-300"
					>Mapping type</label
				>
				<select
					id="new-mapping-type"
					bind:value={newMappingType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="Resource">Resource</option>
					<option value="CfnStack">CloudFormation Stack</option>
					<option value="ResourceGroup">Resource Group</option>
					<option value="Terraform">Terraform</option>
					<option value="EKS">EKS</option>
					<option value="AppRegistryApp">AppRegistry App</option>
				</select>
				{#if newMappingType !== 'Resource'}
					<p class="text-xs text-amber-600 dark:text-amber-400 mt-1">
						This emulator only resolves the "Resource" mapping type into concrete resources —
						other types are accepted and stored but left unresolved. See PARITY.md.
					</p>
				{/if}
			</div>
			<div>
				<label for="new-mapping-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-mapping-name"
					bind:value={newMappingName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-mapping-physid" class="text-sm text-slate-600 dark:text-slate-300"
					>Physical resource ID</label
				>
				<input
					id="new-mapping-physid"
					bind:value={newMappingPhysicalId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-mapping-physid-type" class="text-sm text-slate-600 dark:text-slate-300"
					>Physical ID type</label
				>
				<select
					id="new-mapping-physid-type"
					bind:value={newMappingPhysicalIdType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="Native">Native</option>
					<option value="Arn">Arn</option>
				</select>
			</div>
			{#if createMappingError}
				<p class="text-sm text-red-600 dark:text-red-400">{createMappingError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createMappingModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateMapping}
			disabled={creatingMapping}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{creatingMapping ? 'Adding...' : 'Add'}</button
		>
	{/snippet}
</Modal>

<!-- Import Resources (Input Sources) -->
<Modal bind:this={importModal} title="Import Resources">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="import-source-arns" class="text-sm text-slate-600 dark:text-slate-300"
					>Source ARNs (one per line, or comma-separated)</label
				>
				<textarea
					id="import-source-arns"
					bind:value={importSourceArns}
					rows="5"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			<p class="text-xs text-gray-400">
				This emulator records real input-source bookkeeping and transitions the import status to
				Success, but does not discover real resources from the named sources. See PARITY.md.
			</p>
			{#if importError}
				<p class="text-sm text-red-600 dark:text-red-400">{importError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => importModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitImport}
			disabled={importing}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{importing ? 'Importing...' : 'Import'}</button
		>
	{/snippet}
</Modal>

<!-- Create Policy -->
<Modal bind:this={createPolicyModal} title="Create Resiliency Policy">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-policy-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-policy-name"
					bind:value={newPolicyName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-policy-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="new-policy-desc"
					bind:value={newPolicyDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-policy-tier" class="text-sm text-slate-600 dark:text-slate-300">Tier</label>
				<select
					id="new-policy-tier"
					bind:value={newPolicyTier}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="MissionCritical">Mission Critical</option>
					<option value="Critical">Critical</option>
					<option value="Important">Important</option>
					<option value="CoreServices">Core Services</option>
					<option value="NonCritical">Non Critical</option>
					<option value="NotApplicable">Not Applicable</option>
				</select>
			</div>
			<fieldset class="space-y-2">
				<legend class="text-sm text-slate-600 dark:text-slate-300"
					>RTO / RPO in seconds, per disruption type</legend
				>
				{#each DISRUPTION_TYPES as dt (dt)}
					<div class="grid grid-cols-3 items-center gap-2">
						<span class="text-sm">{dt}</span>
						<input
							type="number"
							min="0"
							aria-label="{dt} RTO in seconds"
							bind:value={newPolicyFailurePolicy[dt].rtoInSecs}
							class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<input
							type="number"
							min="0"
							aria-label="{dt} RPO in seconds"
							bind:value={newPolicyFailurePolicy[dt].rpoInSecs}
							class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				{/each}
			</fieldset>
			{#if createPolicyError}
				<p class="text-sm text-red-600 dark:text-red-400">{createPolicyError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createPolicyModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreatePolicy}
			disabled={creatingPolicy}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{creatingPolicy ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<!-- Policy Detail -->
<Modal bind:this={policyDetailModal} title="Resiliency Policy Detail">
	{#snippet children()}
		<div class="space-y-2 text-sm max-h-[70vh] overflow-y-auto">
			{#if policyDetailLoading}
				<p class="text-gray-500 dark:text-gray-400">Loading...</p>
			{:else if policyDetailError}
				<p class="text-red-600 dark:text-red-400">{policyDetailError}</p>
			{:else if viewedPolicy}
				<p><span class="text-gray-500 dark:text-gray-400">Name:</span> {viewedPolicy.policyName}</p>
				<p class="break-all">
					<span class="text-gray-500 dark:text-gray-400">ARN:</span> {viewedPolicy.policyArn}
				</p>
				<p><span class="text-gray-500 dark:text-gray-400">Tier:</span> {viewedPolicy.tier}</p>
				<p>
					<span class="text-gray-500 dark:text-gray-400">Description:</span>
					{viewedPolicy.policyDescription ?? '—'}
				</p>
				<p>
					<span class="text-gray-500 dark:text-gray-400">Data Location Constraint:</span>
					{viewedPolicy.dataLocationConstraint ?? '—'}
				</p>
				<p title="No cost-estimation model in this emulator — always empty.">
					<span class="text-gray-500 dark:text-gray-400">Estimated Cost Tier:</span>
					{viewedPolicy.estimatedCostTier ?? '— (not computed by this emulator)'}
				</p>
				<table class="w-full text-sm mt-2">
					<thead>
						<tr class="text-left text-gray-500 dark:text-gray-400">
							<th class="pr-4">Disruption Type</th>
							<th class="pr-4">RTO (s)</th>
							<th>RPO (s)</th>
						</tr>
					</thead>
					<tbody>
						{#each DISRUPTION_TYPES as dt (dt)}
							<tr>
								<td class="pr-4">{dt}</td>
								<td class="pr-4">{viewedPolicy.policy?.[dt]?.rtoInSecs ?? '—'}</td>
								<td>{viewedPolicy.policy?.[dt]?.rpoInSecs ?? '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-2 mt-2">
					<h3 class="text-sm font-medium text-slate-700 dark:text-slate-200 mb-1">Tags</h3>
					<ul class="space-y-1">
						{#each Object.entries(policyTags) as [k, v] (k)}
							<li>{k} = {v}</li>
						{/each}
					</ul>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => policyDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- Edit Policy -->
<Modal bind:this={editPolicyModal} title="Edit Resiliency Policy">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-policy-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="edit-policy-desc"
					bind:value={editPolicyDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-policy-tier" class="text-sm text-slate-600 dark:text-slate-300">Tier</label>
				<select
					id="edit-policy-tier"
					bind:value={editPolicyTier}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="MissionCritical">Mission Critical</option>
					<option value="Critical">Critical</option>
					<option value="Important">Important</option>
					<option value="CoreServices">Core Services</option>
					<option value="NonCritical">Non Critical</option>
					<option value="NotApplicable">Not Applicable</option>
				</select>
			</div>
			<fieldset class="space-y-2">
				<legend class="text-sm text-slate-600 dark:text-slate-300"
					>RTO / RPO in seconds, per disruption type</legend
				>
				{#each DISRUPTION_TYPES as dt (dt)}
					<div class="grid grid-cols-3 items-center gap-2">
						<span class="text-sm">{dt}</span>
						<input
							type="number"
							min="0"
							aria-label="{dt} RTO in seconds"
							bind:value={editPolicyFailurePolicy[dt].rtoInSecs}
							class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<input
							type="number"
							min="0"
							aria-label="{dt} RPO in seconds"
							bind:value={editPolicyFailurePolicy[dt].rpoInSecs}
							class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				{/each}
			</fieldset>
			{#if editPolicyError}
				<p class="text-sm text-red-600 dark:text-red-400">{editPolicyError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editPolicyModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditPolicy}
			disabled={editingPolicy}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{editingPolicy ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<!-- Start Assessment -->
<Modal bind:this={createAssessmentModal} title="Start Assessment">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-assessment-name" class="text-sm text-slate-600 dark:text-slate-300"
					>Assessment name</label
				>
				<input
					id="new-assessment-name"
					bind:value={newAssessmentName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<p class="text-xs text-gray-400">
				Runs against the app's draft version. The resulting assessment's resiliency score and
				AI-generated summary are never computed by this emulator — see PARITY.md.
			</p>
			{#if createAssessmentError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAssessmentError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createAssessmentModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateAssessment}
			disabled={creatingAssessment}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{creatingAssessment ? 'Starting...' : 'Start'}</button
		>
	{/snippet}
</Modal>

<!-- Assessment Detail -->
<Modal bind:this={assessmentDetailModal} title="Assessment Detail">
	{#snippet children()}
		<div class="space-y-3 text-sm max-h-[70vh] overflow-y-auto">
			{#if assessmentDetailLoading}
				<p class="text-gray-500 dark:text-gray-400">Loading...</p>
			{:else if assessmentDetailError}
				<p class="text-red-600 dark:text-red-400">{assessmentDetailError}</p>
			{:else if viewedAssessment}
				<p><span class="text-gray-500 dark:text-gray-400">Name:</span> {viewedAssessment.assessmentName}</p>
				<p class="break-all">
					<span class="text-gray-500 dark:text-gray-400">ARN:</span> {viewedAssessment.assessmentArn}
				</p>
				<p>
					<span class="text-gray-500 dark:text-gray-400">Status:</span>
					<span class="text-xs px-2 py-1 rounded-full {statusClass(viewedAssessment.assessmentStatus === 'Success')}"
						>{viewedAssessment.assessmentStatus}</span
					>
				</p>
				<p>
					<span class="text-gray-500 dark:text-gray-400">Compliance:</span>
					<span class="text-xs px-2 py-1 rounded-full {complianceClass(viewedAssessment.complianceStatus)}"
						>{viewedAssessment.complianceStatus ?? '—'}</span
					>
				</p>
				<p class="italic text-gray-400" title={SCORE_NOTE}>
					Resiliency Score: {viewedAssessment.resiliencyScore?.score ?? 0} (not computed by this
					emulator)
				</p>
				<p class="italic text-gray-400">
					AI Summary: not generated by this emulator — real AWS Resilience Hub generates this via a
					Bedrock LLM, available only in the US East (N. Virginia) Region. Always empty here.
				</p>
				{#if viewedAssessment.compliance}
					<table class="w-full text-sm mt-2">
						<thead>
							<tr class="text-left text-gray-500 dark:text-gray-400">
								<th class="pr-4">Disruption Type</th>
								<th class="pr-4">Compliance</th>
								<th class="pr-4">Achievable RTO/RPO</th>
								<th>Current RTO/RPO</th>
							</tr>
						</thead>
						<tbody>
							{#each DISRUPTION_TYPES as dt (dt)}
								{@const dc = viewedAssessment.compliance?.[dt]}
								{#if dc}
									<tr>
										<td class="pr-4">{dt}</td>
										<td class="pr-4">{dc.complianceStatus}</td>
										<td class="pr-4">{dc.achievableRtoInSecs ?? '—'}s / {dc.achievableRpoInSecs ?? '—'}s</td>
										<td>{dc.currentRtoInSecs ?? '—'}s / {dc.currentRpoInSecs ?? '—'}s</td>
									</tr>
								{/if}
							{/each}
						</tbody>
					</table>
				{/if}
				<div class="border-t border-slate-200 dark:border-slate-700 pt-2 mt-2">
					<h3 class="text-sm font-medium text-slate-700 dark:text-slate-200 mb-1">
						Application Component Compliances
					</h3>
					{#if componentCompliances.length === 0}
						<p class="text-gray-500 dark:text-gray-400">No application components assessed.</p>
					{:else}
						<ul class="space-y-1">
							{#each componentCompliances as cc, i (i)}
								<li>{cc.appComponentName}: {cc.status ?? '—'}</li>
							{/each}
						</ul>
					{/if}
				</div>
				<p class="text-xs text-gray-400 border-t border-slate-200 dark:border-slate-700 pt-2 mt-2">
					Compliance drifts, resource drifts, and the SOP/alarm/test/component recommendation
					families are always empty in this emulator — no drift-detection or
					recommendation-engine content is ever fabricated. See PARITY.md.
				</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => assessmentDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- Create Recommendation Template -->
<Modal bind:this={createTemplateModal} title="Create Recommendation Template">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-template-assessment" class="text-sm text-slate-600 dark:text-slate-300"
					>Assessment ARN</label
				>
				<input
					id="new-template-assessment"
					bind:value={newTemplateAssessmentArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-template-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-template-name"
					bind:value={newTemplateName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-template-format" class="text-sm text-slate-600 dark:text-slate-300"
					>Format</label
				>
				<select
					id="new-template-format"
					bind:value={newTemplateFormat}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="CfnJson">CloudFormation JSON</option>
					<option value="CfnYaml">CloudFormation YAML</option>
				</select>
			</div>
			<p class="text-xs text-gray-400">
				Since this emulator never generates real SOP/alarm/test recommendations, the resulting
				template is an empty/trivial one, and its S3 location is a synthetic placeholder — no real
				object is written. See PARITY.md.
			</p>
			{#if createTemplateError}
				<p class="text-sm text-red-600 dark:text-red-400">{createTemplateError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createTemplateModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateTemplate}
			disabled={creatingTemplate}
			class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-medium text-white hover:bg-teal-700 disabled:opacity-50"
			>{creatingTemplate ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>
