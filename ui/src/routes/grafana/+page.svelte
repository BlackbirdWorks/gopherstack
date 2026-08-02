<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { currentRegion } from '$lib/region.svelte';
	import { urlState } from '$lib/url-state.svelte';
	import { getGrafanaClient } from '$lib/aws-client';
	import {
		ListWorkspacesCommand,
		CreateWorkspaceCommand,
		DescribeWorkspaceCommand,
		UpdateWorkspaceCommand,
		DeleteWorkspaceCommand,
		DescribeWorkspaceAuthenticationCommand,
		UpdateWorkspaceAuthenticationCommand,
		DescribeWorkspaceConfigurationCommand,
		UpdateWorkspaceConfigurationCommand,
		AssociateLicenseCommand,
		DisassociateLicenseCommand,
		CreateWorkspaceApiKeyCommand,
		DeleteWorkspaceApiKeyCommand,
		CreateWorkspaceServiceAccountCommand,
		DeleteWorkspaceServiceAccountCommand,
		ListWorkspaceServiceAccountsCommand,
		CreateWorkspaceServiceAccountTokenCommand,
		DeleteWorkspaceServiceAccountTokenCommand,
		ListWorkspaceServiceAccountTokensCommand,
		ListPermissionsCommand,
		UpdatePermissionsCommand,
		ListVersionsCommand,
		ListTagsForResourceCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type WorkspaceSummary,
		type WorkspaceDescription,
		type ServiceAccountSummary,
		type ServiceAccountTokenSummary,
		type PermissionEntry
	} from '@aws-sdk/client-grafana';
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
	import { LineChart, Plus, Trash2, Eye, Pencil, Ban } from 'lucide-svelte';

	const client = regionalClient(getGrafanaClient);

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

	type TabId = 'workspaces' | 'serviceAccounts' | 'tokens' | 'permissions' | 'versions';

	const tabs: TabDef[] = [
		{ id: 'workspaces', label: 'Workspaces' },
		{ id: 'serviceAccounts', label: 'Service Accounts' },
		{ id: 'tokens', label: 'Tokens' },
		{ id: 'permissions', label: 'Permissions' },
		{ id: 'versions', label: 'Versions' }
	];

	// URL-backed (?tab=...); see url-state.svelte.ts. Read via untrack() inside
	// the onRegionChange effect below (switchTab() also writes it): without
	// untrack, every tab switch would re-trigger the region effect and
	// double-fetch.
	const pageTabParam = urlState<TabId>('tab', 'workspaces');
	let activeTab = $derived(pageTabParam.get());
	let searchQuery = $state('');

	// Service Accounts / Tokens / Permissions are all scoped to one
	// workspace, the same shared-selector pattern accessanalyzer uses for its
	// analyzer-scoped tabs. Tokens is additionally scoped to one service
	// account within that workspace.
	let selectedWorkspaceId = $state('');
	let selectedServiceAccountId = $state('');
	const workspaceScopedTabs: TabId[] = ['serviceAccounts', 'tokens', 'permissions'];

	let workspaces = $state<WorkspaceSummary[]>([]);
	let workspacesNextToken = $state<string | undefined>();
	let loadingMoreWorkspaces = $state(false);

	let serviceAccounts = $state<ServiceAccountSummary[]>([]);
	let serviceAccountsNextToken = $state<string | undefined>();
	let loadingMoreServiceAccounts = $state(false);

	let tokens = $state<ServiceAccountTokenSummary[]>([]);
	let tokensNextToken = $state<string | undefined>();
	let loadingMoreTokens = $state(false);

	let permissions = $state<PermissionEntry[]>([]);
	let permissionsNextToken = $state<string | undefined>();
	let loadingMorePermissions = $state(false);

	// ListVersions accepts an optional workspaceId filter ("versions this
	// workspace can be upgraded to"); left empty it returns the global
	// create-time list. Kept as its own free-text field rather than reusing
	// selectedWorkspaceId, since versions is not in workspaceScopedTabs (the
	// real ListVersions op does not require a workspace at all).
	let versionsWorkspaceFilter = $state('');
	let versions = $state<string[]>([]);
	let versionsNextToken = $state<string | undefined>();
	let loadingMoreVersions = $state(false);

	async function fetchWorkspaces(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListWorkspacesCommand({ nextToken: reset ? undefined : workspacesNextToken })
		);
		workspaces = reset ? (resp.workspaces ?? []) : [...workspaces, ...(resp.workspaces ?? [])];
		workspacesNextToken = resp.nextToken;
		if (!selectedWorkspaceId && workspaces.length > 0) {
			selectedWorkspaceId = workspaces[0].id ?? '';
		}
	}

	async function fetchServiceAccounts(reset: boolean): Promise<void> {
		if (!selectedWorkspaceId) {
			serviceAccounts = [];
			serviceAccountsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListWorkspaceServiceAccountsCommand({
				workspaceId: selectedWorkspaceId,
				nextToken: reset ? undefined : serviceAccountsNextToken
			})
		);
		serviceAccounts = reset
			? (resp.serviceAccounts ?? [])
			: [...serviceAccounts, ...(resp.serviceAccounts ?? [])];
		serviceAccountsNextToken = resp.nextToken;
		if (!selectedServiceAccountId && serviceAccounts.length > 0) {
			selectedServiceAccountId = serviceAccounts[0].id ?? '';
		}
	}

	async function fetchTokens(reset: boolean): Promise<void> {
		if (!selectedWorkspaceId || !selectedServiceAccountId) {
			tokens = [];
			tokensNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListWorkspaceServiceAccountTokensCommand({
				workspaceId: selectedWorkspaceId,
				serviceAccountId: selectedServiceAccountId,
				nextToken: reset ? undefined : tokensNextToken
			})
		);
		tokens = reset
			? (resp.serviceAccountTokens ?? [])
			: [...tokens, ...(resp.serviceAccountTokens ?? [])];
		tokensNextToken = resp.nextToken;
	}

	async function fetchPermissions(reset: boolean): Promise<void> {
		if (!selectedWorkspaceId) {
			permissions = [];
			permissionsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListPermissionsCommand({
				workspaceId: selectedWorkspaceId,
				nextToken: reset ? undefined : permissionsNextToken
			})
		);
		permissions = reset ? (resp.permissions ?? []) : [...permissions, ...(resp.permissions ?? [])];
		permissionsNextToken = resp.nextToken;
	}

	async function fetchVersions(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListVersionsCommand({
				workspaceId: versionsWorkspaceFilter.trim() || undefined,
				nextToken: reset ? undefined : versionsNextToken
			})
		);
		versions = reset
			? (resp.grafanaVersions ?? [])
			: [...versions, ...(resp.grafanaVersions ?? [])];
		versionsNextToken = resp.nextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		workspaces: () => fetchWorkspaces(true).catch(rethrowDescribed),
		serviceAccounts: () => fetchServiceAccounts(true).catch(rethrowDescribed),
		tokens: () => fetchTokens(true).catch(rethrowDescribed),
		permissions: () => fetchPermissions(true).catch(rethrowDescribed),
		versions: () => fetchVersions(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		pageTabParam.set(id as TabId);
		searchQuery = '';
		tabLoader.load(id as TabId);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	function onWorkspaceSelect(id: string): void {
		selectedWorkspaceId = id;
		selectedServiceAccountId = '';
		if (workspaceScopedTabs.includes(activeTab)) {
			tabLoader.refresh(activeTab);
		}
	}

	function onServiceAccountSelect(id: string): void {
		selectedServiceAccountId = id;
		if (activeTab === 'tokens') {
			tabLoader.refresh('tokens');
		}
	}

	// Workspaces is the parent resource for the three workspace-scoped tabs:
	// on a region change the previously selected workspace ID belongs to the
	// old region and must not be reused, so reload workspaces first (which
	// re-selects a workspace for the new region) before reloading whichever
	// tab is active. `activeTab` is read via untrack() because switchTab()
	// also writes it (via pageTabParam): without untrack, every tab switch
	// would re-trigger this region effect and double-fetch.
	onRegionChange(() => {
		selectedWorkspaceId = '';
		selectedServiceAccountId = '';
		workspaces = [];
		workspacesNextToken = undefined;
		void tabLoader.refresh('workspaces').then(() => {
			const tab = untrack(() => activeTab);
			if (tab !== 'workspaces') {
				if (tab === 'serviceAccounts' || tab === 'tokens') {
					void tabLoader.refresh('serviceAccounts').then(() => {
						if (tab === 'tokens') tabLoader.refresh('tokens');
					});
				} else {
					tabLoader.refresh(tab);
				}
			}
		});
	});

	const filteredWorkspaces = $derived(
		workspaces.filter((w) => {
			const q = searchQuery.toLowerCase();
			return (
				(w.name ?? '').toLowerCase().includes(q) ||
				(w.id ?? '').toLowerCase().includes(q) ||
				(w.status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredServiceAccounts = $derived(
		serviceAccounts.filter((s) => (s.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredTokens = $derived(
		tokens.filter((t) => (t.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredPermissions = $derived(
		permissions.filter((p) => (p.user?.id ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredVersions = $derived(
		versions
			.map((v) => ({ version: v }))
			.filter((v) => v.version.toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreWorkspaces(): Promise<void> {
		loadingMoreWorkspaces = true;
		try {
			await fetchWorkspaces(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreWorkspaces = false;
		}
	}
	async function loadMoreServiceAccounts(): Promise<void> {
		loadingMoreServiceAccounts = true;
		try {
			await fetchServiceAccounts(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreServiceAccounts = false;
		}
	}
	async function loadMoreTokens(): Promise<void> {
		loadingMoreTokens = true;
		try {
			await fetchTokens(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTokens = false;
		}
	}
	async function loadMorePermissions(): Promise<void> {
		loadingMorePermissions = true;
		try {
			await fetchPermissions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMorePermissions = false;
		}
	}
	async function loadMoreVersions(): Promise<void> {
		loadingMoreVersions = true;
		try {
			await fetchVersions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreVersions = false;
		}
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// --- Workspaces: create / delete / detail / edit ---

	let createWorkspaceModal = $state<Modal | null>(null);
	let creatingWorkspace = $state(false);
	let createWorkspaceError = $state<string | null>(null);
	let newWorkspaceName = $state('');
	let newWorkspaceDescription = $state('');
	let newAccountAccessType = $state<'CURRENT_ACCOUNT' | 'ORGANIZATION'>('CURRENT_ACCOUNT');
	let newPermissionType = $state<'CUSTOMER_MANAGED' | 'SERVICE_MANAGED'>('CUSTOMER_MANAGED');
	let newAuthAwsSso = $state(true);
	let newAuthSaml = $state(false);
	let newWorkspaceRoleArn = $state('');

	function openCreateWorkspaceModal(): void {
		createWorkspaceError = null;
		newWorkspaceName = '';
		newWorkspaceDescription = '';
		newAccountAccessType = 'CURRENT_ACCOUNT';
		newPermissionType = 'CUSTOMER_MANAGED';
		newAuthAwsSso = true;
		newAuthSaml = false;
		newWorkspaceRoleArn = '';
		createWorkspaceModal?.open();
	}

	async function submitCreateWorkspace(): Promise<void> {
		const authenticationProviders: ('AWS_SSO' | 'SAML')[] = [
			...(newAuthAwsSso ? (['AWS_SSO'] as const) : []),
			...(newAuthSaml ? (['SAML'] as const) : [])
		];
		if (authenticationProviders.length === 0) {
			createWorkspaceError = 'Select at least one authentication provider.';
			return;
		}
		creatingWorkspace = true;
		createWorkspaceError = null;
		try {
			await client().send(
				new CreateWorkspaceCommand({
					accountAccessType: newAccountAccessType,
					permissionType: newPermissionType,
					authenticationProviders,
					workspaceName: newWorkspaceName.trim() || undefined,
					workspaceDescription: newWorkspaceDescription.trim() || undefined,
					workspaceRoleArn: newWorkspaceRoleArn.trim() || undefined
				})
			);
			toast.success('Workspace created');
			createWorkspaceModal?.close();
			await tabLoader.refresh('workspaces');
		} catch (e) {
			const msg = describeError(e);
			createWorkspaceError = msg;
			toast.error(msg);
		} finally {
			creatingWorkspace = false;
		}
	}

	async function handleDeleteWorkspace(w: WorkspaceSummary): Promise<void> {
		if (!w.id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete workspace',
			message: `Delete workspace "${w.name ?? w.id}"? This also deletes its API keys, service accounts, tokens, and permissions.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteWorkspaceCommand({ workspaceId: w.id }));
			toast.success('Workspace deleted');
			if (selectedWorkspaceId === w.id) selectedWorkspaceId = '';
			await tabLoader.refresh('workspaces');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let editWorkspaceModal = $state<Modal | null>(null);
	let editingWorkspace = $state(false);
	let editWorkspaceError = $state<string | null>(null);
	let editWorkspaceId = $state('');
	let editWorkspaceName = $state('');
	let editWorkspaceDescription = $state('');
	let editAccountAccessType = $state<'CURRENT_ACCOUNT' | 'ORGANIZATION'>('CURRENT_ACCOUNT');
	let editPermissionType = $state<'CUSTOMER_MANAGED' | 'SERVICE_MANAGED'>('CUSTOMER_MANAGED');
	let editWorkspaceRoleArn = $state('');

	function openEditWorkspaceModal(w: WorkspaceSummary): void {
		if (!w.id) return;
		editWorkspaceError = null;
		editWorkspaceId = w.id;
		editWorkspaceName = w.name ?? '';
		editWorkspaceDescription = '';
		editAccountAccessType = 'CURRENT_ACCOUNT';
		editPermissionType = 'CUSTOMER_MANAGED';
		editWorkspaceRoleArn = '';
		editWorkspaceModal?.open();
	}

	async function submitEditWorkspace(): Promise<void> {
		if (!editWorkspaceId) return;
		editingWorkspace = true;
		editWorkspaceError = null;
		try {
			await client().send(
				new UpdateWorkspaceCommand({
					workspaceId: editWorkspaceId,
					workspaceName: editWorkspaceName.trim() || undefined,
					workspaceDescription: editWorkspaceDescription.trim() || undefined,
					accountAccessType: editAccountAccessType,
					permissionType: editPermissionType,
					workspaceRoleArn: editWorkspaceRoleArn.trim() || undefined
				})
			);
			toast.success('Workspace updated');
			editWorkspaceModal?.close();
			await tabLoader.refresh('workspaces');
		} catch (e) {
			const msg = describeError(e);
			editWorkspaceError = msg;
			toast.error(msg);
		} finally {
			editingWorkspace = false;
		}
	}

	// Workspace detail: base description plus the non-listable sub-resources
	// (authentication, configuration, license, API keys, tags) that this
	// service scopes to a single workspace instead of exposing as their own
	// list operations.
	let workspaceDetailModal = $state<Modal | null>(null);
	let viewedWorkspace = $state<WorkspaceDescription | null>(null);
	let workspaceDetailLoading = $state(false);
	let workspaceDetailError = $state<string | null>(null);
	let workspaceDetailId = $state('');
	let workspaceTags = $state<Record<string, string>>({});

	// This service's WorkspaceDescription has no `arn` field (confirmed
	// against the installed SDK model) -- TagResource/UntagResource/
	// ListTagsForResource need one anyway, so it is built client-side from
	// the documented shape (services/grafana/PARITY.md: "arn:{partition}:
	// grafana:{region}:{account}:/workspaces/{id}", note the ARN's resource
	// segment itself starts with a literal "/"), using this repo's standard
	// local-emulator account ID (000000000000, the same convention already
	// used directly in this dashboard, e.g. dax/+page.svelte's IamRoleArn).
	function workspaceArn(id: string): string {
		return `arn:aws:grafana:${currentRegion()}:000000000000:/workspaces/${id}`;
	}

	async function openWorkspaceDetail(w: WorkspaceSummary): Promise<void> {
		if (!w.id) return;
		workspaceDetailId = w.id;
		viewedWorkspace = null;
		workspaceDetailError = null;
		workspaceTags = {};
		workspaceDetailModal?.open();
		workspaceDetailLoading = true;
		try {
			const resp = await client().send(new DescribeWorkspaceCommand({ workspaceId: w.id }));
			viewedWorkspace = resp.workspace ?? null;
			await refreshWorkspaceTags();
		} catch (e) {
			workspaceDetailError = describeError(e);
		} finally {
			workspaceDetailLoading = false;
		}
	}

	async function refreshWorkspaceTags(): Promise<void> {
		if (!workspaceDetailId) return;
		try {
			const resp = await client().send(
				new ListTagsForResourceCommand({ resourceArn: workspaceArn(workspaceDetailId) })
			);
			workspaceTags = resp.tags ?? {};
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Authentication (workspace-scoped, no list op) ---

	let authModal = $state<Modal | null>(null);
	let savingAuth = $state(false);
	let authError = $state<string | null>(null);
	let authAwsSso = $state(false);
	let authSaml = $state(false);

	function openAuthModal(): void {
		if (!viewedWorkspace) return;
		authError = null;
		const providers = viewedWorkspace.authentication?.providers ?? [];
		authAwsSso = providers.includes('AWS_SSO');
		authSaml = providers.includes('SAML');
		authModal?.open();
	}

	async function submitAuth(): Promise<void> {
		if (!workspaceDetailId) return;
		const authenticationProviders: ('AWS_SSO' | 'SAML')[] = [
			...(authAwsSso ? (['AWS_SSO'] as const) : []),
			...(authSaml ? (['SAML'] as const) : [])
		];
		if (authenticationProviders.length === 0) {
			authError = 'Select at least one authentication provider.';
			return;
		}
		savingAuth = true;
		authError = null;
		try {
			await client().send(
				new UpdateWorkspaceAuthenticationCommand({
					workspaceId: workspaceDetailId,
					authenticationProviders
				})
			);
			toast.success('Authentication updated');
			authModal?.close();
			const resp = await client().send(
				new DescribeWorkspaceCommand({ workspaceId: workspaceDetailId })
			);
			viewedWorkspace = resp.workspace ?? viewedWorkspace;
		} catch (e) {
			const msg = describeError(e);
			authError = msg;
			toast.error(msg);
		} finally {
			savingAuth = false;
		}
	}

	// --- Configuration (workspace-scoped, no list op) ---

	let configModal = $state<Modal | null>(null);
	let savingConfig = $state(false);
	let configError = $state<string | null>(null);
	let configText = $state('{}');
	let configGrafanaVersion = $state('');
	let loadingConfig = $state(false);

	async function openConfigModal(): Promise<void> {
		if (!workspaceDetailId) return;
		configError = null;
		configGrafanaVersion = '';
		configModal?.open();
		loadingConfig = true;
		try {
			const resp = await client().send(
				new DescribeWorkspaceConfigurationCommand({ workspaceId: workspaceDetailId })
			);
			configText =
				typeof resp.configuration === 'string'
					? resp.configuration
					: JSON.stringify(resp.configuration ?? {}, null, 2);
		} catch (e) {
			configError = describeError(e);
		} finally {
			loadingConfig = false;
		}
	}

	async function submitConfig(): Promise<void> {
		if (!workspaceDetailId) return;
		savingConfig = true;
		configError = null;
		try {
			await client().send(
				new UpdateWorkspaceConfigurationCommand({
					workspaceId: workspaceDetailId,
					configuration: configText,
					grafanaVersion: configGrafanaVersion.trim() || undefined
				})
			);
			toast.success('Configuration updated');
			configModal?.close();
		} catch (e) {
			const msg = describeError(e);
			configError = msg;
			toast.error(msg);
		} finally {
			savingConfig = false;
		}
	}

	// --- License (workspace-scoped, no list op) ---

	let licenseModal = $state<Modal | null>(null);
	let savingLicense = $state(false);
	let licenseError = $state<string | null>(null);
	let licenseGrafanaToken = $state('');

	function openLicenseModal(): void {
		licenseError = null;
		licenseGrafanaToken = '';
		licenseModal?.open();
	}

	async function submitAssociateLicense(): Promise<void> {
		if (!workspaceDetailId) return;
		savingLicense = true;
		licenseError = null;
		try {
			await client().send(
				new AssociateLicenseCommand({
					workspaceId: workspaceDetailId,
					licenseType: 'ENTERPRISE',
					grafanaToken: licenseGrafanaToken.trim() || undefined
				})
			);
			toast.success('License associated');
			licenseModal?.close();
			const resp = await client().send(
				new DescribeWorkspaceCommand({ workspaceId: workspaceDetailId })
			);
			viewedWorkspace = resp.workspace ?? viewedWorkspace;
		} catch (e) {
			const msg = describeError(e);
			licenseError = msg;
			toast.error(msg);
		} finally {
			savingLicense = false;
		}
	}

	async function handleDisassociateLicense(): Promise<void> {
		if (!workspaceDetailId || !viewedWorkspace?.licenseType) return;
		const confirmed = await confirmDestructive({
			title: 'Remove license',
			message: 'Remove the Grafana Enterprise license from this workspace?'
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DisassociateLicenseCommand({
					workspaceId: workspaceDetailId,
					licenseType: viewedWorkspace.licenseType
				})
			);
			toast.success('License removed');
			const resp = await client().send(
				new DescribeWorkspaceCommand({ workspaceId: workspaceDetailId })
			);
			viewedWorkspace = resp.workspace ?? viewedWorkspace;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- API Keys (workspace-scoped; the real API has no ListApiKeys
	// operation at all -- create and delete only, confirmed against the
	// installed SDK's 25-operation surface. Deleting requires typing the key
	// name since there is nothing to list. ---

	let apiKeyModal = $state<Modal | null>(null);
	let creatingApiKey = $state(false);
	let apiKeyError = $state<string | null>(null);
	let newApiKeyName = $state('');
	let newApiKeyRole = $state<'ADMIN' | 'EDITOR' | 'VIEWER'>('VIEWER');
	let newApiKeySeconds = $state(3600);
	let createdApiKey = $state<string | null>(null);
	let deleteApiKeyName = $state('');

	function openApiKeyModal(): void {
		apiKeyError = null;
		newApiKeyName = '';
		newApiKeyRole = 'VIEWER';
		newApiKeySeconds = 3600;
		createdApiKey = null;
		apiKeyModal?.open();
	}

	async function submitCreateApiKey(): Promise<void> {
		if (!workspaceDetailId || !newApiKeyName.trim()) {
			apiKeyError = 'Key name is required.';
			return;
		}
		creatingApiKey = true;
		apiKeyError = null;
		try {
			const resp = await client().send(
				new CreateWorkspaceApiKeyCommand({
					workspaceId: workspaceDetailId,
					keyName: newApiKeyName.trim(),
					keyRole: newApiKeyRole,
					secondsToLive: newApiKeySeconds
				})
			);
			createdApiKey = resp.key ?? null;
			toast.success('API key created -- copy it now, it will not be shown again');
		} catch (e) {
			const msg = describeError(e);
			apiKeyError = msg;
			toast.error(msg);
		} finally {
			creatingApiKey = false;
		}
	}

	async function submitDeleteApiKey(): Promise<void> {
		if (!workspaceDetailId || !deleteApiKeyName.trim()) return;
		const confirmed = await confirmDestructive({
			title: 'Delete API key',
			message: `Delete API key "${deleteApiKeyName.trim()}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteWorkspaceApiKeyCommand({
					workspaceId: workspaceDetailId,
					keyName: deleteApiKeyName.trim()
				})
			);
			toast.success('API key deleted');
			deleteApiKeyName = '';
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Tags (workspace-scoped) ---

	let addTagKey = $state('');
	let addTagValue = $state('');

	async function submitAddTag(): Promise<void> {
		if (!workspaceDetailId || !addTagKey.trim()) return;
		try {
			await client().send(
				new TagResourceCommand({
					resourceArn: workspaceArn(workspaceDetailId),
					tags: { [addTagKey.trim()]: addTagValue }
				})
			);
			toast.success('Tag added');
			addTagKey = '';
			addTagValue = '';
			await refreshWorkspaceTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removeTag(key: string): Promise<void> {
		if (!workspaceDetailId) return;
		try {
			await client().send(
				new UntagResourceCommand({ resourceArn: workspaceArn(workspaceDetailId), tagKeys: [key] })
			);
			toast.success('Tag removed');
			await refreshWorkspaceTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Service Accounts: create / delete ---

	let createSaModal = $state<Modal | null>(null);
	let creatingSa = $state(false);
	let createSaError = $state<string | null>(null);
	let newSaName = $state('');
	let newSaRole = $state<'ADMIN' | 'EDITOR' | 'VIEWER'>('VIEWER');

	function openCreateSaModal(): void {
		createSaError = selectedWorkspaceId ? null : 'Select a workspace first.';
		newSaName = '';
		newSaRole = 'VIEWER';
		createSaModal?.open();
	}

	async function submitCreateSa(): Promise<void> {
		if (!selectedWorkspaceId) {
			createSaError = 'Select a workspace first.';
			return;
		}
		if (!newSaName.trim()) {
			createSaError = 'Name is required.';
			return;
		}
		creatingSa = true;
		createSaError = null;
		try {
			await client().send(
				new CreateWorkspaceServiceAccountCommand({
					workspaceId: selectedWorkspaceId,
					name: newSaName.trim(),
					grafanaRole: newSaRole
				})
			);
			toast.success('Service account created');
			createSaModal?.close();
			await tabLoader.refresh('serviceAccounts');
		} catch (e) {
			const msg = describeError(e);
			createSaError = msg;
			toast.error(msg);
		} finally {
			creatingSa = false;
		}
	}

	async function handleDeleteSa(sa: ServiceAccountSummary): Promise<void> {
		if (!sa.id || !selectedWorkspaceId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete service account',
			message: `Delete service account "${sa.name}"? This also deletes its tokens.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteWorkspaceServiceAccountCommand({
					workspaceId: selectedWorkspaceId,
					serviceAccountId: sa.id
				})
			);
			toast.success('Service account deleted');
			if (selectedServiceAccountId === sa.id) selectedServiceAccountId = '';
			await tabLoader.refresh('serviceAccounts');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Tokens: create / delete ---

	let createTokenModal = $state<Modal | null>(null);
	let creatingToken = $state(false);
	let createTokenError = $state<string | null>(null);
	let newTokenName = $state('');
	let newTokenSeconds = $state(3600);
	let createdTokenKey = $state<string | null>(null);

	function openCreateTokenModal(): void {
		createTokenError =
			selectedWorkspaceId && selectedServiceAccountId
				? null
				: 'Select a workspace and service account first.';
		newTokenName = '';
		newTokenSeconds = 3600;
		createdTokenKey = null;
		createTokenModal?.open();
	}

	async function submitCreateToken(): Promise<void> {
		if (!selectedWorkspaceId || !selectedServiceAccountId) {
			createTokenError = 'Select a workspace and service account first.';
			return;
		}
		if (!newTokenName.trim()) {
			createTokenError = 'Name is required.';
			return;
		}
		creatingToken = true;
		createTokenError = null;
		try {
			const resp = await client().send(
				new CreateWorkspaceServiceAccountTokenCommand({
					workspaceId: selectedWorkspaceId,
					serviceAccountId: selectedServiceAccountId,
					name: newTokenName.trim(),
					secondsToLive: newTokenSeconds
				})
			);
			createdTokenKey = resp.serviceAccountToken?.key ?? null;
			toast.success('Token created -- copy it now, it will not be shown again');
			await tabLoader.refresh('tokens');
		} catch (e) {
			const msg = describeError(e);
			createTokenError = msg;
			toast.error(msg);
		} finally {
			creatingToken = false;
		}
	}

	async function handleDeleteToken(t: ServiceAccountTokenSummary): Promise<void> {
		if (!t.id || !selectedWorkspaceId || !selectedServiceAccountId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete token',
			message: `Delete token "${t.name}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteWorkspaceServiceAccountTokenCommand({
					workspaceId: selectedWorkspaceId,
					serviceAccountId: selectedServiceAccountId,
					tokenId: t.id
				})
			);
			toast.success('Token deleted');
			await tabLoader.refresh('tokens');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Permissions: add / revoke (UpdatePermissions takes a batch; this UI
	// submits one instruction at a time, a defensible subset rather than a
	// full batch-editor) ---

	let permissionModal = $state<Modal | null>(null);
	let savingPermission = $state(false);
	let permissionError = $state<string | null>(null);
	let permUserId = $state('');
	let permUserType = $state<'SSO_USER' | 'SSO_GROUP'>('SSO_USER');
	let permRole = $state<'ADMIN' | 'EDITOR' | 'VIEWER'>('VIEWER');
	let permAction = $state<'ADD' | 'REVOKE'>('ADD');

	function openPermissionModal(): void {
		permissionError = selectedWorkspaceId ? null : 'Select a workspace first.';
		permUserId = '';
		permUserType = 'SSO_USER';
		permRole = 'VIEWER';
		permAction = 'ADD';
		permissionModal?.open();
	}

	async function submitPermission(): Promise<void> {
		if (!selectedWorkspaceId) {
			permissionError = 'Select a workspace first.';
			return;
		}
		if (!permUserId.trim()) {
			permissionError = 'User/group ID is required.';
			return;
		}
		savingPermission = true;
		permissionError = null;
		try {
			const resp = await client().send(
				new UpdatePermissionsCommand({
					workspaceId: selectedWorkspaceId,
					updateInstructionBatch: [
						{
							action: permAction,
							role: permRole,
							users: [{ id: permUserId.trim(), type: permUserType }]
						}
					]
				})
			);
			if (resp.errors && resp.errors.length > 0) {
				const msg = resp.errors.map((e) => e.message).join('; ');
				permissionError = msg;
				toast.error(msg);
				return;
			}
			toast.success(permAction === 'ADD' ? 'Permission granted' : 'Permission revoked');
			permissionModal?.close();
			await tabLoader.refresh('permissions');
		} catch (e) {
			const msg = describeError(e);
			permissionError = msg;
			toast.error(msg);
		} finally {
			savingPermission = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={LineChart}
		title="Amazon Managed Grafana"
		description="Managed dashboards and visualization powered by Grafana"
		onRefresh={handleRefresh}
		color="orange"
		service="grafana"
	>
		{#snippet actions()}
			{#if activeTab === 'workspaces'}
				<button
					onclick={openCreateWorkspaceModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create workspace
				</button>
			{:else if activeTab === 'serviceAccounts'}
				<button
					onclick={openCreateSaModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create service account
				</button>
			{:else if activeTab === 'tokens'}
				<button
					onclick={openCreateTokenModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create token
				</button>
			{:else if activeTab === 'permissions'}
				<button
					onclick={openPermissionModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Update permission
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="orange" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if workspaceScopedTabs.includes(activeTab)}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="workspace-select" class="text-sm text-gray-500 dark:text-gray-400"
						>Workspace</label
					>
					<select
						id="workspace-select"
						value={selectedWorkspaceId}
						onchange={(e) => onWorkspaceSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if workspaces.length === 0}
							<option value="">No workspaces</option>
						{/if}
						{#each workspaces as w (w.id)}
							<option value={w.id}>{w.name || w.id} ({w.status})</option>
						{/each}
					</select>
					{#if activeTab === 'tokens'}
						<label for="sa-select" class="text-sm text-gray-500 dark:text-gray-400"
							>Service Account</label
						>
						<select
							id="sa-select"
							value={selectedServiceAccountId}
							onchange={(e) => onServiceAccountSelect((e.target as HTMLSelectElement).value)}
							class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
						>
							{#if serviceAccounts.length === 0}
								<option value="">No service accounts</option>
							{/if}
							{#each serviceAccounts as sa (sa.id)}
								<option value={sa.id}>{sa.name}</option>
							{/each}
						</select>
					{/if}
				</div>
			{/if}

			{#if activeTab === 'versions'}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="versions-workspace-filter" class="text-sm text-gray-500 dark:text-gray-400"
						>Upgrade options for workspace ID (optional)</label
					>
					<input
						id="versions-workspace-filter"
						bind:value={versionsWorkspaceFilter}
						placeholder="g-abc12345"
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
					<button
						onclick={() => tabLoader.refresh('versions')}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-600 dark:text-gray-300"
						>Load</button
					>
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

			{#if activeTab === 'workspaces'}
				{#snippet wsStatusCell(w: WorkspaceSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(w.status === 'ACTIVE')}"
						>{w.status ?? '—'}</span
					>
				{/snippet}
				{#snippet wsCreatedCell(w: WorkspaceSummary)}
					{formatDate(w.created)}
				{/snippet}
				{#snippet wsActionsCell(w: WorkspaceSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openWorkspaceDetail(w)}
							title="View"
							aria-label="View workspace {w.name}"
							class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditWorkspaceModal(w)}
							title="Edit"
							aria-label="Edit workspace {w.name}"
							class="text-gray-400 hover:text-orange-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteWorkspace(w)}
							title="Delete"
							aria-label="Delete workspace {w.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const wsColumns = defineColumns<WorkspaceSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'id', label: 'ID' },
					{ key: 'status', label: 'Status', render: wsStatusCell },
					{ key: 'grafanaVersion', label: 'Version' },
					{ key: 'endpoint', label: 'Endpoint' },
					{ key: 'created', label: 'Created', render: wsCreatedCell },
					{ key: 'actions', label: '', render: wsActionsCell }
				])}
				<DataTable
					rows={filteredWorkspaces}
					rowKey={(w) => w.id ?? ''}
					columns={wsColumns}
					loading={tabLoader.isLoading('workspaces')}
					emptyMessage="No workspaces found"
				/>
				<LoadMore
					hasMore={!!workspacesNextToken}
					loading={loadingMoreWorkspaces}
					onLoadMore={loadMoreWorkspaces}
				/>
			{:else if activeTab === 'serviceAccounts'}
				{#snippet saDisabledCell(sa: ServiceAccountSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(sa.isDisabled !== 'true')}"
						>{sa.isDisabled === 'true' ? 'Disabled' : 'Enabled'}</span
					>
				{/snippet}
				{#snippet saActionsCell(sa: ServiceAccountSummary)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => handleDeleteSa(sa)}
							title="Delete"
							aria-label="Delete service account {sa.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const saColumns = defineColumns<ServiceAccountSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'id', label: 'ID' },
					{ key: 'grafanaRole', label: 'Role' },
					{ key: 'isDisabled', label: 'Status', render: saDisabledCell },
					{ key: 'actions', label: '', render: saActionsCell }
				])}
				<DataTable
					rows={filteredServiceAccounts}
					rowKey={(sa) => sa.id ?? ''}
					columns={saColumns}
					loading={tabLoader.isLoading('serviceAccounts')}
					emptyMessage={selectedWorkspaceId
						? 'No service accounts found'
						: 'Select a workspace to see its service accounts'}
				/>
				<LoadMore
					hasMore={!!serviceAccountsNextToken}
					loading={loadingMoreServiceAccounts}
					onLoadMore={loadMoreServiceAccounts}
				/>
			{:else if activeTab === 'tokens'}
				{#snippet tokenCreatedCell(t: ServiceAccountTokenSummary)}
					{formatDate(t.createdAt)}
				{/snippet}
				{#snippet tokenExpiresCell(t: ServiceAccountTokenSummary)}
					{formatDate(t.expiresAt)}
				{/snippet}
				{#snippet tokenActionsCell(t: ServiceAccountTokenSummary)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => handleDeleteToken(t)}
							title="Delete"
							aria-label="Delete token {t.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const tokenColumns = defineColumns<ServiceAccountTokenSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'id', label: 'ID' },
					{ key: 'createdAt', label: 'Created', render: tokenCreatedCell },
					{ key: 'expiresAt', label: 'Expires', render: tokenExpiresCell },
					{ key: 'actions', label: '', render: tokenActionsCell }
				])}
				<DataTable
					rows={filteredTokens}
					rowKey={(t) => t.id ?? ''}
					columns={tokenColumns}
					loading={tabLoader.isLoading('tokens')}
					emptyMessage={selectedServiceAccountId
						? 'No tokens found'
						: 'Select a workspace and service account to see its tokens'}
				/>
				<LoadMore
					hasMore={!!tokensNextToken}
					loading={loadingMoreTokens}
					onLoadMore={loadMoreTokens}
				/>
			{:else if activeTab === 'permissions'}
				{#snippet permUserCell(p: PermissionEntry)}
					<span class="font-mono text-xs">{p.user?.id}</span>
				{/snippet}
				{#snippet permTypeCell(p: PermissionEntry)}
					{p.user?.type}
				{/snippet}
				{@const permColumns = defineColumns<PermissionEntry>([
					{ key: 'user', label: 'User/Group ID', render: permUserCell },
					{ key: 'type', label: 'Type', render: permTypeCell },
					{ key: 'role', label: 'Role' }
				])}
				<DataTable
					rows={filteredPermissions}
					rowKey={(p) => `${p.user?.type ?? ''}:${p.user?.id ?? ''}`}
					columns={permColumns}
					loading={tabLoader.isLoading('permissions')}
					emptyMessage={selectedWorkspaceId
						? 'No permissions found'
						: 'Select a workspace to see its permissions'}
				/>
				<LoadMore
					hasMore={!!permissionsNextToken}
					loading={loadingMorePermissions}
					onLoadMore={loadMorePermissions}
				/>
			{:else if activeTab === 'versions'}
				{@const versionColumns = defineColumns<{ version: string }>([
					{ key: 'version', label: 'Grafana Version' }
				])}
				<DataTable
					rows={filteredVersions}
					rowKey={(v) => v.version}
					columns={versionColumns}
					loading={tabLoader.isLoading('versions')}
					emptyMessage="No versions found"
				/>
				<LoadMore
					hasMore={!!versionsNextToken}
					loading={loadingMoreVersions}
					onLoadMore={loadMoreVersions}
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createWorkspaceModal} title="Create Workspace">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-ws-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-ws-name"
					bind:value={newWorkspaceName}
					placeholder="my-workspace"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-ws-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="new-ws-desc"
					bind:value={newWorkspaceDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-ws-access" class="text-sm text-slate-600 dark:text-slate-300"
					>Account Access</label
				>
				<select
					id="new-ws-access"
					bind:value={newAccountAccessType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="CURRENT_ACCOUNT">Current Account</option>
					<option value="ORGANIZATION">Organization</option>
				</select>
			</div>
			<div>
				<label for="new-ws-perm" class="text-sm text-slate-600 dark:text-slate-300"
					>Permission Type</label
				>
				<select
					id="new-ws-perm"
					bind:value={newPermissionType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="CUSTOMER_MANAGED">Customer Managed</option>
					<option value="SERVICE_MANAGED">Service Managed</option>
				</select>
			</div>
			<div>
				<label for="new-ws-role-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>Workspace Role ARN (optional)</label
				>
				<input
					id="new-ws-role-arn"
					bind:value={newWorkspaceRoleArn}
					placeholder="arn:aws:iam::000000000000:role/grafana-role"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<fieldset>
				<legend class="text-sm text-slate-600 dark:text-slate-300">Authentication providers</legend>
				<div class="flex items-center gap-4 mt-1">
					<label class="flex items-center gap-2 text-sm">
						<input type="checkbox" bind:checked={newAuthAwsSso} /> IAM Identity Center
					</label>
					<label class="flex items-center gap-2 text-sm">
						<input type="checkbox" bind:checked={newAuthSaml} /> SAML
					</label>
				</div>
			</fieldset>
			{#if createWorkspaceError}
				<p class="text-sm text-red-600 dark:text-red-400">{createWorkspaceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createWorkspaceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateWorkspace}
			disabled={creatingWorkspace}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50"
			>{creatingWorkspace ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editWorkspaceModal} title="Edit Workspace">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-ws-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="edit-ws-name"
					bind:value={editWorkspaceName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-ws-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="edit-ws-desc"
					bind:value={editWorkspaceDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-ws-access" class="text-sm text-slate-600 dark:text-slate-300"
					>Account Access</label
				>
				<select
					id="edit-ws-access"
					bind:value={editAccountAccessType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="CURRENT_ACCOUNT">Current Account</option>
					<option value="ORGANIZATION">Organization</option>
				</select>
			</div>
			<div>
				<label for="edit-ws-perm" class="text-sm text-slate-600 dark:text-slate-300"
					>Permission Type</label
				>
				<select
					id="edit-ws-perm"
					bind:value={editPermissionType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="CUSTOMER_MANAGED">Customer Managed</option>
					<option value="SERVICE_MANAGED">Service Managed</option>
				</select>
			</div>
			<div>
				<label for="edit-ws-role-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>Workspace Role ARN</label
				>
				<input
					id="edit-ws-role-arn"
					bind:value={editWorkspaceRoleArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editWorkspaceError}
				<p class="text-sm text-red-600 dark:text-red-400">{editWorkspaceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editWorkspaceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditWorkspace}
			disabled={editingWorkspace}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50"
			>{editingWorkspace ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={workspaceDetailModal} title="Workspace Detail">
	{#snippet children()}
		<div class="space-y-4 max-h-[70vh] overflow-y-auto">
			{#if workspaceDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if workspaceDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{workspaceDetailError}</p>
			{:else if viewedWorkspace}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">ID:</span> {viewedWorkspace.id}</div>
					<div><span class="text-slate-500">Status:</span> {viewedWorkspace.status}</div>
					<div><span class="text-slate-500">Version:</span> {viewedWorkspace.grafanaVersion}</div>
					<div><span class="text-slate-500">Endpoint:</span> {viewedWorkspace.endpoint}</div>
					<div>
						<span class="text-slate-500">Access:</span> {viewedWorkspace.accountAccessType}
					</div>
					<div>
						<span class="text-slate-500">Permission:</span> {viewedWorkspace.permissionType}
					</div>
					<div><span class="text-slate-500">Created:</span> {formatDate(viewedWorkspace.created)}</div>
					<div>
						<span class="text-slate-500">Modified:</span> {formatDate(viewedWorkspace.modified)}
					</div>
					{#if viewedWorkspace.workspaceRoleArn}
						<div class="col-span-2 break-all">
							<span class="text-slate-500">Role ARN:</span> {viewedWorkspace.workspaceRoleArn}
						</div>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<div class="flex items-center justify-between">
						<h3 class="text-sm font-semibold">Authentication</h3>
						<button onclick={openAuthModal} class="text-xs text-orange-600 hover:underline"
							>Edit</button
						>
					</div>
					<p class="text-xs text-slate-500 mt-1">
						Providers: {(viewedWorkspace.authentication?.providers ?? []).join(', ') || 'none'}
					</p>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<div class="flex items-center justify-between">
						<h3 class="text-sm font-semibold">Configuration</h3>
						<button onclick={openConfigModal} class="text-xs text-orange-600 hover:underline"
							>View / Edit</button
						>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<div class="flex items-center justify-between">
						<h3 class="text-sm font-semibold">License</h3>
						{#if viewedWorkspace.licenseType}
							<button
								onclick={handleDisassociateLicense}
								class="text-xs text-red-600 hover:underline">Remove</button
							>
						{:else}
							<button onclick={openLicenseModal} class="text-xs text-orange-600 hover:underline"
								>Associate</button
							>
						{/if}
					</div>
					<p class="text-xs text-slate-500 mt-1">
						{viewedWorkspace.licenseType ?? 'No license associated'}
					</p>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<div class="flex items-center justify-between">
						<h3 class="text-sm font-semibold">API Keys</h3>
						<button onclick={openApiKeyModal} class="text-xs text-orange-600 hover:underline"
							>Manage</button
						>
					</div>
					<p class="text-xs text-slate-500 mt-1">
						No list operation exists for API keys on this API -- create or delete by name.
					</p>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<h3 class="text-sm font-semibold mb-2">Tags</h3>
					<div class="flex flex-wrap gap-2 mb-2">
						{#each Object.entries(workspaceTags) as [key, value] (key)}
							<span
								class="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-slate-100 dark:bg-slate-700"
							>
								{key}={value}
								<button
									onclick={() => removeTag(key)}
									aria-label="Remove tag {key}"
									class="text-slate-400 hover:text-red-500"><Ban class="w-3 h-3" /></button
								>
							</span>
						{:else}
							<span class="text-xs text-slate-500">No tags</span>
						{/each}
					</div>
					<div class="flex items-center gap-2">
						<label class="sr-only" for="tag-key-input">Tag key</label>
						<input
							id="tag-key-input"
							bind:value={addTagKey}
							placeholder="key"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="tag-value-input">Tag value</label>
						<input
							id="tag-value-input"
							bind:value={addTagValue}
							placeholder="value"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={submitAddTag}
							class="text-xs px-2 py-1 rounded bg-orange-600 text-white hover:bg-orange-700"
							>Add</button
						>
					</div>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => workspaceDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={authModal} title="Update Authentication">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-xs text-slate-500">
				Only provider selection is supported here; full SAML IdP metadata configuration is not
				exposed in this UI.
			</p>
			<label class="flex items-center gap-2 text-sm">
				<input type="checkbox" bind:checked={authAwsSso} /> IAM Identity Center
			</label>
			<label class="flex items-center gap-2 text-sm">
				<input type="checkbox" bind:checked={authSaml} /> SAML
			</label>
			{#if authError}
				<p class="text-sm text-red-600 dark:text-red-400">{authError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => authModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitAuth}
			disabled={savingAuth}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50"
			>{savingAuth ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={configModal} title="Workspace Configuration">
	{#snippet children()}
		<div class="space-y-3">
			{#if loadingConfig}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else}
				<div>
					<label for="config-textarea" class="text-sm text-slate-600 dark:text-slate-300"
						>Configuration JSON</label
					>
					<textarea
						id="config-textarea"
						bind:value={configText}
						rows="8"
						class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					></textarea>
				</div>
				<div>
					<label for="config-version" class="text-sm text-slate-600 dark:text-slate-300"
						>Upgrade Grafana version (optional, upgrade-only)</label
					>
					<input
						id="config-version"
						bind:value={configGrafanaVersion}
						placeholder="10.4"
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			{/if}
			{#if configError}
				<p class="text-sm text-red-600 dark:text-red-400">{configError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => configModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitConfig}
			disabled={savingConfig}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50"
			>{savingConfig ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={licenseModal} title="Associate License">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">License type: ENTERPRISE</p>
			<div>
				<label for="license-token" class="text-sm text-slate-600 dark:text-slate-300"
					>Grafana Labs Token (optional)</label
				>
				<input
					id="license-token"
					bind:value={licenseGrafanaToken}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if licenseError}
				<p class="text-sm text-red-600 dark:text-red-400">{licenseError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => licenseModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitAssociateLicense}
			disabled={savingLicense}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50"
			>{savingLicense ? 'Saving...' : 'Associate'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={apiKeyModal} title="API Keys">
	{#snippet children()}
		<div class="space-y-4">
			<div class="space-y-2">
				<h4 class="text-sm font-semibold">Create</h4>
				<label class="sr-only" for="new-api-key-name">Key name</label>
				<input
					id="new-api-key-name"
					bind:value={newApiKeyName}
					placeholder="key name"
					class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
				<div class="flex gap-2">
					<label class="sr-only" for="new-api-key-role">Key role</label>
					<select
						id="new-api-key-role"
						bind:value={newApiKeyRole}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="ADMIN">Admin</option>
						<option value="EDITOR">Editor</option>
						<option value="VIEWER">Viewer</option>
					</select>
					<label class="sr-only" for="new-api-key-seconds">Seconds to live</label>
					<input
						id="new-api-key-seconds"
						type="number"
						bind:value={newApiKeySeconds}
						class="w-32 px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
					<button
						onclick={submitCreateApiKey}
						disabled={creatingApiKey}
						class="px-3 py-2 text-sm rounded-lg bg-orange-600 text-white hover:bg-orange-700 disabled:opacity-50"
						>{creatingApiKey ? 'Creating...' : 'Create'}</button
					>
				</div>
				{#if createdApiKey}
					<p class="text-xs font-mono break-all bg-slate-100 dark:bg-slate-700 rounded p-2">
						{createdApiKey}
					</p>
				{/if}
				{#if apiKeyError}
					<p class="text-sm text-red-600 dark:text-red-400">{apiKeyError}</p>
				{/if}
			</div>
			<div class="space-y-2 border-t border-slate-200 dark:border-slate-700 pt-3">
				<h4 class="text-sm font-semibold">Delete</h4>
				<div class="flex gap-2">
					<label class="sr-only" for="delete-api-key-name">Key name to delete</label>
					<input
						id="delete-api-key-name"
						bind:value={deleteApiKeyName}
						placeholder="key name"
						class="flex-1 px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
					<button
						onclick={submitDeleteApiKey}
						class="px-3 py-2 text-sm rounded-lg border border-red-300 text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20"
						>Delete</button
					>
				</div>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => apiKeyModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createSaModal} title="Create Service Account">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-sa-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-sa-name"
					bind:value={newSaName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-sa-role" class="text-sm text-slate-600 dark:text-slate-300">Role</label>
				<select
					id="new-sa-role"
					bind:value={newSaRole}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ADMIN">Admin</option>
					<option value="EDITOR">Editor</option>
					<option value="VIEWER">Viewer</option>
				</select>
			</div>
			{#if createSaError}
				<p class="text-sm text-red-600 dark:text-red-400">{createSaError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createSaModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateSa}
			disabled={creatingSa}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50"
			>{creatingSa ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createTokenModal} title="Create Token">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-token-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-token-name"
					bind:value={newTokenName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-token-seconds" class="text-sm text-slate-600 dark:text-slate-300"
					>Seconds to live (max 30 days)</label
				>
				<input
					id="new-token-seconds"
					type="number"
					bind:value={newTokenSeconds}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createdTokenKey}
				<p class="text-xs font-mono break-all bg-slate-100 dark:bg-slate-700 rounded p-2">
					{createdTokenKey}
				</p>
			{/if}
			{#if createTokenError}
				<p class="text-sm text-red-600 dark:text-red-400">{createTokenError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createTokenModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
		<button
			type="button"
			onclick={submitCreateToken}
			disabled={creatingToken}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50"
			>{creatingToken ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={permissionModal} title="Update Permission">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="perm-action" class="text-sm text-slate-600 dark:text-slate-300">Action</label>
				<select
					id="perm-action"
					bind:value={permAction}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ADD">Add</option>
					<option value="REVOKE">Revoke</option>
				</select>
			</div>
			<div>
				<label for="perm-user-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select
					id="perm-user-type"
					bind:value={permUserType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SSO_USER">SSO User</option>
					<option value="SSO_GROUP">SSO Group</option>
				</select>
			</div>
			<div>
				<label for="perm-user-id" class="text-sm text-slate-600 dark:text-slate-300"
					>User/Group ID</label
				>
				<input
					id="perm-user-id"
					bind:value={permUserId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="perm-role" class="text-sm text-slate-600 dark:text-slate-300">Role</label>
				<select
					id="perm-role"
					bind:value={permRole}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ADMIN">Admin</option>
					<option value="EDITOR">Editor</option>
					<option value="VIEWER">Viewer</option>
				</select>
			</div>
			{#if permissionError}
				<p class="text-sm text-red-600 dark:text-red-400">{permissionError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => permissionModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitPermission}
			disabled={savingPermission}
			class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-medium text-white hover:bg-orange-700 disabled:opacity-50"
			>{savingPermission ? 'Saving...' : 'Submit'}</button
		>
	{/snippet}
</Modal>
