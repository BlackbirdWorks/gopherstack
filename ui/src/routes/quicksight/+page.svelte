<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getQuickSightClient } from '$lib/aws-client';
	import {
		ListDashboardsCommand,
		CreateDashboardCommand,
		UpdateDashboardCommand,
		DeleteDashboardCommand,
		DescribeDashboardCommand,
		ListAnalysesCommand,
		CreateAnalysisCommand,
		UpdateAnalysisCommand,
		DeleteAnalysisCommand,
		DescribeAnalysisCommand,
		ListDataSetsCommand,
		CreateDataSetCommand,
		UpdateDataSetCommand,
		DeleteDataSetCommand,
		DescribeDataSetCommand,
		ListDataSourcesCommand,
		CreateDataSourceCommand,
		UpdateDataSourceCommand,
		DeleteDataSourceCommand,
		DescribeDataSourceCommand,
		ListFoldersCommand,
		CreateFolderCommand,
		UpdateFolderCommand,
		DeleteFolderCommand,
		DescribeFolderCommand,
		ListVPCConnectionsCommand,
		CreateVPCConnectionCommand,
		UpdateVPCConnectionCommand,
		DeleteVPCConnectionCommand,
		DescribeVPCConnectionCommand,
		ListTemplatesCommand,
		CreateTemplateCommand,
		UpdateTemplateCommand,
		DeleteTemplateCommand,
		DescribeTemplateCommand,
		ListThemesCommand,
		CreateThemeCommand,
		UpdateThemeCommand,
		DeleteThemeCommand,
		DescribeThemeCommand,
		ListTopicsCommand,
		CreateTopicCommand,
		UpdateTopicCommand,
		DeleteTopicCommand,
		DescribeTopicCommand,
		ListNamespacesCommand,
		CreateNamespaceCommand,
		DeleteNamespaceCommand,
		DescribeNamespaceCommand,
		ListGroupsCommand,
		CreateGroupCommand,
		UpdateGroupCommand,
		DeleteGroupCommand,
		DescribeGroupCommand,
		ListUsersCommand,
		RegisterUserCommand,
		UpdateUserCommand,
		DeleteUserCommand,
		DescribeUserCommand,
		ListIAMPolicyAssignmentsCommand,
		CreateIAMPolicyAssignmentCommand,
		UpdateIAMPolicyAssignmentCommand,
		DeleteIAMPolicyAssignmentCommand,
		DescribeIAMPolicyAssignmentCommand,
		ListCustomPermissionsCommand,
		CreateCustomPermissionsCommand,
		UpdateCustomPermissionsCommand,
		DeleteCustomPermissionsCommand,
		DescribeCustomPermissionsCommand,
		ListBrandsCommand,
		CreateBrandCommand,
		UpdateBrandCommand,
		DeleteBrandCommand,
		DescribeBrandCommand,
		ListActionConnectorsCommand,
		CreateActionConnectorCommand,
		UpdateActionConnectorCommand,
		DeleteActionConnectorCommand,
		DescribeActionConnectorCommand,
		ListAgentsCommand,
		CreateAgentCommand,
		UpdateAgentCommand,
		DeleteAgentCommand,
		DescribeAgentCommand,
		ListKnowledgeBasesCommand,
		CreateKnowledgeBaseCommand,
		UpdateKnowledgeBaseCommand,
		DeleteKnowledgeBaseCommand,
		DescribeKnowledgeBaseCommand,
		ListSpacesCommand,
		CreateSpaceCommand,
		UpdateSpaceCommand,
		DeleteSpaceCommand,
		DescribeSpaceCommand,
		type DashboardSummary,
		type Dashboard,
		type DashboardVersionDefinition,
		type AnalysisSummary,
		type Analysis,
		type AnalysisDefinition,
		type DataSetSummary,
		type DataSet,
		type DataSetImportMode,
		type DataSource,
		type DataSourceType,
		type FolderSummary,
		type Folder,
		type FolderType,
		type SharingModel,
		type VPCConnectionSummary,
		type VPCConnection,
		type TemplateSummary,
		type Template,
		type TemplateVersionDefinition,
		type ThemeSummary,
		type Theme,
		type ThemeConfiguration,
		type TopicSummary,
		type TopicDetails,
		type NamespaceInfoV2,
		type IdentityStore,
		type Group,
		type User,
		type IdentityType,
		type UserRole,
		type IAMPolicyAssignmentSummary,
		type IAMPolicyAssignment,
		type AssignmentStatus,
		type CustomPermissions as CustomPermissionsDetail,
		type BrandSummary,
		type BrandDetail,
		type BrandDefinition,
		type ActionConnectorSummary,
		type ActionConnector,
		type ActionConnectorType,
		type AuthConfig,
		type AgentSummary,
		type Agent,
		type KnowledgeBaseSummary,
		type KnowledgeBase,
		type KnowledgeBaseConfiguration,
		type SpaceSummary,
		type SpaceDetails
	} from '@aws-sdk/client-quicksight';
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
	import { BarChart3, Plus, Trash2, Eye, Pencil } from 'lucide-svelte';

	const client = regionalClient(getQuickSightClient);

	type TabId =
		| 'dashboards'
		| 'analyses'
		| 'datasets'
		| 'datasources'
		| 'folders'
		| 'vpcConnections'
		| 'templates'
		| 'themes'
		| 'topics'
		| 'namespaces'
		| 'groups'
		| 'users'
		| 'iamPolicyAssignments'
		| 'customPermissions'
		| 'brands'
		| 'actionConnectors'
		| 'agents'
		| 'knowledgeBases'
		| 'spaces';

	const tabs: TabDef[] = [
		{ id: 'dashboards', label: 'Dashboards' },
		{ id: 'analyses', label: 'Analyses' },
		{ id: 'datasets', label: 'Data Sets' },
		{ id: 'datasources', label: 'Data Sources' },
		{ id: 'folders', label: 'Folders' },
		{ id: 'vpcConnections', label: 'VPC Connections' },
		{ id: 'templates', label: 'Templates' },
		{ id: 'themes', label: 'Themes' },
		{ id: 'topics', label: 'Topics' },
		{ id: 'namespaces', label: 'Namespaces' },
		{ id: 'groups', label: 'Groups' },
		{ id: 'users', label: 'Users' },
		{ id: 'iamPolicyAssignments', label: 'IAM Policy Assignments' },
		{ id: 'customPermissions', label: 'Custom Permissions' },
		{ id: 'brands', label: 'Brands' },
		{ id: 'actionConnectors', label: 'Action Connectors' },
		{ id: 'agents', label: 'Agents' },
		{ id: 'knowledgeBases', label: 'Knowledge Bases' },
		{ id: 'spaces', label: 'Spaces' }
	];

	// Group/User/IAMPolicyAssignment are the only families scoped by Namespace
	// (ListGroupsRequest/ListUsersRequest/ListIAMPolicyAssignmentsRequest all
	// require it) rather than just AwsAccountId. 'default' is the namespace
	// CreateNamespace and every other gopherstack page implicitly assumes when
	// none is specified; editable so a caller-created namespace is reachable.
	let namespace = $state('default');

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

	function parseCommaList(s: string): string[] {
		return s
			.split(',')
			.map((x) => x.trim())
			.filter((x) => x.length > 0);
	}

	let activeTab = $state<TabId>('dashboards');
	let searchQuery = $state('');

	// Every QuickSight call requires an AwsAccountId. Rather than hardcode a
	// placeholder, this is resolved once from the server's real effective
	// account id (GET /dashboard/api/system/settings, the same endpoint the
	// Settings page reads -- see settings/+page.svelte) so list/create/update
	// calls address the account this gopherstack instance actually runs as.
	// '000000000000' is kept only as the fallback for an older server build
	// that doesn't expose the endpoint yet, or when it's unreachable.
	let awsAccountId = $state('000000000000');
	let accountIdLoaded = false;

	async function ensureAccountId(): Promise<void> {
		if (accountIdLoaded) return;
		accountIdLoaded = true;
		try {
			const res = await fetch('/dashboard/api/system/settings');
			if (!res.ok) return;
			const data = (await res.json()) as { accountID?: unknown };
			if (typeof data.accountID === 'string' && data.accountID) {
				awsAccountId = data.accountID;
			}
		} catch {
			// Endpoint missing (older server build) or unreachable -- keep the
			// '000000000000' placeholder default.
		}
	}

	let dashboards = $state<DashboardSummary[]>([]);
	let dashboardsNextToken = $state<string | undefined>();
	let loadingMoreDashboards = $state(false);

	let analyses = $state<AnalysisSummary[]>([]);
	let analysesNextToken = $state<string | undefined>();
	let loadingMoreAnalyses = $state(false);

	let dataSets = $state<DataSetSummary[]>([]);
	let dataSetsNextToken = $state<string | undefined>();
	let loadingMoreDataSets = $state(false);

	let dataSources = $state<DataSource[]>([]);
	let dataSourcesNextToken = $state<string | undefined>();
	let loadingMoreDataSources = $state(false);

	let folders = $state<FolderSummary[]>([]);
	let foldersNextToken = $state<string | undefined>();
	let loadingMoreFolders = $state(false);

	let vpcConnections = $state<VPCConnectionSummary[]>([]);
	let vpcConnectionsNextToken = $state<string | undefined>();
	let loadingMoreVpcConnections = $state(false);

	let templates = $state<TemplateSummary[]>([]);
	let templatesNextToken = $state<string | undefined>();
	let loadingMoreTemplates = $state(false);

	let themes = $state<ThemeSummary[]>([]);
	let themesNextToken = $state<string | undefined>();
	let loadingMoreThemes = $state(false);

	let topics = $state<TopicSummary[]>([]);
	let topicsNextToken = $state<string | undefined>();
	let loadingMoreTopics = $state(false);

	let namespaces = $state<NamespaceInfoV2[]>([]);
	let namespacesNextToken = $state<string | undefined>();
	let loadingMoreNamespaces = $state(false);

	let groups = $state<Group[]>([]);
	let groupsNextToken = $state<string | undefined>();
	let loadingMoreGroups = $state(false);

	let users = $state<User[]>([]);
	let usersNextToken = $state<string | undefined>();
	let loadingMoreUsers = $state(false);

	let iamPolicyAssignments = $state<IAMPolicyAssignmentSummary[]>([]);
	let iamPolicyAssignmentsNextToken = $state<string | undefined>();
	let loadingMoreIamPolicyAssignments = $state(false);

	let customPermissions = $state<CustomPermissionsDetail[]>([]);
	let customPermissionsNextToken = $state<string | undefined>();
	let loadingMoreCustomPermissions = $state(false);

	let brands = $state<BrandSummary[]>([]);
	let brandsNextToken = $state<string | undefined>();
	let loadingMoreBrands = $state(false);

	let actionConnectors = $state<ActionConnectorSummary[]>([]);
	let actionConnectorsNextToken = $state<string | undefined>();
	let loadingMoreActionConnectors = $state(false);

	let agents = $state<AgentSummary[]>([]);
	let agentsNextToken = $state<string | undefined>();
	let loadingMoreAgents = $state(false);

	let knowledgeBases = $state<KnowledgeBaseSummary[]>([]);
	let knowledgeBasesNextToken = $state<string | undefined>();
	let loadingMoreKnowledgeBases = $state(false);

	let spaces = $state<SpaceSummary[]>([]);
	let spacesNextToken = $state<string | undefined>();
	let loadingMoreSpaces = $state(false);

	async function fetchDashboards(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListDashboardsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : dashboardsNextToken
			})
		);
		dashboards = reset
			? (resp.DashboardSummaryList ?? [])
			: [...dashboards, ...(resp.DashboardSummaryList ?? [])];
		dashboardsNextToken = resp.NextToken;
	}

	async function fetchAnalyses(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListAnalysesCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : analysesNextToken
			})
		);
		analyses = reset
			? (resp.AnalysisSummaryList ?? [])
			: [...analyses, ...(resp.AnalysisSummaryList ?? [])];
		analysesNextToken = resp.NextToken;
	}

	async function fetchDataSets(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListDataSetsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : dataSetsNextToken
			})
		);
		dataSets = reset ? (resp.DataSetSummaries ?? []) : [...dataSets, ...(resp.DataSetSummaries ?? [])];
		dataSetsNextToken = resp.NextToken;
	}

	async function fetchDataSources(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListDataSourcesCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : dataSourcesNextToken
			})
		);
		dataSources = reset
			? (resp.DataSources ?? [])
			: [...dataSources, ...(resp.DataSources ?? [])];
		dataSourcesNextToken = resp.NextToken;
	}

	async function fetchFolders(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListFoldersCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : foldersNextToken
			})
		);
		folders = reset ? (resp.FolderSummaryList ?? []) : [...folders, ...(resp.FolderSummaryList ?? [])];
		foldersNextToken = resp.NextToken;
	}

	async function fetchVpcConnections(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListVPCConnectionsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : vpcConnectionsNextToken
			})
		);
		vpcConnections = reset
			? (resp.VPCConnectionSummaries ?? [])
			: [...vpcConnections, ...(resp.VPCConnectionSummaries ?? [])];
		vpcConnectionsNextToken = resp.NextToken;
	}

	async function fetchTemplates(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListTemplatesCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : templatesNextToken
			})
		);
		templates = reset
			? (resp.TemplateSummaryList ?? [])
			: [...templates, ...(resp.TemplateSummaryList ?? [])];
		templatesNextToken = resp.NextToken;
	}

	async function fetchThemes(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListThemesCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : themesNextToken
			})
		);
		themes = reset ? (resp.ThemeSummaryList ?? []) : [...themes, ...(resp.ThemeSummaryList ?? [])];
		themesNextToken = resp.NextToken;
	}

	async function fetchTopics(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListTopicsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : topicsNextToken
			})
		);
		topics = reset ? (resp.TopicsSummaries ?? []) : [...topics, ...(resp.TopicsSummaries ?? [])];
		topicsNextToken = resp.NextToken;
	}

	async function fetchNamespaces(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListNamespacesCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : namespacesNextToken
			})
		);
		namespaces = reset ? (resp.Namespaces ?? []) : [...namespaces, ...(resp.Namespaces ?? [])];
		namespacesNextToken = resp.NextToken;
	}

	async function fetchGroups(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListGroupsCommand({
				AwsAccountId: awsAccountId,
				Namespace: namespace,
				NextToken: reset ? undefined : groupsNextToken
			})
		);
		groups = reset ? (resp.GroupList ?? []) : [...groups, ...(resp.GroupList ?? [])];
		groupsNextToken = resp.NextToken;
	}

	async function fetchUsers(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListUsersCommand({
				AwsAccountId: awsAccountId,
				Namespace: namespace,
				NextToken: reset ? undefined : usersNextToken
			})
		);
		users = reset ? (resp.UserList ?? []) : [...users, ...(resp.UserList ?? [])];
		usersNextToken = resp.NextToken;
	}

	async function fetchIamPolicyAssignments(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListIAMPolicyAssignmentsCommand({
				AwsAccountId: awsAccountId,
				Namespace: namespace,
				NextToken: reset ? undefined : iamPolicyAssignmentsNextToken
			})
		);
		iamPolicyAssignments = reset
			? (resp.IAMPolicyAssignments ?? [])
			: [...iamPolicyAssignments, ...(resp.IAMPolicyAssignments ?? [])];
		iamPolicyAssignmentsNextToken = resp.NextToken;
	}

	async function fetchCustomPermissions(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListCustomPermissionsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : customPermissionsNextToken
			})
		);
		customPermissions = reset
			? (resp.CustomPermissionsList ?? [])
			: [...customPermissions, ...(resp.CustomPermissionsList ?? [])];
		customPermissionsNextToken = resp.NextToken;
	}

	async function fetchBrands(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListBrandsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : brandsNextToken
			})
		);
		brands = reset ? (resp.Brands ?? []) : [...brands, ...(resp.Brands ?? [])];
		brandsNextToken = resp.NextToken;
	}

	async function fetchActionConnectors(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListActionConnectorsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : actionConnectorsNextToken
			})
		);
		actionConnectors = reset
			? (resp.ActionConnectorSummaries ?? [])
			: [...actionConnectors, ...(resp.ActionConnectorSummaries ?? [])];
		actionConnectorsNextToken = resp.NextToken;
	}

	async function fetchAgents(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListAgentsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : agentsNextToken
			})
		);
		agents = reset ? (resp.AgentSummaries ?? []) : [...agents, ...(resp.AgentSummaries ?? [])];
		agentsNextToken = resp.NextToken;
	}

	async function fetchKnowledgeBases(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListKnowledgeBasesCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : knowledgeBasesNextToken
			})
		);
		knowledgeBases = reset
			? (resp.KnowledgeBaseSummaries ?? [])
			: [...knowledgeBases, ...(resp.KnowledgeBaseSummaries ?? [])];
		knowledgeBasesNextToken = resp.NextToken;
	}

	async function fetchSpaces(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListSpacesCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : spacesNextToken
			})
		);
		spaces = reset ? (resp.SpaceSummaries ?? []) : [...spaces, ...(resp.SpaceSummaries ?? [])];
		spacesNextToken = resp.NextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		dashboards: () => fetchDashboards(true).catch(rethrowDescribed),
		analyses: () => fetchAnalyses(true).catch(rethrowDescribed),
		datasets: () => fetchDataSets(true).catch(rethrowDescribed),
		datasources: () => fetchDataSources(true).catch(rethrowDescribed),
		folders: () => fetchFolders(true).catch(rethrowDescribed),
		vpcConnections: () => fetchVpcConnections(true).catch(rethrowDescribed),
		templates: () => fetchTemplates(true).catch(rethrowDescribed),
		themes: () => fetchThemes(true).catch(rethrowDescribed),
		topics: () => fetchTopics(true).catch(rethrowDescribed),
		namespaces: () => fetchNamespaces(true).catch(rethrowDescribed),
		groups: () => fetchGroups(true).catch(rethrowDescribed),
		users: () => fetchUsers(true).catch(rethrowDescribed),
		iamPolicyAssignments: () => fetchIamPolicyAssignments(true).catch(rethrowDescribed),
		customPermissions: () => fetchCustomPermissions(true).catch(rethrowDescribed),
		brands: () => fetchBrands(true).catch(rethrowDescribed),
		actionConnectors: () => fetchActionConnectors(true).catch(rethrowDescribed),
		agents: () => fetchAgents(true).catch(rethrowDescribed),
		knowledgeBases: () => fetchKnowledgeBases(true).catch(rethrowDescribed),
		spaces: () => fetchSpaces(true).catch(rethrowDescribed)
	});

	function handleNamespaceChange(): void {
		if (activeTab === 'groups' || activeTab === 'users' || activeTab === 'iamPolicyAssignments') {
			void tabLoader.refresh(activeTab);
		}
	}

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// Every one of the six tabs holds data scoped by AwsAccountId + region.
	// `onRegionChange`'s effect always fires once immediately on mount, in
	// addition to on every later region switch (see region-effect.svelte.ts).
	// This is used for two different jobs depending on which of those it is:
	//
	//  - On mount: resolve the real account id from
	//    /dashboard/api/system/settings (see ensureAccountId) before the
	//    first list call, then load only the active tab -- the normal lazy,
	//    load-on-demand behavior every other tab-loader page has.
	//  - On a REAL region change: refresh every tab, not just the active
	//    one. Unlike accessanalyzer/detective's parent-scoped child tabs,
	//    these six tabs are independent of each other, so there is no
	//    cascading "reload the parent then the active child" to lean on --
	//    but tab-loader's `loaded` flag is still per-tab, and would
	//    otherwise let switching to an already-visited tab after a region
	//    change silently keep serving the previous region's cached rows.
	//    This only runs on an actual region switch (a deliberate, rare user
	//    action), not on mount, so it doesn't turn every page load into six
	//    requests.
	let regionChangeCount = 0;
	onRegionChange(() => {
		const isInitialMount = regionChangeCount === 0;
		regionChangeCount++;
		// Capture the tab active AT MOUNT, not whatever `activeTab` is once this
		// resolves -- ensureAccountId()'s fetch is async, so a user (or a fast
		// test) can switch tabs before it settles. Re-reading `activeTab` inside
		// the callback would then refresh whatever tab they switched TO a second
		// time and never load the one that was active when this effect fired.
		const tabAtMount = activeTab;
		void ensureAccountId().then(() => {
			if (isInitialMount) {
				void tabLoader.refresh(tabAtMount);
				return;
			}
			for (const t of tabs) {
				void tabLoader.refresh(t.id as TabId);
			}
		});
	});

	const filteredDashboards = $derived(
		dashboards.filter((d) => {
			const q = searchQuery.toLowerCase();
			return (
				(d.Name ?? '').toLowerCase().includes(q) || (d.DashboardId ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredAnalyses = $derived(
		analyses.filter((a) => {
			const q = searchQuery.toLowerCase();
			return (
				(a.Name ?? '').toLowerCase().includes(q) ||
				(a.AnalysisId ?? '').toLowerCase().includes(q) ||
				(a.Status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredDataSets = $derived(
		dataSets.filter((ds) => {
			const q = searchQuery.toLowerCase();
			return (
				(ds.Name ?? '').toLowerCase().includes(q) ||
				(ds.DataSetId ?? '').toLowerCase().includes(q) ||
				(ds.ImportMode ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredDataSources = $derived(
		dataSources.filter((ds) => {
			const q = searchQuery.toLowerCase();
			return (
				(ds.Name ?? '').toLowerCase().includes(q) ||
				(ds.DataSourceId ?? '').toLowerCase().includes(q) ||
				(ds.Type ?? '').toLowerCase().includes(q) ||
				(ds.Status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredFolders = $derived(
		folders.filter((f) => {
			const q = searchQuery.toLowerCase();
			return (
				(f.Name ?? '').toLowerCase().includes(q) ||
				(f.FolderId ?? '').toLowerCase().includes(q) ||
				(f.FolderType ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredVpcConnections = $derived(
		vpcConnections.filter((v) => {
			const q = searchQuery.toLowerCase();
			return (
				(v.Name ?? '').toLowerCase().includes(q) ||
				(v.VPCConnectionId ?? '').toLowerCase().includes(q) ||
				(v.VPCId ?? '').toLowerCase().includes(q) ||
				(v.Status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredTemplates = $derived(
		templates.filter((t) => {
			const q = searchQuery.toLowerCase();
			return (t.Name ?? '').toLowerCase().includes(q) || (t.TemplateId ?? '').toLowerCase().includes(q);
		})
	);
	const filteredThemes = $derived(
		themes.filter((t) => {
			const q = searchQuery.toLowerCase();
			return (t.Name ?? '').toLowerCase().includes(q) || (t.ThemeId ?? '').toLowerCase().includes(q);
		})
	);
	const filteredTopics = $derived(
		topics.filter((t) => {
			const q = searchQuery.toLowerCase();
			return (t.Name ?? '').toLowerCase().includes(q) || (t.TopicId ?? '').toLowerCase().includes(q);
		})
	);
	const filteredNamespaces = $derived(
		namespaces.filter((n) => (n.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredGroups = $derived(
		groups.filter((g) => {
			const q = searchQuery.toLowerCase();
			return (g.GroupName ?? '').toLowerCase().includes(q) || (g.Description ?? '').toLowerCase().includes(q);
		})
	);
	const filteredUsers = $derived(
		users.filter((u) => {
			const q = searchQuery.toLowerCase();
			return (
				(u.UserName ?? '').toLowerCase().includes(q) || (u.Email ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredIamPolicyAssignments = $derived(
		iamPolicyAssignments.filter((a) =>
			(a.AssignmentName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredCustomPermissions = $derived(
		customPermissions.filter((c) =>
			(c.CustomPermissionsName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredBrands = $derived(
		brands.filter((b) => {
			const q = searchQuery.toLowerCase();
			return (b.BrandName ?? '').toLowerCase().includes(q) || (b.BrandId ?? '').toLowerCase().includes(q);
		})
	);
	const filteredActionConnectors = $derived(
		actionConnectors.filter((a) => {
			const q = searchQuery.toLowerCase();
			return (
				(a.Name ?? '').toLowerCase().includes(q) ||
				(a.ActionConnectorId ?? '').toLowerCase().includes(q) ||
				(a.Type ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredAgents = $derived(
		agents.filter((a) => {
			const q = searchQuery.toLowerCase();
			return (a.Name ?? '').toLowerCase().includes(q) || (a.AgentId ?? '').toLowerCase().includes(q);
		})
	);
	const filteredKnowledgeBases = $derived(
		knowledgeBases.filter((k) => {
			const q = searchQuery.toLowerCase();
			return (
				(k.Name ?? '').toLowerCase().includes(q) || (k.KnowledgeBaseId ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredSpaces = $derived(
		spaces.filter((s) => {
			const q = searchQuery.toLowerCase();
			return (s.name ?? '').toLowerCase().includes(q) || (s.spaceId ?? '').toLowerCase().includes(q);
		})
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreDashboards(): Promise<void> {
		loadingMoreDashboards = true;
		try {
			await fetchDashboards(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreDashboards = false;
		}
	}

	async function loadMoreAnalyses(): Promise<void> {
		loadingMoreAnalyses = true;
		try {
			await fetchAnalyses(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAnalyses = false;
		}
	}

	async function loadMoreDataSets(): Promise<void> {
		loadingMoreDataSets = true;
		try {
			await fetchDataSets(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreDataSets = false;
		}
	}

	async function loadMoreDataSources(): Promise<void> {
		loadingMoreDataSources = true;
		try {
			await fetchDataSources(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreDataSources = false;
		}
	}

	async function loadMoreFolders(): Promise<void> {
		loadingMoreFolders = true;
		try {
			await fetchFolders(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreFolders = false;
		}
	}

	async function loadMoreVpcConnections(): Promise<void> {
		loadingMoreVpcConnections = true;
		try {
			await fetchVpcConnections(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreVpcConnections = false;
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

	async function loadMoreThemes(): Promise<void> {
		loadingMoreThemes = true;
		try {
			await fetchThemes(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreThemes = false;
		}
	}

	async function loadMoreTopics(): Promise<void> {
		loadingMoreTopics = true;
		try {
			await fetchTopics(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTopics = false;
		}
	}

	async function loadMoreNamespaces(): Promise<void> {
		loadingMoreNamespaces = true;
		try {
			await fetchNamespaces(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreNamespaces = false;
		}
	}

	async function loadMoreGroups(): Promise<void> {
		loadingMoreGroups = true;
		try {
			await fetchGroups(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGroups = false;
		}
	}

	async function loadMoreUsers(): Promise<void> {
		loadingMoreUsers = true;
		try {
			await fetchUsers(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreUsers = false;
		}
	}

	async function loadMoreIamPolicyAssignments(): Promise<void> {
		loadingMoreIamPolicyAssignments = true;
		try {
			await fetchIamPolicyAssignments(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreIamPolicyAssignments = false;
		}
	}

	async function loadMoreCustomPermissions(): Promise<void> {
		loadingMoreCustomPermissions = true;
		try {
			await fetchCustomPermissions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCustomPermissions = false;
		}
	}

	async function loadMoreBrands(): Promise<void> {
		loadingMoreBrands = true;
		try {
			await fetchBrands(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreBrands = false;
		}
	}

	async function loadMoreActionConnectors(): Promise<void> {
		loadingMoreActionConnectors = true;
		try {
			await fetchActionConnectors(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreActionConnectors = false;
		}
	}

	async function loadMoreAgents(): Promise<void> {
		loadingMoreAgents = true;
		try {
			await fetchAgents(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAgents = false;
		}
	}

	async function loadMoreKnowledgeBases(): Promise<void> {
		loadingMoreKnowledgeBases = true;
		try {
			await fetchKnowledgeBases(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreKnowledgeBases = false;
		}
	}

	async function loadMoreSpaces(): Promise<void> {
		loadingMoreSpaces = true;
		try {
			await fetchSpaces(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSpaces = false;
		}
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// ==================== Dashboards: create / update / delete / detail ====================

	let createDashboardModal = $state<Modal | null>(null);
	let creatingDashboard = $state(false);
	let createDashboardError = $state<string | null>(null);
	let newDashboardId = $state('');
	let newDashboardName = $state('');
	let newDashboardDefinition = $state('');

	function openCreateDashboardModal(): void {
		createDashboardError = null;
		newDashboardId = '';
		newDashboardName = '';
		newDashboardDefinition = '';
		createDashboardModal?.open();
	}

	async function submitCreateDashboard(): Promise<void> {
		if (!newDashboardId || !newDashboardName) {
			createDashboardError = 'Dashboard ID and name are required.';
			return;
		}
		let definition: DashboardVersionDefinition | undefined;
		if (newDashboardDefinition.trim()) {
			try {
				definition = JSON.parse(newDashboardDefinition) as DashboardVersionDefinition;
			} catch {
				createDashboardError = 'Definition must be valid JSON.';
				return;
			}
		}
		creatingDashboard = true;
		createDashboardError = null;
		try {
			await client().send(
				new CreateDashboardCommand({
					AwsAccountId: awsAccountId,
					DashboardId: newDashboardId,
					Name: newDashboardName,
					Definition: definition
				})
			);
			toast.success('Dashboard created');
			createDashboardModal?.close();
			await tabLoader.refresh('dashboards');
		} catch (e) {
			const msg = describeError(e);
			createDashboardError = msg;
			toast.error(msg);
		} finally {
			creatingDashboard = false;
		}
	}

	let editDashboardModal = $state<Modal | null>(null);
	let editingDashboard = $state<DashboardSummary | null>(null);
	let editingDashboardName = $state('');
	let editingDashboardDefinition = $state('');
	let savingDashboard = $state(false);
	let editDashboardError = $state<string | null>(null);

	function openEditDashboardModal(d: DashboardSummary): void {
		editingDashboard = d;
		editingDashboardName = d.Name ?? '';
		editingDashboardDefinition = '';
		editDashboardError = null;
		editDashboardModal?.open();
	}

	async function submitEditDashboard(): Promise<void> {
		if (!editingDashboard?.DashboardId) return;
		if (!editingDashboardName) {
			editDashboardError = 'Name is required.';
			return;
		}
		let definition: DashboardVersionDefinition | undefined;
		if (editingDashboardDefinition.trim()) {
			try {
				definition = JSON.parse(editingDashboardDefinition) as DashboardVersionDefinition;
			} catch {
				editDashboardError = 'Definition must be valid JSON.';
				return;
			}
		}
		savingDashboard = true;
		editDashboardError = null;
		try {
			await client().send(
				new UpdateDashboardCommand({
					AwsAccountId: awsAccountId,
					DashboardId: editingDashboard.DashboardId,
					Name: editingDashboardName,
					Definition: definition
				})
			);
			toast.success('Dashboard updated');
			editDashboardModal?.close();
			await tabLoader.refresh('dashboards');
		} catch (e) {
			const msg = describeError(e);
			editDashboardError = msg;
			toast.error(msg);
		} finally {
			savingDashboard = false;
		}
	}

	async function handleDeleteDashboard(d: DashboardSummary): Promise<void> {
		if (!d.DashboardId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete dashboard',
			message: `Delete dashboard ${d.Name ?? d.DashboardId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteDashboardCommand({ AwsAccountId: awsAccountId, DashboardId: d.DashboardId })
			);
			toast.success('Dashboard deleted');
			await tabLoader.refresh('dashboards');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let dashboardDetailModal = $state<Modal | null>(null);
	let viewedDashboard = $state<DashboardSummary | Dashboard | null>(null);
	let dashboardDetailLoading = $state(false);
	let dashboardDetailError = $state<string | null>(null);

	async function openDashboardDetail(d: DashboardSummary): Promise<void> {
		viewedDashboard = d;
		dashboardDetailError = null;
		dashboardDetailModal?.open();
		if (!d.DashboardId) return;
		dashboardDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeDashboardCommand({ AwsAccountId: awsAccountId, DashboardId: d.DashboardId })
			);
			viewedDashboard = resp.Dashboard ?? d;
		} catch (e) {
			dashboardDetailError = describeError(e);
		} finally {
			dashboardDetailLoading = false;
		}
	}

	// ==================== Analyses: create / update / delete / detail ====================

	let createAnalysisModal = $state<Modal | null>(null);
	let creatingAnalysis = $state(false);
	let createAnalysisError = $state<string | null>(null);
	let newAnalysisId = $state('');
	let newAnalysisName = $state('');
	let newAnalysisDefinition = $state('');

	function openCreateAnalysisModal(): void {
		createAnalysisError = null;
		newAnalysisId = '';
		newAnalysisName = '';
		newAnalysisDefinition = '';
		createAnalysisModal?.open();
	}

	async function submitCreateAnalysis(): Promise<void> {
		if (!newAnalysisId || !newAnalysisName) {
			createAnalysisError = 'Analysis ID and name are required.';
			return;
		}
		let definition: AnalysisDefinition | undefined;
		if (newAnalysisDefinition.trim()) {
			try {
				definition = JSON.parse(newAnalysisDefinition) as AnalysisDefinition;
			} catch {
				createAnalysisError = 'Definition must be valid JSON.';
				return;
			}
		}
		creatingAnalysis = true;
		createAnalysisError = null;
		try {
			await client().send(
				new CreateAnalysisCommand({
					AwsAccountId: awsAccountId,
					AnalysisId: newAnalysisId,
					Name: newAnalysisName,
					Definition: definition
				})
			);
			toast.success('Analysis created');
			createAnalysisModal?.close();
			await tabLoader.refresh('analyses');
		} catch (e) {
			const msg = describeError(e);
			createAnalysisError = msg;
			toast.error(msg);
		} finally {
			creatingAnalysis = false;
		}
	}

	let editAnalysisModal = $state<Modal | null>(null);
	let editingAnalysis = $state<AnalysisSummary | null>(null);
	let editingAnalysisName = $state('');
	let editingAnalysisDefinition = $state('');
	let savingAnalysis = $state(false);
	let editAnalysisError = $state<string | null>(null);

	function openEditAnalysisModal(a: AnalysisSummary): void {
		editingAnalysis = a;
		editingAnalysisName = a.Name ?? '';
		editingAnalysisDefinition = '';
		editAnalysisError = null;
		editAnalysisModal?.open();
	}

	async function submitEditAnalysis(): Promise<void> {
		if (!editingAnalysis?.AnalysisId) return;
		if (!editingAnalysisName) {
			editAnalysisError = 'Name is required.';
			return;
		}
		let definition: AnalysisDefinition | undefined;
		if (editingAnalysisDefinition.trim()) {
			try {
				definition = JSON.parse(editingAnalysisDefinition) as AnalysisDefinition;
			} catch {
				editAnalysisError = 'Definition must be valid JSON.';
				return;
			}
		}
		savingAnalysis = true;
		editAnalysisError = null;
		try {
			await client().send(
				new UpdateAnalysisCommand({
					AwsAccountId: awsAccountId,
					AnalysisId: editingAnalysis.AnalysisId,
					Name: editingAnalysisName,
					Definition: definition
				})
			);
			toast.success('Analysis updated');
			editAnalysisModal?.close();
			await tabLoader.refresh('analyses');
		} catch (e) {
			const msg = describeError(e);
			editAnalysisError = msg;
			toast.error(msg);
		} finally {
			savingAnalysis = false;
		}
	}

	async function handleDeleteAnalysis(a: AnalysisSummary): Promise<void> {
		if (!a.AnalysisId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete analysis',
			message: `Delete analysis ${a.Name ?? a.AnalysisId}? This deletes it immediately, without the 30-day recovery window real AWS offers by default.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteAnalysisCommand({
					AwsAccountId: awsAccountId,
					AnalysisId: a.AnalysisId,
					ForceDeleteWithoutRecovery: true
				})
			);
			toast.success('Analysis deleted');
			await tabLoader.refresh('analyses');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let analysisDetailModal = $state<Modal | null>(null);
	let viewedAnalysis = $state<AnalysisSummary | Analysis | null>(null);
	let analysisDetailLoading = $state(false);
	let analysisDetailError = $state<string | null>(null);

	async function openAnalysisDetail(a: AnalysisSummary): Promise<void> {
		viewedAnalysis = a;
		analysisDetailError = null;
		analysisDetailModal?.open();
		if (!a.AnalysisId) return;
		analysisDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeAnalysisCommand({ AwsAccountId: awsAccountId, AnalysisId: a.AnalysisId })
			);
			viewedAnalysis = resp.Analysis ?? a;
		} catch (e) {
			analysisDetailError = describeError(e);
		} finally {
			analysisDetailLoading = false;
		}
	}

	// ==================== Data Sets: create / update / delete / detail ====================
	//
	// CreateDataSetCommand/UpdateDataSetCommand's PhysicalTableMap is required
	// by the SDK's request type (real AWS uses it to declare the dataset's
	// underlying physical tables), but this backend never reads or returns it
	// (dataSetToMap only echoes Arn/CreatedTime/DataSetId/ImportMode/
	// LastUpdatedTime/Name -- confirmed in services/quicksight/handler_dataset.go).
	// Rather than build a physical-table-declaration editor for a value this
	// backend silently discards, an empty map is sent to satisfy the type; see
	// PARITY.md's precedent of documenting fields a mock backend can't
	// meaningfully model.

	let createDataSetModal = $state<Modal | null>(null);
	let creatingDataSet = $state(false);
	let createDataSetError = $state<string | null>(null);
	let newDataSetId = $state('');
	let newDataSetName = $state('');
	let newDataSetImportMode = $state<DataSetImportMode>('SPICE');

	function openCreateDataSetModal(): void {
		createDataSetError = null;
		newDataSetId = '';
		newDataSetName = '';
		newDataSetImportMode = 'SPICE';
		createDataSetModal?.open();
	}

	async function submitCreateDataSet(): Promise<void> {
		if (!newDataSetId || !newDataSetName) {
			createDataSetError = 'Data set ID and name are required.';
			return;
		}
		creatingDataSet = true;
		createDataSetError = null;
		try {
			await client().send(
				new CreateDataSetCommand({
					AwsAccountId: awsAccountId,
					DataSetId: newDataSetId,
					Name: newDataSetName,
					ImportMode: newDataSetImportMode,
					PhysicalTableMap: {}
				})
			);
			toast.success('Data set created');
			createDataSetModal?.close();
			await tabLoader.refresh('datasets');
		} catch (e) {
			const msg = describeError(e);
			createDataSetError = msg;
			toast.error(msg);
		} finally {
			creatingDataSet = false;
		}
	}

	let editDataSetModal = $state<Modal | null>(null);
	let editingDataSet = $state<DataSetSummary | null>(null);
	let editingDataSetName = $state('');
	let editingDataSetImportMode = $state<DataSetImportMode>('SPICE');
	let savingDataSet = $state(false);
	let editDataSetError = $state<string | null>(null);

	function openEditDataSetModal(ds: DataSetSummary): void {
		editingDataSet = ds;
		editingDataSetName = ds.Name ?? '';
		editingDataSetImportMode = ds.ImportMode ?? 'SPICE';
		editDataSetError = null;
		editDataSetModal?.open();
	}

	async function submitEditDataSet(): Promise<void> {
		if (!editingDataSet?.DataSetId) return;
		if (!editingDataSetName) {
			editDataSetError = 'Name is required.';
			return;
		}
		savingDataSet = true;
		editDataSetError = null;
		try {
			await client().send(
				new UpdateDataSetCommand({
					AwsAccountId: awsAccountId,
					DataSetId: editingDataSet.DataSetId,
					Name: editingDataSetName,
					ImportMode: editingDataSetImportMode,
					PhysicalTableMap: {}
				})
			);
			toast.success('Data set updated');
			editDataSetModal?.close();
			await tabLoader.refresh('datasets');
		} catch (e) {
			const msg = describeError(e);
			editDataSetError = msg;
			toast.error(msg);
		} finally {
			savingDataSet = false;
		}
	}

	async function handleDeleteDataSet(ds: DataSetSummary): Promise<void> {
		if (!ds.DataSetId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete data set',
			message: `Delete data set ${ds.Name ?? ds.DataSetId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteDataSetCommand({ AwsAccountId: awsAccountId, DataSetId: ds.DataSetId })
			);
			toast.success('Data set deleted');
			await tabLoader.refresh('datasets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let dataSetDetailModal = $state<Modal | null>(null);
	let viewedDataSet = $state<DataSetSummary | DataSet | null>(null);
	let dataSetDetailLoading = $state(false);
	let dataSetDetailError = $state<string | null>(null);

	async function openDataSetDetail(ds: DataSetSummary): Promise<void> {
		viewedDataSet = ds;
		dataSetDetailError = null;
		dataSetDetailModal?.open();
		if (!ds.DataSetId) return;
		dataSetDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeDataSetCommand({ AwsAccountId: awsAccountId, DataSetId: ds.DataSetId })
			);
			viewedDataSet = resp.DataSet ?? ds;
		} catch (e) {
			dataSetDetailError = describeError(e);
		} finally {
			dataSetDetailLoading = false;
		}
	}

	// ==================== Data Sources: create / update / delete / detail ====================

	const dataSourceTypes: DataSourceType[] = [
		'MYSQL',
		'POSTGRESQL',
		'REDSHIFT',
		'S3',
		'ATHENA',
		'SNOWFLAKE',
		'ORACLE',
		'SQLSERVER',
		'AURORA',
		'AURORA_POSTGRESQL',
		'MARIADB',
		'BIGQUERY',
		'SPARK',
		'TERADATA'
	];

	let createDataSourceModal = $state<Modal | null>(null);
	let creatingDataSource = $state(false);
	let createDataSourceError = $state<string | null>(null);
	let newDataSourceId = $state('');
	let newDataSourceName = $state('');
	let newDataSourceType = $state<DataSourceType>('MYSQL');

	function openCreateDataSourceModal(): void {
		createDataSourceError = null;
		newDataSourceId = '';
		newDataSourceName = '';
		newDataSourceType = 'MYSQL';
		createDataSourceModal?.open();
	}

	async function submitCreateDataSource(): Promise<void> {
		if (!newDataSourceId || !newDataSourceName) {
			createDataSourceError = 'Data source ID and name are required.';
			return;
		}
		creatingDataSource = true;
		createDataSourceError = null;
		try {
			await client().send(
				new CreateDataSourceCommand({
					AwsAccountId: awsAccountId,
					DataSourceId: newDataSourceId,
					Name: newDataSourceName,
					Type: newDataSourceType
				})
			);
			toast.success('Data source created');
			createDataSourceModal?.close();
			await tabLoader.refresh('datasources');
		} catch (e) {
			const msg = describeError(e);
			createDataSourceError = msg;
			toast.error(msg);
		} finally {
			creatingDataSource = false;
		}
	}

	let editDataSourceModal = $state<Modal | null>(null);
	let editingDataSource = $state<DataSource | null>(null);
	let editingDataSourceName = $state('');
	let savingDataSource = $state(false);
	let editDataSourceError = $state<string | null>(null);

	function openEditDataSourceModal(ds: DataSource): void {
		editingDataSource = ds;
		editingDataSourceName = ds.Name ?? '';
		editDataSourceError = null;
		editDataSourceModal?.open();
	}

	async function submitEditDataSource(): Promise<void> {
		if (!editingDataSource?.DataSourceId) return;
		if (!editingDataSourceName) {
			editDataSourceError = 'Name is required.';
			return;
		}
		savingDataSource = true;
		editDataSourceError = null;
		try {
			await client().send(
				new UpdateDataSourceCommand({
					AwsAccountId: awsAccountId,
					DataSourceId: editingDataSource.DataSourceId,
					Name: editingDataSourceName
				})
			);
			toast.success('Data source updated');
			editDataSourceModal?.close();
			await tabLoader.refresh('datasources');
		} catch (e) {
			const msg = describeError(e);
			editDataSourceError = msg;
			toast.error(msg);
		} finally {
			savingDataSource = false;
		}
	}

	async function handleDeleteDataSource(ds: DataSource): Promise<void> {
		if (!ds.DataSourceId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete data source',
			message: `Delete data source ${ds.Name ?? ds.DataSourceId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteDataSourceCommand({ AwsAccountId: awsAccountId, DataSourceId: ds.DataSourceId })
			);
			toast.success('Data source deleted');
			await tabLoader.refresh('datasources');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let dataSourceDetailModal = $state<Modal | null>(null);
	let viewedDataSource = $state<DataSource | null>(null);
	let dataSourceDetailLoading = $state(false);
	let dataSourceDetailError = $state<string | null>(null);

	async function openDataSourceDetail(ds: DataSource): Promise<void> {
		viewedDataSource = ds;
		dataSourceDetailError = null;
		dataSourceDetailModal?.open();
		if (!ds.DataSourceId) return;
		dataSourceDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeDataSourceCommand({ AwsAccountId: awsAccountId, DataSourceId: ds.DataSourceId })
			);
			viewedDataSource = resp.DataSource ?? ds;
		} catch (e) {
			dataSourceDetailError = describeError(e);
		} finally {
			dataSourceDetailLoading = false;
		}
	}

	// ==================== Folders: create / update / delete / detail ====================

	let createFolderModal = $state<Modal | null>(null);
	let creatingFolder = $state(false);
	let createFolderError = $state<string | null>(null);
	let newFolderId = $state('');
	let newFolderName = $state('');
	let newFolderType = $state<FolderType>('SHARED');
	let newFolderParentArn = $state('');
	let newFolderSharingModel = $state<SharingModel | ''>('');

	function openCreateFolderModal(): void {
		createFolderError = null;
		newFolderId = '';
		newFolderName = '';
		newFolderType = 'SHARED';
		newFolderParentArn = '';
		newFolderSharingModel = '';
		createFolderModal?.open();
	}

	async function submitCreateFolder(): Promise<void> {
		if (!newFolderId || !newFolderName) {
			createFolderError = 'Folder ID and name are required.';
			return;
		}
		creatingFolder = true;
		createFolderError = null;
		try {
			await client().send(
				new CreateFolderCommand({
					AwsAccountId: awsAccountId,
					FolderId: newFolderId,
					Name: newFolderName,
					FolderType: newFolderType,
					ParentFolderArn: newFolderParentArn || undefined,
					SharingModel: newFolderSharingModel || undefined
				})
			);
			toast.success('Folder created');
			createFolderModal?.close();
			await tabLoader.refresh('folders');
		} catch (e) {
			const msg = describeError(e);
			createFolderError = msg;
			toast.error(msg);
		} finally {
			creatingFolder = false;
		}
	}

	let editFolderModal = $state<Modal | null>(null);
	let editingFolder = $state<FolderSummary | null>(null);
	let editingFolderName = $state('');
	let savingFolder = $state(false);
	let editFolderError = $state<string | null>(null);

	function openEditFolderModal(f: FolderSummary): void {
		editingFolder = f;
		editingFolderName = f.Name ?? '';
		editFolderError = null;
		editFolderModal?.open();
	}

	async function submitEditFolder(): Promise<void> {
		if (!editingFolder?.FolderId) return;
		if (!editingFolderName) {
			editFolderError = 'Name is required.';
			return;
		}
		savingFolder = true;
		editFolderError = null;
		try {
			await client().send(
				new UpdateFolderCommand({
					AwsAccountId: awsAccountId,
					FolderId: editingFolder.FolderId,
					Name: editingFolderName
				})
			);
			toast.success('Folder updated');
			editFolderModal?.close();
			await tabLoader.refresh('folders');
		} catch (e) {
			const msg = describeError(e);
			editFolderError = msg;
			toast.error(msg);
		} finally {
			savingFolder = false;
		}
	}

	async function handleDeleteFolder(f: FolderSummary): Promise<void> {
		if (!f.FolderId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete folder',
			message: `Delete folder ${f.Name ?? f.FolderId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteFolderCommand({ AwsAccountId: awsAccountId, FolderId: f.FolderId }));
			toast.success('Folder deleted');
			await tabLoader.refresh('folders');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let folderDetailModal = $state<Modal | null>(null);
	let viewedFolder = $state<FolderSummary | Folder | null>(null);
	let folderDetailLoading = $state(false);
	let folderDetailError = $state<string | null>(null);

	async function openFolderDetail(f: FolderSummary): Promise<void> {
		viewedFolder = f;
		folderDetailError = null;
		folderDetailModal?.open();
		if (!f.FolderId) return;
		folderDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeFolderCommand({ AwsAccountId: awsAccountId, FolderId: f.FolderId })
			);
			viewedFolder = resp.Folder ?? f;
		} catch (e) {
			folderDetailError = describeError(e);
		} finally {
			folderDetailLoading = false;
		}
	}

	// ==================== VPC Connections: create / update / delete / detail ====================

	let createVpcConnectionModal = $state<Modal | null>(null);
	let creatingVpcConnection = $state(false);
	let createVpcConnectionError = $state<string | null>(null);
	let newVpcConnectionId = $state('');
	let newVpcConnectionName = $state('');
	let newVpcConnectionSubnetIds = $state('');
	let newVpcConnectionSecurityGroupIds = $state('');
	let newVpcConnectionDnsResolvers = $state('');
	let newVpcConnectionRoleArn = $state('');

	function openCreateVpcConnectionModal(): void {
		createVpcConnectionError = null;
		newVpcConnectionId = '';
		newVpcConnectionName = '';
		newVpcConnectionSubnetIds = '';
		newVpcConnectionSecurityGroupIds = '';
		newVpcConnectionDnsResolvers = '';
		newVpcConnectionRoleArn = '';
		createVpcConnectionModal?.open();
	}

	async function submitCreateVpcConnection(): Promise<void> {
		if (
			!newVpcConnectionId ||
			!newVpcConnectionName ||
			!newVpcConnectionSubnetIds ||
			!newVpcConnectionSecurityGroupIds ||
			!newVpcConnectionRoleArn
		) {
			createVpcConnectionError =
				'VPC connection ID, name, subnet IDs, security group IDs, and role ARN are required.';
			return;
		}
		creatingVpcConnection = true;
		createVpcConnectionError = null;
		try {
			await client().send(
				new CreateVPCConnectionCommand({
					AwsAccountId: awsAccountId,
					VPCConnectionId: newVpcConnectionId,
					Name: newVpcConnectionName,
					SubnetIds: parseCommaList(newVpcConnectionSubnetIds),
					SecurityGroupIds: parseCommaList(newVpcConnectionSecurityGroupIds),
					DnsResolvers: newVpcConnectionDnsResolvers
						? parseCommaList(newVpcConnectionDnsResolvers)
						: undefined,
					RoleArn: newVpcConnectionRoleArn
				})
			);
			toast.success('VPC connection created');
			createVpcConnectionModal?.close();
			await tabLoader.refresh('vpcConnections');
		} catch (e) {
			const msg = describeError(e);
			createVpcConnectionError = msg;
			toast.error(msg);
		} finally {
			creatingVpcConnection = false;
		}
	}

	let editVpcConnectionModal = $state<Modal | null>(null);
	let editingVpcConnection = $state<VPCConnectionSummary | null>(null);
	let editingVpcConnectionName = $state('');
	let editingVpcConnectionSubnetIds = $state('');
	let editingVpcConnectionSecurityGroupIds = $state('');
	let editingVpcConnectionDnsResolvers = $state('');
	let editingVpcConnectionRoleArn = $state('');
	let savingVpcConnection = $state(false);
	let editVpcConnectionError = $state<string | null>(null);

	function openEditVpcConnectionModal(v: VPCConnectionSummary): void {
		editingVpcConnection = v;
		editingVpcConnectionName = v.Name ?? '';
		// Real AWS's VPCConnectionSummary/VPCConnection response shapes carry no
		// SubnetIds field at all (confirmed against both aws-sdk-go-v2's and
		// this installed @aws-sdk/client-quicksight's types: SubnetIds is
		// accepted by Create/UpdateVPCConnectionRequest but never echoed back by
		// Describe/List -- only indirectly inferable, once AWS actually
		// provisions ENIs, via NetworkInterfaces[].SubnetId). So unlike every
		// other editable field here, there is no value to prefill this from;
		// UpdateVPCConnectionCommand still requires it, so the field is left
		// blank for the caller to (re)supply.
		editingVpcConnectionSubnetIds = '';
		editingVpcConnectionSecurityGroupIds = (v.SecurityGroupIds ?? []).join(', ');
		editingVpcConnectionDnsResolvers = (v.DnsResolvers ?? []).join(', ');
		editingVpcConnectionRoleArn = v.RoleArn ?? '';
		editVpcConnectionError = null;
		editVpcConnectionModal?.open();
	}

	async function submitEditVpcConnection(): Promise<void> {
		if (!editingVpcConnection?.VPCConnectionId) return;
		if (
			!editingVpcConnectionName ||
			!editingVpcConnectionSubnetIds ||
			!editingVpcConnectionSecurityGroupIds ||
			!editingVpcConnectionRoleArn
		) {
			editVpcConnectionError = 'Name, subnet IDs, security group IDs, and role ARN are required.';
			return;
		}
		savingVpcConnection = true;
		editVpcConnectionError = null;
		try {
			await client().send(
				new UpdateVPCConnectionCommand({
					AwsAccountId: awsAccountId,
					VPCConnectionId: editingVpcConnection.VPCConnectionId,
					Name: editingVpcConnectionName,
					SubnetIds: parseCommaList(editingVpcConnectionSubnetIds),
					SecurityGroupIds: parseCommaList(editingVpcConnectionSecurityGroupIds),
					DnsResolvers: editingVpcConnectionDnsResolvers
						? parseCommaList(editingVpcConnectionDnsResolvers)
						: undefined,
					RoleArn: editingVpcConnectionRoleArn
				})
			);
			toast.success('VPC connection updated');
			editVpcConnectionModal?.close();
			await tabLoader.refresh('vpcConnections');
		} catch (e) {
			const msg = describeError(e);
			editVpcConnectionError = msg;
			toast.error(msg);
		} finally {
			savingVpcConnection = false;
		}
	}

	async function handleDeleteVpcConnection(v: VPCConnectionSummary): Promise<void> {
		if (!v.VPCConnectionId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete VPC connection',
			message: `Delete VPC connection ${v.Name ?? v.VPCConnectionId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteVPCConnectionCommand({
					AwsAccountId: awsAccountId,
					VPCConnectionId: v.VPCConnectionId
				})
			);
			toast.success('VPC connection deleted');
			await tabLoader.refresh('vpcConnections');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let vpcConnectionDetailModal = $state<Modal | null>(null);
	let viewedVpcConnection = $state<VPCConnectionSummary | VPCConnection | null>(null);
	let vpcConnectionDetailLoading = $state(false);
	let vpcConnectionDetailError = $state<string | null>(null);

	async function openVpcConnectionDetail(v: VPCConnectionSummary): Promise<void> {
		viewedVpcConnection = v;
		vpcConnectionDetailError = null;
		vpcConnectionDetailModal?.open();
		if (!v.VPCConnectionId) return;
		vpcConnectionDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeVPCConnectionCommand({
					AwsAccountId: awsAccountId,
					VPCConnectionId: v.VPCConnectionId
				})
			);
			viewedVpcConnection = resp.VPCConnection ?? v;
		} catch (e) {
			vpcConnectionDetailError = describeError(e);
		} finally {
			vpcConnectionDetailLoading = false;
		}
	}

	// ==================== Templates: create / update / delete / detail ====================
	// SourceEntity (SourceTemplate/SourceAnalysis reference) is not modeled by this form --
	// only the Definition path, same JSON-textarea convention as Dashboard/Analysis above.

	let createTemplateModal = $state<Modal | null>(null);
	let creatingTemplate = $state(false);
	let createTemplateError = $state<string | null>(null);
	let newTemplateId = $state('');
	let newTemplateName = $state('');
	let newTemplateDefinition = $state('');

	function openCreateTemplateModal(): void {
		createTemplateError = null;
		newTemplateId = '';
		newTemplateName = '';
		newTemplateDefinition = '';
		createTemplateModal?.open();
	}

	async function submitCreateTemplate(): Promise<void> {
		if (!newTemplateId || !newTemplateName || !newTemplateDefinition.trim()) {
			createTemplateError = 'Template ID, name, and definition are required.';
			return;
		}
		let definition: TemplateVersionDefinition;
		try {
			definition = JSON.parse(newTemplateDefinition) as TemplateVersionDefinition;
		} catch {
			createTemplateError = 'Definition must be valid JSON.';
			return;
		}
		creatingTemplate = true;
		createTemplateError = null;
		try {
			await client().send(
				new CreateTemplateCommand({
					AwsAccountId: awsAccountId,
					TemplateId: newTemplateId,
					Name: newTemplateName,
					Definition: definition
				})
			);
			toast.success('Template created');
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

	let editTemplateModal = $state<Modal | null>(null);
	let editingTemplate = $state<TemplateSummary | null>(null);
	let editingTemplateName = $state('');
	let editingTemplateDefinition = $state('');
	let savingTemplate = $state(false);
	let editTemplateError = $state<string | null>(null);

	function openEditTemplateModal(t: TemplateSummary): void {
		editingTemplate = t;
		editingTemplateName = t.Name ?? '';
		editingTemplateDefinition = '';
		editTemplateError = null;
		editTemplateModal?.open();
	}

	async function submitEditTemplate(): Promise<void> {
		if (!editingTemplate?.TemplateId) return;
		if (!editingTemplateName) {
			editTemplateError = 'Name is required.';
			return;
		}
		let definition: TemplateVersionDefinition | undefined;
		if (editingTemplateDefinition.trim()) {
			try {
				definition = JSON.parse(editingTemplateDefinition) as TemplateVersionDefinition;
			} catch {
				editTemplateError = 'Definition must be valid JSON.';
				return;
			}
		}
		savingTemplate = true;
		editTemplateError = null;
		try {
			await client().send(
				new UpdateTemplateCommand({
					AwsAccountId: awsAccountId,
					TemplateId: editingTemplate.TemplateId,
					Name: editingTemplateName,
					Definition: definition
				})
			);
			toast.success('Template updated');
			editTemplateModal?.close();
			await tabLoader.refresh('templates');
		} catch (e) {
			const msg = describeError(e);
			editTemplateError = msg;
			toast.error(msg);
		} finally {
			savingTemplate = false;
		}
	}

	async function handleDeleteTemplate(t: TemplateSummary): Promise<void> {
		if (!t.TemplateId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete template',
			message: `Delete template ${t.Name ?? t.TemplateId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteTemplateCommand({ AwsAccountId: awsAccountId, TemplateId: t.TemplateId })
			);
			toast.success('Template deleted');
			await tabLoader.refresh('templates');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let templateDetailModal = $state<Modal | null>(null);
	let viewedTemplate = $state<TemplateSummary | Template | null>(null);
	let templateDetailLoading = $state(false);
	let templateDetailError = $state<string | null>(null);

	async function openTemplateDetail(t: TemplateSummary): Promise<void> {
		viewedTemplate = t;
		templateDetailError = null;
		templateDetailModal?.open();
		if (!t.TemplateId) return;
		templateDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeTemplateCommand({ AwsAccountId: awsAccountId, TemplateId: t.TemplateId })
			);
			viewedTemplate = resp.Template ?? t;
		} catch (e) {
			templateDetailError = describeError(e);
		} finally {
			templateDetailLoading = false;
		}
	}

	// ==================== Themes: create / update / delete / detail ====================

	let createThemeModal = $state<Modal | null>(null);
	let creatingTheme = $state(false);
	let createThemeError = $state<string | null>(null);
	let newThemeId = $state('');
	let newThemeName = $state('');
	let newThemeBaseThemeId = $state('TAKARA');
	let newThemeConfiguration = $state('');

	function openCreateThemeModal(): void {
		createThemeError = null;
		newThemeId = '';
		newThemeName = '';
		newThemeBaseThemeId = 'TAKARA';
		newThemeConfiguration = '';
		createThemeModal?.open();
	}

	async function submitCreateTheme(): Promise<void> {
		if (!newThemeId || !newThemeName || !newThemeBaseThemeId || !newThemeConfiguration.trim()) {
			createThemeError = 'Theme ID, name, base theme ID, and configuration are required.';
			return;
		}
		let configuration: ThemeConfiguration;
		try {
			configuration = JSON.parse(newThemeConfiguration) as ThemeConfiguration;
		} catch {
			createThemeError = 'Configuration must be valid JSON.';
			return;
		}
		creatingTheme = true;
		createThemeError = null;
		try {
			await client().send(
				new CreateThemeCommand({
					AwsAccountId: awsAccountId,
					ThemeId: newThemeId,
					Name: newThemeName,
					BaseThemeId: newThemeBaseThemeId,
					Configuration: configuration
				})
			);
			toast.success('Theme created');
			createThemeModal?.close();
			await tabLoader.refresh('themes');
		} catch (e) {
			const msg = describeError(e);
			createThemeError = msg;
			toast.error(msg);
		} finally {
			creatingTheme = false;
		}
	}

	let editThemeModal = $state<Modal | null>(null);
	let editingTheme = $state<ThemeSummary | null>(null);
	let editingThemeName = $state('');
	let editingThemeBaseThemeId = $state('');
	let editingThemeConfiguration = $state('');
	let savingTheme = $state(false);
	let editThemeError = $state<string | null>(null);

	function openEditThemeModal(t: ThemeSummary): void {
		editingTheme = t;
		editingThemeName = t.Name ?? '';
		editingThemeBaseThemeId = 'TAKARA';
		editingThemeConfiguration = '';
		editThemeError = null;
		editThemeModal?.open();
	}

	async function submitEditTheme(): Promise<void> {
		if (!editingTheme?.ThemeId) return;
		if (!editingThemeName || !editingThemeBaseThemeId) {
			editThemeError = 'Name and base theme ID are required.';
			return;
		}
		let configuration: ThemeConfiguration | undefined;
		if (editingThemeConfiguration.trim()) {
			try {
				configuration = JSON.parse(editingThemeConfiguration) as ThemeConfiguration;
			} catch {
				editThemeError = 'Configuration must be valid JSON.';
				return;
			}
		}
		savingTheme = true;
		editThemeError = null;
		try {
			await client().send(
				new UpdateThemeCommand({
					AwsAccountId: awsAccountId,
					ThemeId: editingTheme.ThemeId,
					Name: editingThemeName,
					BaseThemeId: editingThemeBaseThemeId,
					Configuration: configuration
				})
			);
			toast.success('Theme updated');
			editThemeModal?.close();
			await tabLoader.refresh('themes');
		} catch (e) {
			const msg = describeError(e);
			editThemeError = msg;
			toast.error(msg);
		} finally {
			savingTheme = false;
		}
	}

	async function handleDeleteTheme(t: ThemeSummary): Promise<void> {
		if (!t.ThemeId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete theme',
			message: `Delete theme ${t.Name ?? t.ThemeId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteThemeCommand({ AwsAccountId: awsAccountId, ThemeId: t.ThemeId }));
			toast.success('Theme deleted');
			await tabLoader.refresh('themes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let themeDetailModal = $state<Modal | null>(null);
	let viewedTheme = $state<ThemeSummary | Theme | null>(null);
	let themeDetailLoading = $state(false);
	let themeDetailError = $state<string | null>(null);

	async function openThemeDetail(t: ThemeSummary): Promise<void> {
		viewedTheme = t;
		themeDetailError = null;
		themeDetailModal?.open();
		if (!t.ThemeId) return;
		themeDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeThemeCommand({ AwsAccountId: awsAccountId, ThemeId: t.ThemeId })
			);
			viewedTheme = resp.Theme ?? t;
		} catch (e) {
			themeDetailError = describeError(e);
		} finally {
			themeDetailLoading = false;
		}
	}

	// ==================== Topics: create / update / delete / detail ====================

	let createTopicModal = $state<Modal | null>(null);
	let creatingTopic = $state(false);
	let createTopicError = $state<string | null>(null);
	let newTopicId = $state('');
	let newTopicDefinition = $state('{\n  "Name": ""\n}');

	function openCreateTopicModal(): void {
		createTopicError = null;
		newTopicId = '';
		newTopicDefinition = '{\n  "Name": ""\n}';
		createTopicModal?.open();
	}

	async function submitCreateTopic(): Promise<void> {
		if (!newTopicId || !newTopicDefinition.trim()) {
			createTopicError = 'Topic ID and definition are required.';
			return;
		}
		let topic: TopicDetails;
		try {
			topic = JSON.parse(newTopicDefinition) as TopicDetails;
		} catch {
			createTopicError = 'Topic definition must be valid JSON.';
			return;
		}
		creatingTopic = true;
		createTopicError = null;
		try {
			await client().send(
				new CreateTopicCommand({ AwsAccountId: awsAccountId, TopicId: newTopicId, Topic: topic })
			);
			toast.success('Topic created');
			createTopicModal?.close();
			await tabLoader.refresh('topics');
		} catch (e) {
			const msg = describeError(e);
			createTopicError = msg;
			toast.error(msg);
		} finally {
			creatingTopic = false;
		}
	}

	let editTopicModal = $state<Modal | null>(null);
	let editingTopic = $state<TopicSummary | null>(null);
	let editingTopicDefinition = $state('');
	let savingTopic = $state(false);
	let editTopicError = $state<string | null>(null);

	function openEditTopicModal(t: TopicSummary): void {
		editingTopic = t;
		editingTopicDefinition = JSON.stringify({ Name: t.Name ?? '' }, null, 2);
		editTopicError = null;
		editTopicModal?.open();
	}

	async function submitEditTopic(): Promise<void> {
		if (!editingTopic?.TopicId) return;
		let topic: TopicDetails;
		try {
			topic = JSON.parse(editingTopicDefinition) as TopicDetails;
		} catch {
			editTopicError = 'Topic definition must be valid JSON.';
			return;
		}
		savingTopic = true;
		editTopicError = null;
		try {
			await client().send(
				new UpdateTopicCommand({ AwsAccountId: awsAccountId, TopicId: editingTopic.TopicId, Topic: topic })
			);
			toast.success('Topic updated');
			editTopicModal?.close();
			await tabLoader.refresh('topics');
		} catch (e) {
			const msg = describeError(e);
			editTopicError = msg;
			toast.error(msg);
		} finally {
			savingTopic = false;
		}
	}

	async function handleDeleteTopic(t: TopicSummary): Promise<void> {
		if (!t.TopicId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete topic',
			message: `Delete topic ${t.Name ?? t.TopicId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteTopicCommand({ AwsAccountId: awsAccountId, TopicId: t.TopicId }));
			toast.success('Topic deleted');
			await tabLoader.refresh('topics');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let topicDetailModal = $state<Modal | null>(null);
	let viewedTopic = $state<(TopicSummary & TopicDetails) | null>(null);
	let topicDetailLoading = $state(false);
	let topicDetailError = $state<string | null>(null);

	async function openTopicDetail(t: TopicSummary): Promise<void> {
		viewedTopic = t;
		topicDetailError = null;
		topicDetailModal?.open();
		if (!t.TopicId) return;
		topicDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeTopicCommand({ AwsAccountId: awsAccountId, TopicId: t.TopicId })
			);
			viewedTopic = { ...t, ...resp.Topic };
		} catch (e) {
			topicDetailError = describeError(e);
		} finally {
			topicDetailLoading = false;
		}
	}

	// ==================== Namespaces: create / delete / detail ====================
	// No UpdateNamespace op exists in the real API -- CreateNamespace/
	// DescribeNamespace/DeleteNamespace/ListNamespaces only.

	let createNamespaceModal = $state<Modal | null>(null);
	let creatingNamespace = $state(false);
	let createNamespaceError = $state<string | null>(null);
	let newNamespaceName = $state('');
	let newNamespaceIdentityStore = $state<IdentityStore>('QUICKSIGHT');

	function openCreateNamespaceModal(): void {
		createNamespaceError = null;
		newNamespaceName = '';
		newNamespaceIdentityStore = 'QUICKSIGHT';
		createNamespaceModal?.open();
	}

	async function submitCreateNamespace(): Promise<void> {
		if (!newNamespaceName) {
			createNamespaceError = 'Namespace name is required.';
			return;
		}
		creatingNamespace = true;
		createNamespaceError = null;
		try {
			await client().send(
				new CreateNamespaceCommand({
					AwsAccountId: awsAccountId,
					Namespace: newNamespaceName,
					IdentityStore: newNamespaceIdentityStore
				})
			);
			toast.success('Namespace created');
			createNamespaceModal?.close();
			await tabLoader.refresh('namespaces');
		} catch (e) {
			const msg = describeError(e);
			createNamespaceError = msg;
			toast.error(msg);
		} finally {
			creatingNamespace = false;
		}
	}

	async function handleDeleteNamespace(n: NamespaceInfoV2): Promise<void> {
		if (!n.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete namespace',
			message: `Delete namespace ${n.Name}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteNamespaceCommand({ AwsAccountId: awsAccountId, Namespace: n.Name }));
			toast.success('Namespace deleted');
			await tabLoader.refresh('namespaces');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let namespaceDetailModal = $state<Modal | null>(null);
	let viewedNamespace = $state<NamespaceInfoV2 | null>(null);
	let namespaceDetailLoading = $state(false);
	let namespaceDetailError = $state<string | null>(null);

	async function openNamespaceDetail(n: NamespaceInfoV2): Promise<void> {
		viewedNamespace = n;
		namespaceDetailError = null;
		namespaceDetailModal?.open();
		if (!n.Name) return;
		namespaceDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeNamespaceCommand({ AwsAccountId: awsAccountId, Namespace: n.Name })
			);
			viewedNamespace = resp.Namespace ?? n;
		} catch (e) {
			namespaceDetailError = describeError(e);
		} finally {
			namespaceDetailLoading = false;
		}
	}

	// ==================== Groups: create / update / delete / detail ====================

	let createGroupModal = $state<Modal | null>(null);
	let creatingGroup = $state(false);
	let createGroupError = $state<string | null>(null);
	let newGroupName = $state('');
	let newGroupDescription = $state('');

	function openCreateGroupModal(): void {
		createGroupError = null;
		newGroupName = '';
		newGroupDescription = '';
		createGroupModal?.open();
	}

	async function submitCreateGroup(): Promise<void> {
		if (!newGroupName) {
			createGroupError = 'Group name is required.';
			return;
		}
		creatingGroup = true;
		createGroupError = null;
		try {
			await client().send(
				new CreateGroupCommand({
					AwsAccountId: awsAccountId,
					Namespace: namespace,
					GroupName: newGroupName,
					Description: newGroupDescription || undefined
				})
			);
			toast.success('Group created');
			createGroupModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			const msg = describeError(e);
			createGroupError = msg;
			toast.error(msg);
		} finally {
			creatingGroup = false;
		}
	}

	let editGroupModal = $state<Modal | null>(null);
	let editingGroup = $state<Group | null>(null);
	let editingGroupDescription = $state('');
	let savingGroup = $state(false);
	let editGroupError = $state<string | null>(null);

	function openEditGroupModal(g: Group): void {
		editingGroup = g;
		editingGroupDescription = g.Description ?? '';
		editGroupError = null;
		editGroupModal?.open();
	}

	async function submitEditGroup(): Promise<void> {
		if (!editingGroup?.GroupName) return;
		savingGroup = true;
		editGroupError = null;
		try {
			await client().send(
				new UpdateGroupCommand({
					AwsAccountId: awsAccountId,
					Namespace: namespace,
					GroupName: editingGroup.GroupName,
					Description: editingGroupDescription || undefined
				})
			);
			toast.success('Group updated');
			editGroupModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			const msg = describeError(e);
			editGroupError = msg;
			toast.error(msg);
		} finally {
			savingGroup = false;
		}
	}

	async function handleDeleteGroup(g: Group): Promise<void> {
		if (!g.GroupName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete group',
			message: `Delete group ${g.GroupName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteGroupCommand({ AwsAccountId: awsAccountId, Namespace: namespace, GroupName: g.GroupName })
			);
			toast.success('Group deleted');
			await tabLoader.refresh('groups');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let groupDetailModal = $state<Modal | null>(null);
	let viewedGroup = $state<Group | null>(null);
	let groupDetailLoading = $state(false);
	let groupDetailError = $state<string | null>(null);

	async function openGroupDetail(g: Group): Promise<void> {
		viewedGroup = g;
		groupDetailError = null;
		groupDetailModal?.open();
		if (!g.GroupName) return;
		groupDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeGroupCommand({ AwsAccountId: awsAccountId, Namespace: namespace, GroupName: g.GroupName })
			);
			viewedGroup = resp.Group ?? g;
		} catch (e) {
			groupDetailError = describeError(e);
		} finally {
			groupDetailLoading = false;
		}
	}

	// ==================== Users: register / update / delete / detail ====================
	// RegisterUser is the real create op -- there is no "CreateUser".

	let createUserModal = $state<Modal | null>(null);
	let creatingUser = $state(false);
	let createUserError = $state<string | null>(null);
	let newUserName = $state('');
	let newUserEmail = $state('');
	let newUserIdentityType = $state<IdentityType>('QUICKSIGHT');
	let newUserRole = $state<UserRole>('READER');
	let newUserIamArn = $state('');

	function openCreateUserModal(): void {
		createUserError = null;
		newUserName = '';
		newUserEmail = '';
		newUserIdentityType = 'QUICKSIGHT';
		newUserRole = 'READER';
		newUserIamArn = '';
		createUserModal?.open();
	}

	async function submitCreateUser(): Promise<void> {
		if (!newUserEmail || (newUserIdentityType === 'IAM' && !newUserIamArn)) {
			createUserError =
				newUserIdentityType === 'IAM'
					? 'Email and IAM ARN are required.'
					: 'Email is required.';
			return;
		}
		creatingUser = true;
		createUserError = null;
		try {
			await client().send(
				new RegisterUserCommand({
					AwsAccountId: awsAccountId,
					Namespace: namespace,
					IdentityType: newUserIdentityType,
					Email: newUserEmail,
					UserRole: newUserRole,
					UserName: newUserName || undefined,
					IamArn: newUserIdentityType === 'IAM' ? newUserIamArn : undefined
				})
			);
			toast.success('User registered');
			createUserModal?.close();
			await tabLoader.refresh('users');
		} catch (e) {
			const msg = describeError(e);
			createUserError = msg;
			toast.error(msg);
		} finally {
			creatingUser = false;
		}
	}

	let editUserModal = $state<Modal | null>(null);
	let editingUser = $state<User | null>(null);
	let editingUserEmail = $state('');
	let editingUserRole = $state<UserRole>('READER');
	let savingUser = $state(false);
	let editUserError = $state<string | null>(null);

	function openEditUserModal(u: User): void {
		editingUser = u;
		editingUserEmail = u.Email ?? '';
		editingUserRole = u.Role ?? 'READER';
		editUserError = null;
		editUserModal?.open();
	}

	async function submitEditUser(): Promise<void> {
		if (!editingUser?.UserName) return;
		if (!editingUserEmail) {
			editUserError = 'Email is required.';
			return;
		}
		savingUser = true;
		editUserError = null;
		try {
			await client().send(
				new UpdateUserCommand({
					AwsAccountId: awsAccountId,
					Namespace: namespace,
					UserName: editingUser.UserName,
					Email: editingUserEmail,
					Role: editingUserRole
				})
			);
			toast.success('User updated');
			editUserModal?.close();
			await tabLoader.refresh('users');
		} catch (e) {
			const msg = describeError(e);
			editUserError = msg;
			toast.error(msg);
		} finally {
			savingUser = false;
		}
	}

	async function handleDeleteUser(u: User): Promise<void> {
		if (!u.UserName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete user',
			message: `Delete user ${u.UserName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteUserCommand({ AwsAccountId: awsAccountId, Namespace: namespace, UserName: u.UserName })
			);
			toast.success('User deleted');
			await tabLoader.refresh('users');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let userDetailModal = $state<Modal | null>(null);
	let viewedUser = $state<User | null>(null);
	let userDetailLoading = $state(false);
	let userDetailError = $state<string | null>(null);

	async function openUserDetail(u: User): Promise<void> {
		viewedUser = u;
		userDetailError = null;
		userDetailModal?.open();
		if (!u.UserName) return;
		userDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeUserCommand({ AwsAccountId: awsAccountId, Namespace: namespace, UserName: u.UserName })
			);
			viewedUser = resp.User ?? u;
		} catch (e) {
			userDetailError = describeError(e);
		} finally {
			userDetailLoading = false;
		}
	}

	// ==================== IAM Policy Assignments: create / update / delete / detail ====================

	let createIamPolicyAssignmentModal = $state<Modal | null>(null);
	let creatingIamPolicyAssignment = $state(false);
	let createIamPolicyAssignmentError = $state<string | null>(null);
	let newAssignmentName = $state('');
	let newAssignmentStatus = $state<AssignmentStatus>('ENABLED');
	let newAssignmentPolicyArn = $state('');

	function openCreateIamPolicyAssignmentModal(): void {
		createIamPolicyAssignmentError = null;
		newAssignmentName = '';
		newAssignmentStatus = 'ENABLED';
		newAssignmentPolicyArn = '';
		createIamPolicyAssignmentModal?.open();
	}

	async function submitCreateIamPolicyAssignment(): Promise<void> {
		if (!newAssignmentName) {
			createIamPolicyAssignmentError = 'Assignment name is required.';
			return;
		}
		creatingIamPolicyAssignment = true;
		createIamPolicyAssignmentError = null;
		try {
			await client().send(
				new CreateIAMPolicyAssignmentCommand({
					AwsAccountId: awsAccountId,
					Namespace: namespace,
					AssignmentName: newAssignmentName,
					AssignmentStatus: newAssignmentStatus,
					PolicyArn: newAssignmentPolicyArn || undefined
				})
			);
			toast.success('IAM policy assignment created');
			createIamPolicyAssignmentModal?.close();
			await tabLoader.refresh('iamPolicyAssignments');
		} catch (e) {
			const msg = describeError(e);
			createIamPolicyAssignmentError = msg;
			toast.error(msg);
		} finally {
			creatingIamPolicyAssignment = false;
		}
	}

	let editIamPolicyAssignmentModal = $state<Modal | null>(null);
	let editingIamPolicyAssignment = $state<IAMPolicyAssignmentSummary | null>(null);
	let editingAssignmentStatus = $state<AssignmentStatus>('ENABLED');
	let editingAssignmentPolicyArn = $state('');
	let savingIamPolicyAssignment = $state(false);
	let editIamPolicyAssignmentError = $state<string | null>(null);

	function openEditIamPolicyAssignmentModal(a: IAMPolicyAssignmentSummary): void {
		editingIamPolicyAssignment = a;
		editingAssignmentStatus = a.AssignmentStatus ?? 'ENABLED';
		editingAssignmentPolicyArn = '';
		editIamPolicyAssignmentError = null;
		editIamPolicyAssignmentModal?.open();
	}

	async function submitEditIamPolicyAssignment(): Promise<void> {
		if (!editingIamPolicyAssignment?.AssignmentName) return;
		savingIamPolicyAssignment = true;
		editIamPolicyAssignmentError = null;
		try {
			await client().send(
				new UpdateIAMPolicyAssignmentCommand({
					AwsAccountId: awsAccountId,
					Namespace: namespace,
					AssignmentName: editingIamPolicyAssignment.AssignmentName,
					AssignmentStatus: editingAssignmentStatus,
					PolicyArn: editingAssignmentPolicyArn || undefined
				})
			);
			toast.success('IAM policy assignment updated');
			editIamPolicyAssignmentModal?.close();
			await tabLoader.refresh('iamPolicyAssignments');
		} catch (e) {
			const msg = describeError(e);
			editIamPolicyAssignmentError = msg;
			toast.error(msg);
		} finally {
			savingIamPolicyAssignment = false;
		}
	}

	async function handleDeleteIamPolicyAssignment(a: IAMPolicyAssignmentSummary): Promise<void> {
		if (!a.AssignmentName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete IAM policy assignment',
			message: `Delete assignment ${a.AssignmentName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteIAMPolicyAssignmentCommand({
					AwsAccountId: awsAccountId,
					Namespace: namespace,
					AssignmentName: a.AssignmentName
				})
			);
			toast.success('IAM policy assignment deleted');
			await tabLoader.refresh('iamPolicyAssignments');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let iamPolicyAssignmentDetailModal = $state<Modal | null>(null);
	let viewedIamPolicyAssignment = $state<IAMPolicyAssignmentSummary | IAMPolicyAssignment | null>(null);
	let iamPolicyAssignmentDetailLoading = $state(false);
	let iamPolicyAssignmentDetailError = $state<string | null>(null);

	async function openIamPolicyAssignmentDetail(a: IAMPolicyAssignmentSummary): Promise<void> {
		viewedIamPolicyAssignment = a;
		iamPolicyAssignmentDetailError = null;
		iamPolicyAssignmentDetailModal?.open();
		if (!a.AssignmentName) return;
		iamPolicyAssignmentDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeIAMPolicyAssignmentCommand({
					AwsAccountId: awsAccountId,
					Namespace: namespace,
					AssignmentName: a.AssignmentName
				})
			);
			viewedIamPolicyAssignment = resp.IAMPolicyAssignment ?? a;
		} catch (e) {
			iamPolicyAssignmentDetailError = describeError(e);
		} finally {
			iamPolicyAssignmentDetailLoading = false;
		}
	}

	// ==================== Custom Permissions: create / update / delete / detail ====================

	let createCustomPermissionsModal = $state<Modal | null>(null);
	let creatingCustomPermissions = $state(false);
	let createCustomPermissionsError = $state<string | null>(null);
	let newCustomPermissionsName = $state('');

	function openCreateCustomPermissionsModal(): void {
		createCustomPermissionsError = null;
		newCustomPermissionsName = '';
		createCustomPermissionsModal?.open();
	}

	async function submitCreateCustomPermissions(): Promise<void> {
		if (!newCustomPermissionsName) {
			createCustomPermissionsError = 'Custom permissions name is required.';
			return;
		}
		creatingCustomPermissions = true;
		createCustomPermissionsError = null;
		try {
			await client().send(
				new CreateCustomPermissionsCommand({
					AwsAccountId: awsAccountId,
					CustomPermissionsName: newCustomPermissionsName
				})
			);
			toast.success('Custom permissions created');
			createCustomPermissionsModal?.close();
			await tabLoader.refresh('customPermissions');
		} catch (e) {
			const msg = describeError(e);
			createCustomPermissionsError = msg;
			toast.error(msg);
		} finally {
			creatingCustomPermissions = false;
		}
	}

	async function handleDeleteCustomPermissions(c: CustomPermissionsDetail): Promise<void> {
		if (!c.CustomPermissionsName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete custom permissions',
			message: `Delete custom permissions ${c.CustomPermissionsName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteCustomPermissionsCommand({
					AwsAccountId: awsAccountId,
					CustomPermissionsName: c.CustomPermissionsName
				})
			);
			toast.success('Custom permissions deleted');
			await tabLoader.refresh('customPermissions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let customPermissionsDetailModal = $state<Modal | null>(null);
	let viewedCustomPermissions = $state<CustomPermissionsDetail | null>(null);
	let customPermissionsDetailLoading = $state(false);
	let customPermissionsDetailError = $state<string | null>(null);

	async function openCustomPermissionsDetail(c: CustomPermissionsDetail): Promise<void> {
		viewedCustomPermissions = c;
		customPermissionsDetailError = null;
		customPermissionsDetailModal?.open();
		if (!c.CustomPermissionsName) return;
		customPermissionsDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeCustomPermissionsCommand({
					AwsAccountId: awsAccountId,
					CustomPermissionsName: c.CustomPermissionsName
				})
			);
			viewedCustomPermissions = resp.CustomPermissions ?? c;
		} catch (e) {
			customPermissionsDetailError = describeError(e);
		} finally {
			customPermissionsDetailLoading = false;
		}
	}

	// ==================== Brands: create / update / delete / detail ====================

	let createBrandModal = $state<Modal | null>(null);
	let creatingBrand = $state(false);
	let createBrandError = $state<string | null>(null);
	let newBrandId = $state('');
	let newBrandDefinition = $state('');

	function openCreateBrandModal(): void {
		createBrandError = null;
		newBrandId = '';
		newBrandDefinition = '';
		createBrandModal?.open();
	}

	async function submitCreateBrand(): Promise<void> {
		if (!newBrandId) {
			createBrandError = 'Brand ID is required.';
			return;
		}
		let brandDefinition: BrandDefinition | undefined;
		if (newBrandDefinition.trim()) {
			try {
				brandDefinition = JSON.parse(newBrandDefinition) as BrandDefinition;
			} catch {
				createBrandError = 'Brand definition must be valid JSON.';
				return;
			}
		}
		creatingBrand = true;
		createBrandError = null;
		try {
			await client().send(
				new CreateBrandCommand({
					AwsAccountId: awsAccountId,
					BrandId: newBrandId,
					BrandDefinition: brandDefinition
				})
			);
			toast.success('Brand created');
			createBrandModal?.close();
			await tabLoader.refresh('brands');
		} catch (e) {
			const msg = describeError(e);
			createBrandError = msg;
			toast.error(msg);
		} finally {
			creatingBrand = false;
		}
	}

	let editBrandModal = $state<Modal | null>(null);
	let editingBrand = $state<BrandSummary | null>(null);
	let editingBrandDefinition = $state('');
	let savingBrand = $state(false);
	let editBrandError = $state<string | null>(null);

	function openEditBrandModal(b: BrandSummary): void {
		editingBrand = b;
		editingBrandDefinition = '';
		editBrandError = null;
		editBrandModal?.open();
	}

	async function submitEditBrand(): Promise<void> {
		if (!editingBrand?.BrandId) return;
		if (!editingBrandDefinition.trim()) {
			editBrandError = 'Brand definition is required.';
			return;
		}
		let brandDefinition: BrandDefinition;
		try {
			brandDefinition = JSON.parse(editingBrandDefinition) as BrandDefinition;
		} catch {
			editBrandError = 'Brand definition must be valid JSON.';
			return;
		}
		savingBrand = true;
		editBrandError = null;
		try {
			await client().send(
				new UpdateBrandCommand({
					AwsAccountId: awsAccountId,
					BrandId: editingBrand.BrandId,
					BrandDefinition: brandDefinition
				})
			);
			toast.success('Brand updated');
			editBrandModal?.close();
			await tabLoader.refresh('brands');
		} catch (e) {
			const msg = describeError(e);
			editBrandError = msg;
			toast.error(msg);
		} finally {
			savingBrand = false;
		}
	}

	async function handleDeleteBrand(b: BrandSummary): Promise<void> {
		if (!b.BrandId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete brand',
			message: `Delete brand ${b.BrandName ?? b.BrandId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteBrandCommand({ AwsAccountId: awsAccountId, BrandId: b.BrandId }));
			toast.success('Brand deleted');
			await tabLoader.refresh('brands');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let brandDetailModal = $state<Modal | null>(null);
	let viewedBrand = $state<(BrandSummary & Partial<BrandDetail>) | null>(null);
	let brandDetailLoading = $state(false);
	let brandDetailError = $state<string | null>(null);

	async function openBrandDetail(b: BrandSummary): Promise<void> {
		viewedBrand = b;
		brandDetailError = null;
		brandDetailModal?.open();
		if (!b.BrandId) return;
		brandDetailLoading = true;
		try {
			const resp = await client().send(new DescribeBrandCommand({ AwsAccountId: awsAccountId, BrandId: b.BrandId }));
			viewedBrand = { ...b, ...resp.BrandDetail };
		} catch (e) {
			brandDetailError = describeError(e);
		} finally {
			brandDetailLoading = false;
		}
	}

	// ==================== Action Connectors: create / update / delete / detail ====================

	const actionConnectorTypes: ActionConnectorType[] = [
		'GENERIC_HTTP',
		'AMAZON_S3',
		'AMAZON_BEDROCK_RUNTIME',
		'SLACK',
		'JIRA_CLOUD',
		'SALESFORCE_CRM',
		'ZENDESK_SUITE'
	];

	let createActionConnectorModal = $state<Modal | null>(null);
	let creatingActionConnector = $state(false);
	let createActionConnectorError = $state<string | null>(null);
	let newActionConnectorId = $state('');
	let newActionConnectorName = $state('');
	let newActionConnectorType = $state<ActionConnectorType>('GENERIC_HTTP');
	let newActionConnectorAuthConfig = $state('{}');
	let newActionConnectorDescription = $state('');

	function openCreateActionConnectorModal(): void {
		createActionConnectorError = null;
		newActionConnectorId = '';
		newActionConnectorName = '';
		newActionConnectorType = 'GENERIC_HTTP';
		newActionConnectorAuthConfig = '{}';
		newActionConnectorDescription = '';
		createActionConnectorModal?.open();
	}

	async function submitCreateActionConnector(): Promise<void> {
		if (!newActionConnectorId || !newActionConnectorName || !newActionConnectorAuthConfig.trim()) {
			createActionConnectorError = 'ID, name, and authentication config are required.';
			return;
		}
		let authenticationConfig: AuthConfig;
		try {
			authenticationConfig = JSON.parse(newActionConnectorAuthConfig) as AuthConfig;
		} catch {
			createActionConnectorError = 'Authentication config must be valid JSON.';
			return;
		}
		creatingActionConnector = true;
		createActionConnectorError = null;
		try {
			await client().send(
				new CreateActionConnectorCommand({
					AwsAccountId: awsAccountId,
					ActionConnectorId: newActionConnectorId,
					Name: newActionConnectorName,
					Type: newActionConnectorType,
					AuthenticationConfig: authenticationConfig,
					Description: newActionConnectorDescription || undefined
				})
			);
			toast.success('Action connector created');
			createActionConnectorModal?.close();
			await tabLoader.refresh('actionConnectors');
		} catch (e) {
			const msg = describeError(e);
			createActionConnectorError = msg;
			toast.error(msg);
		} finally {
			creatingActionConnector = false;
		}
	}

	let editActionConnectorModal = $state<Modal | null>(null);
	let editingActionConnector = $state<ActionConnectorSummary | null>(null);
	let editingActionConnectorName = $state('');
	let editingActionConnectorAuthConfig = $state('{}');
	let editingActionConnectorDescription = $state('');
	let savingActionConnector = $state(false);
	let editActionConnectorError = $state<string | null>(null);

	function openEditActionConnectorModal(a: ActionConnectorSummary): void {
		editingActionConnector = a;
		editingActionConnectorName = a.Name ?? '';
		editingActionConnectorAuthConfig = '{}';
		editingActionConnectorDescription = '';
		editActionConnectorError = null;
		editActionConnectorModal?.open();
	}

	async function submitEditActionConnector(): Promise<void> {
		if (!editingActionConnector?.ActionConnectorId) return;
		if (!editingActionConnectorName || !editingActionConnectorAuthConfig.trim()) {
			editActionConnectorError = 'Name and authentication config are required.';
			return;
		}
		let authenticationConfig: AuthConfig;
		try {
			authenticationConfig = JSON.parse(editingActionConnectorAuthConfig) as AuthConfig;
		} catch {
			editActionConnectorError = 'Authentication config must be valid JSON.';
			return;
		}
		savingActionConnector = true;
		editActionConnectorError = null;
		try {
			await client().send(
				new UpdateActionConnectorCommand({
					AwsAccountId: awsAccountId,
					ActionConnectorId: editingActionConnector.ActionConnectorId,
					Name: editingActionConnectorName,
					AuthenticationConfig: authenticationConfig,
					Description: editingActionConnectorDescription || undefined
				})
			);
			toast.success('Action connector updated');
			editActionConnectorModal?.close();
			await tabLoader.refresh('actionConnectors');
		} catch (e) {
			const msg = describeError(e);
			editActionConnectorError = msg;
			toast.error(msg);
		} finally {
			savingActionConnector = false;
		}
	}

	async function handleDeleteActionConnector(a: ActionConnectorSummary): Promise<void> {
		if (!a.ActionConnectorId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete action connector',
			message: `Delete action connector ${a.Name ?? a.ActionConnectorId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteActionConnectorCommand({
					AwsAccountId: awsAccountId,
					ActionConnectorId: a.ActionConnectorId
				})
			);
			toast.success('Action connector deleted');
			await tabLoader.refresh('actionConnectors');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let actionConnectorDetailModal = $state<Modal | null>(null);
	let viewedActionConnector = $state<ActionConnectorSummary | ActionConnector | null>(null);
	let actionConnectorDetailLoading = $state(false);
	let actionConnectorDetailError = $state<string | null>(null);

	async function openActionConnectorDetail(a: ActionConnectorSummary): Promise<void> {
		viewedActionConnector = a;
		actionConnectorDetailError = null;
		actionConnectorDetailModal?.open();
		if (!a.ActionConnectorId) return;
		actionConnectorDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeActionConnectorCommand({
					AwsAccountId: awsAccountId,
					ActionConnectorId: a.ActionConnectorId
				})
			);
			viewedActionConnector = resp.ActionConnector ?? a;
		} catch (e) {
			actionConnectorDetailError = describeError(e);
		} finally {
			actionConnectorDetailLoading = false;
		}
	}

	// ==================== Agents: create / update / delete / detail ====================

	let createAgentModal = $state<Modal | null>(null);
	let creatingAgent = $state(false);
	let createAgentError = $state<string | null>(null);
	let newAgentId = $state('');
	let newAgentName = $state('');
	let newAgentDescription = $state('');

	function openCreateAgentModal(): void {
		createAgentError = null;
		newAgentId = '';
		newAgentName = '';
		newAgentDescription = '';
		createAgentModal?.open();
	}

	async function submitCreateAgent(): Promise<void> {
		if (!newAgentId || !newAgentName) {
			createAgentError = 'Agent ID and name are required.';
			return;
		}
		creatingAgent = true;
		createAgentError = null;
		try {
			await client().send(
				new CreateAgentCommand({
					AwsAccountId: awsAccountId,
					AgentId: newAgentId,
					Name: newAgentName,
					Description: newAgentDescription || undefined
				})
			);
			toast.success('Agent created');
			createAgentModal?.close();
			await tabLoader.refresh('agents');
		} catch (e) {
			const msg = describeError(e);
			createAgentError = msg;
			toast.error(msg);
		} finally {
			creatingAgent = false;
		}
	}

	let editAgentModal = $state<Modal | null>(null);
	let editingAgent = $state<AgentSummary | null>(null);
	let editingAgentName = $state('');
	let editingAgentDescription = $state('');
	let savingAgent = $state(false);
	let editAgentError = $state<string | null>(null);

	function openEditAgentModal(a: AgentSummary): void {
		editingAgent = a;
		editingAgentName = a.Name ?? '';
		editingAgentDescription = a.Description ?? '';
		editAgentError = null;
		editAgentModal?.open();
	}

	async function submitEditAgent(): Promise<void> {
		if (!editingAgent?.AgentId) return;
		if (!editingAgentName) {
			editAgentError = 'Name is required.';
			return;
		}
		savingAgent = true;
		editAgentError = null;
		try {
			await client().send(
				new UpdateAgentCommand({
					AwsAccountId: awsAccountId,
					AgentId: editingAgent.AgentId,
					Name: editingAgentName,
					Description: editingAgentDescription || undefined
				})
			);
			toast.success('Agent updated');
			editAgentModal?.close();
			await tabLoader.refresh('agents');
		} catch (e) {
			const msg = describeError(e);
			editAgentError = msg;
			toast.error(msg);
		} finally {
			savingAgent = false;
		}
	}

	async function handleDeleteAgent(a: AgentSummary): Promise<void> {
		if (!a.AgentId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete agent',
			message: `Delete agent ${a.Name ?? a.AgentId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAgentCommand({ AwsAccountId: awsAccountId, AgentId: a.AgentId }));
			toast.success('Agent deleted');
			await tabLoader.refresh('agents');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let agentDetailModal = $state<Modal | null>(null);
	let viewedAgent = $state<AgentSummary | Agent | null>(null);
	let agentDetailLoading = $state(false);
	let agentDetailError = $state<string | null>(null);

	async function openAgentDetail(a: AgentSummary): Promise<void> {
		viewedAgent = a;
		agentDetailError = null;
		agentDetailModal?.open();
		if (!a.AgentId) return;
		agentDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeAgentCommand({ AwsAccountId: awsAccountId, AgentId: a.AgentId })
			);
			viewedAgent = resp.Agent ?? a;
		} catch (e) {
			agentDetailError = describeError(e);
		} finally {
			agentDetailLoading = false;
		}
	}

	// ==================== Knowledge Bases: create / update / delete / detail ====================

	let createKnowledgeBaseModal = $state<Modal | null>(null);
	let creatingKnowledgeBase = $state(false);
	let createKnowledgeBaseError = $state<string | null>(null);
	let newKnowledgeBaseId = $state('');
	let newKnowledgeBaseName = $state('');
	let newKnowledgeBaseDataSourceArn = $state('');
	let newKnowledgeBaseConfiguration = $state('{}');

	function openCreateKnowledgeBaseModal(): void {
		createKnowledgeBaseError = null;
		newKnowledgeBaseId = '';
		newKnowledgeBaseName = '';
		newKnowledgeBaseDataSourceArn = '';
		newKnowledgeBaseConfiguration = '{}';
		createKnowledgeBaseModal?.open();
	}

	async function submitCreateKnowledgeBase(): Promise<void> {
		if (
			!newKnowledgeBaseId ||
			!newKnowledgeBaseName ||
			!newKnowledgeBaseDataSourceArn ||
			!newKnowledgeBaseConfiguration.trim()
		) {
			createKnowledgeBaseError = 'ID, name, data source ARN, and configuration are required.';
			return;
		}
		let configuration: KnowledgeBaseConfiguration;
		try {
			configuration = JSON.parse(newKnowledgeBaseConfiguration) as KnowledgeBaseConfiguration;
		} catch {
			createKnowledgeBaseError = 'Configuration must be valid JSON.';
			return;
		}
		creatingKnowledgeBase = true;
		createKnowledgeBaseError = null;
		try {
			await client().send(
				new CreateKnowledgeBaseCommand({
					AwsAccountId: awsAccountId,
					KnowledgeBaseId: newKnowledgeBaseId,
					Name: newKnowledgeBaseName,
					DataSourceArn: newKnowledgeBaseDataSourceArn,
					KnowledgeBaseConfiguration: configuration
				})
			);
			toast.success('Knowledge base created');
			createKnowledgeBaseModal?.close();
			await tabLoader.refresh('knowledgeBases');
		} catch (e) {
			const msg = describeError(e);
			createKnowledgeBaseError = msg;
			toast.error(msg);
		} finally {
			creatingKnowledgeBase = false;
		}
	}

	let editKnowledgeBaseModal = $state<Modal | null>(null);
	let editingKnowledgeBase = $state<KnowledgeBaseSummary | null>(null);
	let editingKnowledgeBaseName = $state('');
	let editingKnowledgeBaseDescription = $state('');
	let savingKnowledgeBase = $state(false);
	let editKnowledgeBaseError = $state<string | null>(null);

	function openEditKnowledgeBaseModal(k: KnowledgeBaseSummary): void {
		editingKnowledgeBase = k;
		editingKnowledgeBaseName = k.Name ?? '';
		editingKnowledgeBaseDescription = '';
		editKnowledgeBaseError = null;
		editKnowledgeBaseModal?.open();
	}

	async function submitEditKnowledgeBase(): Promise<void> {
		if (!editingKnowledgeBase?.KnowledgeBaseId) return;
		if (!editingKnowledgeBaseName) {
			editKnowledgeBaseError = 'Name is required.';
			return;
		}
		savingKnowledgeBase = true;
		editKnowledgeBaseError = null;
		try {
			await client().send(
				new UpdateKnowledgeBaseCommand({
					AwsAccountId: awsAccountId,
					KnowledgeBaseId: editingKnowledgeBase.KnowledgeBaseId,
					Name: editingKnowledgeBaseName,
					Description: editingKnowledgeBaseDescription || undefined
				})
			);
			toast.success('Knowledge base updated');
			editKnowledgeBaseModal?.close();
			await tabLoader.refresh('knowledgeBases');
		} catch (e) {
			const msg = describeError(e);
			editKnowledgeBaseError = msg;
			toast.error(msg);
		} finally {
			savingKnowledgeBase = false;
		}
	}

	async function handleDeleteKnowledgeBase(k: KnowledgeBaseSummary): Promise<void> {
		if (!k.KnowledgeBaseId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete knowledge base',
			message: `Delete knowledge base ${k.Name ?? k.KnowledgeBaseId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteKnowledgeBaseCommand({
					AwsAccountId: awsAccountId,
					KnowledgeBaseId: k.KnowledgeBaseId
				})
			);
			toast.success('Knowledge base deleted');
			await tabLoader.refresh('knowledgeBases');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let knowledgeBaseDetailModal = $state<Modal | null>(null);
	let viewedKnowledgeBase = $state<KnowledgeBaseSummary | KnowledgeBase | null>(null);
	let knowledgeBaseDetailLoading = $state(false);
	let knowledgeBaseDetailError = $state<string | null>(null);

	async function openKnowledgeBaseDetail(k: KnowledgeBaseSummary): Promise<void> {
		viewedKnowledgeBase = k;
		knowledgeBaseDetailError = null;
		knowledgeBaseDetailModal?.open();
		if (!k.KnowledgeBaseId) return;
		knowledgeBaseDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeKnowledgeBaseCommand({
					AwsAccountId: awsAccountId,
					KnowledgeBaseId: k.KnowledgeBaseId
				})
			);
			viewedKnowledgeBase = resp.KnowledgeBase ?? k;
		} catch (e) {
			knowledgeBaseDetailError = describeError(e);
		} finally {
			knowledgeBaseDetailLoading = false;
		}
	}

	// ==================== Spaces: create / update / delete / detail ====================
	// Space's wire shape is uniquely camelCase (spaceId/name/...), not PascalCase like
	// every other family here -- confirmed in services/quicksight/PARITY.md.

	let createSpaceModal = $state<Modal | null>(null);
	let creatingSpace = $state(false);
	let createSpaceError = $state<string | null>(null);
	let newSpaceId = $state('');
	let newSpaceName = $state('');
	let newSpaceDescription = $state('');

	function openCreateSpaceModal(): void {
		createSpaceError = null;
		newSpaceId = '';
		newSpaceName = '';
		newSpaceDescription = '';
		createSpaceModal?.open();
	}

	async function submitCreateSpace(): Promise<void> {
		if (!newSpaceId || !newSpaceName) {
			createSpaceError = 'Space ID and name are required.';
			return;
		}
		creatingSpace = true;
		createSpaceError = null;
		try {
			await client().send(
				new CreateSpaceCommand({
					AwsAccountId: awsAccountId,
					SpaceId: newSpaceId,
					Name: newSpaceName,
					Description: newSpaceDescription || undefined
				})
			);
			toast.success('Space created');
			createSpaceModal?.close();
			await tabLoader.refresh('spaces');
		} catch (e) {
			const msg = describeError(e);
			createSpaceError = msg;
			toast.error(msg);
		} finally {
			creatingSpace = false;
		}
	}

	let editSpaceModal = $state<Modal | null>(null);
	let editingSpace = $state<SpaceSummary | null>(null);
	let editingSpaceName = $state('');
	let editingSpaceDescription = $state('');
	let savingSpace = $state(false);
	let editSpaceError = $state<string | null>(null);

	function openEditSpaceModal(s: SpaceSummary): void {
		editingSpace = s;
		editingSpaceName = s.name ?? '';
		editingSpaceDescription = s.description ?? '';
		editSpaceError = null;
		editSpaceModal?.open();
	}

	async function submitEditSpace(): Promise<void> {
		if (!editingSpace?.spaceId) return;
		if (!editingSpaceName) {
			editSpaceError = 'Name is required.';
			return;
		}
		savingSpace = true;
		editSpaceError = null;
		try {
			await client().send(
				new UpdateSpaceCommand({
					AwsAccountId: awsAccountId,
					SpaceId: editingSpace.spaceId,
					Name: editingSpaceName,
					Description: editingSpaceDescription || undefined
				})
			);
			toast.success('Space updated');
			editSpaceModal?.close();
			await tabLoader.refresh('spaces');
		} catch (e) {
			const msg = describeError(e);
			editSpaceError = msg;
			toast.error(msg);
		} finally {
			savingSpace = false;
		}
	}

	async function handleDeleteSpace(s: SpaceSummary): Promise<void> {
		if (!s.spaceId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete space',
			message: `Delete space ${s.name ?? s.spaceId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteSpaceCommand({ AwsAccountId: awsAccountId, SpaceId: s.spaceId }));
			toast.success('Space deleted');
			await tabLoader.refresh('spaces');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let spaceDetailModal = $state<Modal | null>(null);
	let viewedSpace = $state<(SpaceSummary & Partial<SpaceDetails>) | null>(null);
	let spaceDetailLoading = $state(false);
	let spaceDetailError = $state<string | null>(null);

	async function openSpaceDetail(s: SpaceSummary): Promise<void> {
		viewedSpace = s;
		spaceDetailError = null;
		spaceDetailModal?.open();
		if (!s.spaceId) return;
		spaceDetailLoading = true;
		try {
			const resp = await client().send(new DescribeSpaceCommand({ AwsAccountId: awsAccountId, SpaceId: s.spaceId }));
			viewedSpace = { ...s, ...resp.Space };
		} catch (e) {
			spaceDetailError = describeError(e);
		} finally {
			spaceDetailLoading = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={BarChart3}
		title="Amazon QuickSight"
		description="Business intelligence dashboards"
		onRefresh={handleRefresh}
		color="blue"
	>
		{#snippet actions()}
			{#if activeTab === 'dashboards'}
				<button
					onclick={openCreateDashboardModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create dashboard
				</button>
			{:else if activeTab === 'analyses'}
				<button
					onclick={openCreateAnalysisModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create analysis
				</button>
			{:else if activeTab === 'datasets'}
				<button
					onclick={openCreateDataSetModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create data set
				</button>
			{:else if activeTab === 'datasources'}
				<button
					onclick={openCreateDataSourceModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create data source
				</button>
			{:else if activeTab === 'folders'}
				<button
					onclick={openCreateFolderModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create folder
				</button>
			{:else if activeTab === 'vpcConnections'}
				<button
					onclick={openCreateVpcConnectionModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create VPC connection
				</button>
			{:else if activeTab === 'templates'}
				<button
					onclick={openCreateTemplateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create template
				</button>
			{:else if activeTab === 'themes'}
				<button
					onclick={openCreateThemeModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create theme
				</button>
			{:else if activeTab === 'topics'}
				<button
					onclick={openCreateTopicModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create topic
				</button>
			{:else if activeTab === 'namespaces'}
				<button
					onclick={openCreateNamespaceModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create namespace
				</button>
			{:else if activeTab === 'groups'}
				<button
					onclick={openCreateGroupModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create group
				</button>
			{:else if activeTab === 'users'}
				<button
					onclick={openCreateUserModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Register user
				</button>
			{:else if activeTab === 'iamPolicyAssignments'}
				<button
					onclick={openCreateIamPolicyAssignmentModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create assignment
				</button>
			{:else if activeTab === 'customPermissions'}
				<button
					onclick={openCreateCustomPermissionsModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create custom permissions
				</button>
			{:else if activeTab === 'brands'}
				<button
					onclick={openCreateBrandModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create brand
				</button>
			{:else if activeTab === 'actionConnectors'}
				<button
					onclick={openCreateActionConnectorModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create action connector
				</button>
			{:else if activeTab === 'agents'}
				<button
					onclick={openCreateAgentModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create agent
				</button>
			{:else if activeTab === 'knowledgeBases'}
				<button
					onclick={openCreateKnowledgeBaseModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create knowledge base
				</button>
			{:else if activeTab === 'spaces'}
				<button
					onclick={openCreateSpaceModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create space
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
			<div class="flex items-center gap-3">
				{#if activeTab === 'groups' || activeTab === 'users' || activeTab === 'iamPolicyAssignments'}
					<div class="flex items-center gap-1.5">
						<label for="namespace-input" class="text-xs text-slate-500 dark:text-slate-400">Namespace</label>
						<input
							id="namespace-input"
							bind:value={namespace}
							onblur={handleNamespaceChange}
							onkeydown={(e) => e.key === 'Enter' && handleNamespaceChange()}
							class="w-28 px-2 py-1.5 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				{/if}
				<SearchInput bind:value={searchQuery} />
			</div>
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'dashboards'}
				{#snippet dashboardVersionCell(d: DashboardSummary)}
					{d.PublishedVersionNumber ?? '—'}
				{/snippet}
				{#snippet dashboardUpdatedCell(d: DashboardSummary)}
					{formatDate(d.LastUpdatedTime)}
				{/snippet}
				{#snippet dashboardActionsCell(d: DashboardSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openDashboardDetail(d)}
							title="View"
							aria-label="View dashboard {d.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditDashboardModal(d)}
							title="Edit"
							aria-label="Edit dashboard {d.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteDashboard(d)}
							title="Delete"
							aria-label="Delete dashboard {d.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const dashboardColumns = defineColumns<DashboardSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'DashboardId', label: 'ID' },
					{ key: 'PublishedVersionNumber', label: 'Version', render: dashboardVersionCell },
					{ key: 'LastUpdatedTime', label: 'Updated', render: dashboardUpdatedCell },
					{ key: 'actions', label: '', render: dashboardActionsCell }
				])}
				<DataTable
					rows={filteredDashboards}
					rowKey={(d) => d.DashboardId ?? ''}
					columns={dashboardColumns}
					loading={tabLoader.isLoading('dashboards')}
					emptyMessage="No dashboards found"
				/>
				<LoadMore
					hasMore={!!dashboardsNextToken}
					loading={loadingMoreDashboards}
					onLoadMore={loadMoreDashboards}
				/>
			{:else if activeTab === 'analyses'}
				{#snippet analysisStatusCell(a: AnalysisSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(a.Status === 'CREATION_SUCCESSFUL')}"
						>{a.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet analysisUpdatedCell(a: AnalysisSummary)}
					{formatDate(a.LastUpdatedTime)}
				{/snippet}
				{#snippet analysisActionsCell(a: AnalysisSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openAnalysisDetail(a)}
							title="View"
							aria-label="View analysis {a.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditAnalysisModal(a)}
							title="Edit"
							aria-label="Edit analysis {a.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteAnalysis(a)}
							title="Delete"
							aria-label="Delete analysis {a.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const analysisColumns = defineColumns<AnalysisSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'AnalysisId', label: 'ID' },
					{ key: 'Status', label: 'Status', render: analysisStatusCell },
					{ key: 'LastUpdatedTime', label: 'Updated', render: analysisUpdatedCell },
					{ key: 'actions', label: '', render: analysisActionsCell }
				])}
				<DataTable
					rows={filteredAnalyses}
					rowKey={(a) => a.AnalysisId ?? ''}
					columns={analysisColumns}
					loading={tabLoader.isLoading('analyses')}
					emptyMessage="No analyses found"
				/>
				<LoadMore
					hasMore={!!analysesNextToken}
					loading={loadingMoreAnalyses}
					onLoadMore={loadMoreAnalyses}
				/>
			{:else if activeTab === 'datasets'}
				{#snippet dataSetUpdatedCell(ds: DataSetSummary)}
					{formatDate(ds.LastUpdatedTime)}
				{/snippet}
				{#snippet dataSetActionsCell(ds: DataSetSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openDataSetDetail(ds)}
							title="View"
							aria-label="View data set {ds.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditDataSetModal(ds)}
							title="Edit"
							aria-label="Edit data set {ds.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteDataSet(ds)}
							title="Delete"
							aria-label="Delete data set {ds.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const dataSetColumns = defineColumns<DataSetSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'DataSetId', label: 'ID' },
					{ key: 'ImportMode', label: 'Import Mode' },
					{ key: 'LastUpdatedTime', label: 'Updated', render: dataSetUpdatedCell },
					{ key: 'actions', label: '', render: dataSetActionsCell }
				])}
				<DataTable
					rows={filteredDataSets}
					rowKey={(ds) => ds.DataSetId ?? ''}
					columns={dataSetColumns}
					loading={tabLoader.isLoading('datasets')}
					emptyMessage="No data sets found"
				/>
				<LoadMore
					hasMore={!!dataSetsNextToken}
					loading={loadingMoreDataSets}
					onLoadMore={loadMoreDataSets}
				/>
			{:else if activeTab === 'datasources'}
				{#snippet dataSourceStatusCell(ds: DataSource)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(ds.Status === 'CREATION_SUCCESSFUL')}"
						>{ds.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet dataSourceUpdatedCell(ds: DataSource)}
					{formatDate(ds.LastUpdatedTime)}
				{/snippet}
				{#snippet dataSourceActionsCell(ds: DataSource)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openDataSourceDetail(ds)}
							title="View"
							aria-label="View data source {ds.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditDataSourceModal(ds)}
							title="Edit"
							aria-label="Edit data source {ds.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteDataSource(ds)}
							title="Delete"
							aria-label="Delete data source {ds.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const dataSourceColumns = defineColumns<DataSource>([
					{ key: 'Name', label: 'Name' },
					{ key: 'DataSourceId', label: 'ID' },
					{ key: 'Type', label: 'Type' },
					{ key: 'Status', label: 'Status', render: dataSourceStatusCell },
					{ key: 'LastUpdatedTime', label: 'Updated', render: dataSourceUpdatedCell },
					{ key: 'actions', label: '', render: dataSourceActionsCell }
				])}
				<DataTable
					rows={filteredDataSources}
					rowKey={(ds) => ds.DataSourceId ?? ''}
					columns={dataSourceColumns}
					loading={tabLoader.isLoading('datasources')}
					emptyMessage="No data sources found"
				/>
				<LoadMore
					hasMore={!!dataSourcesNextToken}
					loading={loadingMoreDataSources}
					onLoadMore={loadMoreDataSources}
				/>
			{:else if activeTab === 'folders'}
				{#snippet folderUpdatedCell(f: FolderSummary)}
					{formatDate(f.LastUpdatedTime)}
				{/snippet}
				{#snippet folderActionsCell(f: FolderSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openFolderDetail(f)}
							title="View"
							aria-label="View folder {f.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditFolderModal(f)}
							title="Edit"
							aria-label="Edit folder {f.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteFolder(f)}
							title="Delete"
							aria-label="Delete folder {f.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const folderColumns = defineColumns<FolderSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'FolderId', label: 'ID' },
					{ key: 'FolderType', label: 'Type' },
					{ key: 'LastUpdatedTime', label: 'Updated', render: folderUpdatedCell },
					{ key: 'actions', label: '', render: folderActionsCell }
				])}
				<DataTable
					rows={filteredFolders}
					rowKey={(f) => f.FolderId ?? ''}
					columns={folderColumns}
					loading={tabLoader.isLoading('folders')}
					emptyMessage="No folders found"
				/>
				<LoadMore
					hasMore={!!foldersNextToken}
					loading={loadingMoreFolders}
					onLoadMore={loadMoreFolders}
				/>
			{:else if activeTab === 'vpcConnections'}
				{#snippet vpcStatusCell(v: VPCConnectionSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(v.Status === 'CREATION_SUCCESSFUL')}"
						>{v.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet vpcUpdatedCell(v: VPCConnectionSummary)}
					{formatDate(v.LastUpdatedTime)}
				{/snippet}
				{#snippet vpcActionsCell(v: VPCConnectionSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openVpcConnectionDetail(v)}
							title="View"
							aria-label="View VPC connection {v.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditVpcConnectionModal(v)}
							title="Edit"
							aria-label="Edit VPC connection {v.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteVpcConnection(v)}
							title="Delete"
							aria-label="Delete VPC connection {v.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const vpcColumns = defineColumns<VPCConnectionSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'VPCConnectionId', label: 'ID' },
					{ key: 'Status', label: 'Status', render: vpcStatusCell },
					{ key: 'LastUpdatedTime', label: 'Updated', render: vpcUpdatedCell },
					{ key: 'actions', label: '', render: vpcActionsCell }
				])}
				<DataTable
					rows={filteredVpcConnections}
					rowKey={(v) => v.VPCConnectionId ?? ''}
					columns={vpcColumns}
					loading={tabLoader.isLoading('vpcConnections')}
					emptyMessage="No VPC connections found"
				/>
				<LoadMore
					hasMore={!!vpcConnectionsNextToken}
					loading={loadingMoreVpcConnections}
					onLoadMore={loadMoreVpcConnections}
				/>
			{:else if activeTab === 'templates'}
				{#snippet templateUpdatedCell(t: TemplateSummary)}
					{formatDate(t.LastUpdatedTime)}
				{/snippet}
				{#snippet templateActionsCell(t: TemplateSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openTemplateDetail(t)}
							title="View"
							aria-label="View template {t.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditTemplateModal(t)}
							title="Edit"
							aria-label="Edit template {t.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteTemplate(t)}
							title="Delete"
							aria-label="Delete template {t.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const templateColumns = defineColumns<TemplateSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'TemplateId', label: 'ID' },
					{ key: 'LatestVersionNumber', label: 'Version' },
					{ key: 'LastUpdatedTime', label: 'Updated', render: templateUpdatedCell },
					{ key: 'actions', label: '', render: templateActionsCell }
				])}
				<DataTable
					rows={filteredTemplates}
					rowKey={(t) => t.TemplateId ?? ''}
					columns={templateColumns}
					loading={tabLoader.isLoading('templates')}
					emptyMessage="No templates found"
				/>
				<LoadMore hasMore={!!templatesNextToken} loading={loadingMoreTemplates} onLoadMore={loadMoreTemplates} />
			{:else if activeTab === 'themes'}
				{#snippet themeUpdatedCell(t: ThemeSummary)}
					{formatDate(t.LastUpdatedTime)}
				{/snippet}
				{#snippet themeActionsCell(t: ThemeSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openThemeDetail(t)}
							title="View"
							aria-label="View theme {t.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditThemeModal(t)}
							title="Edit"
							aria-label="Edit theme {t.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteTheme(t)}
							title="Delete"
							aria-label="Delete theme {t.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const themeColumns = defineColumns<ThemeSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'ThemeId', label: 'ID' },
					{ key: 'LatestVersionNumber', label: 'Version' },
					{ key: 'LastUpdatedTime', label: 'Updated', render: themeUpdatedCell },
					{ key: 'actions', label: '', render: themeActionsCell }
				])}
				<DataTable
					rows={filteredThemes}
					rowKey={(t) => t.ThemeId ?? ''}
					columns={themeColumns}
					loading={tabLoader.isLoading('themes')}
					emptyMessage="No themes found"
				/>
				<LoadMore hasMore={!!themesNextToken} loading={loadingMoreThemes} onLoadMore={loadMoreThemes} />
			{:else if activeTab === 'topics'}
				{#snippet topicActionsCell(t: TopicSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openTopicDetail(t)}
							title="View"
							aria-label="View topic {t.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditTopicModal(t)}
							title="Edit"
							aria-label="Edit topic {t.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteTopic(t)}
							title="Delete"
							aria-label="Delete topic {t.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const topicColumns = defineColumns<TopicSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'TopicId', label: 'ID' },
					{ key: 'UserExperienceVersion', label: 'UX Version' },
					{ key: 'actions', label: '', render: topicActionsCell }
				])}
				<DataTable
					rows={filteredTopics}
					rowKey={(t) => t.TopicId ?? ''}
					columns={topicColumns}
					loading={tabLoader.isLoading('topics')}
					emptyMessage="No topics found"
				/>
				<LoadMore hasMore={!!topicsNextToken} loading={loadingMoreTopics} onLoadMore={loadMoreTopics} />
			{:else if activeTab === 'namespaces'}
				{#snippet namespaceActionsCell(n: NamespaceInfoV2)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openNamespaceDetail(n)}
							title="View"
							aria-label="View namespace {n.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteNamespace(n)}
							title="Delete"
							aria-label="Delete namespace {n.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const namespaceColumns = defineColumns<NamespaceInfoV2>([
					{ key: 'Name', label: 'Name' },
					{ key: 'CapacityRegion', label: 'Region' },
					{ key: 'CreationStatus', label: 'Status' },
					{ key: 'IdentityStore', label: 'Identity Store' },
					{ key: 'actions', label: '', render: namespaceActionsCell }
				])}
				<DataTable
					rows={filteredNamespaces}
					rowKey={(n) => n.Name ?? ''}
					columns={namespaceColumns}
					loading={tabLoader.isLoading('namespaces')}
					emptyMessage="No namespaces found"
				/>
				<LoadMore
					hasMore={!!namespacesNextToken}
					loading={loadingMoreNamespaces}
					onLoadMore={loadMoreNamespaces}
				/>
			{:else if activeTab === 'groups'}
				{#snippet groupActionsCell(g: Group)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openGroupDetail(g)}
							title="View"
							aria-label="View group {g.GroupName}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditGroupModal(g)}
							title="Edit"
							aria-label="Edit group {g.GroupName}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteGroup(g)}
							title="Delete"
							aria-label="Delete group {g.GroupName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const groupColumns = defineColumns<Group>([
					{ key: 'GroupName', label: 'Name' },
					{ key: 'Description', label: 'Description' },
					{ key: 'actions', label: '', render: groupActionsCell }
				])}
				<DataTable
					rows={filteredGroups}
					rowKey={(g) => g.GroupName ?? ''}
					columns={groupColumns}
					loading={tabLoader.isLoading('groups')}
					emptyMessage="No groups found"
				/>
				<LoadMore hasMore={!!groupsNextToken} loading={loadingMoreGroups} onLoadMore={loadMoreGroups} />
			{:else if activeTab === 'users'}
				{#snippet userActiveCell(u: User)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(!!u.Active)}">{u.Active ? 'Active' : 'Inactive'}</span>
				{/snippet}
				{#snippet userActionsCell(u: User)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openUserDetail(u)}
							title="View"
							aria-label="View user {u.UserName}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditUserModal(u)}
							title="Edit"
							aria-label="Edit user {u.UserName}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteUser(u)}
							title="Delete"
							aria-label="Delete user {u.UserName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const userColumns = defineColumns<User>([
					{ key: 'UserName', label: 'Username' },
					{ key: 'Email', label: 'Email' },
					{ key: 'Role', label: 'Role' },
					{ key: 'Active', label: 'Active', render: userActiveCell },
					{ key: 'actions', label: '', render: userActionsCell }
				])}
				<DataTable
					rows={filteredUsers}
					rowKey={(u) => u.UserName ?? ''}
					columns={userColumns}
					loading={tabLoader.isLoading('users')}
					emptyMessage="No users found"
				/>
				<LoadMore hasMore={!!usersNextToken} loading={loadingMoreUsers} onLoadMore={loadMoreUsers} />
			{:else if activeTab === 'iamPolicyAssignments'}
				{#snippet assignmentStatusCell(a: IAMPolicyAssignmentSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(a.AssignmentStatus === 'ENABLED')}"
						>{a.AssignmentStatus ?? '—'}</span
					>
				{/snippet}
				{#snippet assignmentActionsCell(a: IAMPolicyAssignmentSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openIamPolicyAssignmentDetail(a)}
							title="View"
							aria-label="View assignment {a.AssignmentName}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditIamPolicyAssignmentModal(a)}
							title="Edit"
							aria-label="Edit assignment {a.AssignmentName}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteIamPolicyAssignment(a)}
							title="Delete"
							aria-label="Delete assignment {a.AssignmentName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const assignmentColumns = defineColumns<IAMPolicyAssignmentSummary>([
					{ key: 'AssignmentName', label: 'Name' },
					{ key: 'AssignmentStatus', label: 'Status', render: assignmentStatusCell },
					{ key: 'actions', label: '', render: assignmentActionsCell }
				])}
				<DataTable
					rows={filteredIamPolicyAssignments}
					rowKey={(a) => a.AssignmentName ?? ''}
					columns={assignmentColumns}
					loading={tabLoader.isLoading('iamPolicyAssignments')}
					emptyMessage="No IAM policy assignments found"
				/>
				<LoadMore
					hasMore={!!iamPolicyAssignmentsNextToken}
					loading={loadingMoreIamPolicyAssignments}
					onLoadMore={loadMoreIamPolicyAssignments}
				/>
			{:else if activeTab === 'customPermissions'}
				{#snippet customPermActionsCell(c: CustomPermissionsDetail)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openCustomPermissionsDetail(c)}
							title="View"
							aria-label="View custom permissions {c.CustomPermissionsName}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteCustomPermissions(c)}
							title="Delete"
							aria-label="Delete custom permissions {c.CustomPermissionsName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const customPermColumns = defineColumns<CustomPermissionsDetail>([
					{ key: 'CustomPermissionsName', label: 'Name' },
					{ key: 'actions', label: '', render: customPermActionsCell }
				])}
				<DataTable
					rows={filteredCustomPermissions}
					rowKey={(c) => c.CustomPermissionsName ?? ''}
					columns={customPermColumns}
					loading={tabLoader.isLoading('customPermissions')}
					emptyMessage="No custom permissions found"
				/>
				<LoadMore
					hasMore={!!customPermissionsNextToken}
					loading={loadingMoreCustomPermissions}
					onLoadMore={loadMoreCustomPermissions}
				/>
			{:else if activeTab === 'brands'}
				{#snippet brandStatusCell(b: BrandSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(b.BrandStatus === 'CREATE_SUCCEEDED')}"
						>{b.BrandStatus ?? '—'}</span
					>
				{/snippet}
				{#snippet brandUpdatedCell(b: BrandSummary)}
					{formatDate(b.LastUpdatedTime)}
				{/snippet}
				{#snippet brandActionsCell(b: BrandSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openBrandDetail(b)}
							title="View"
							aria-label="View brand {b.BrandName}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditBrandModal(b)}
							title="Edit"
							aria-label="Edit brand {b.BrandName}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteBrand(b)}
							title="Delete"
							aria-label="Delete brand {b.BrandName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const brandColumns = defineColumns<BrandSummary>([
					{ key: 'BrandName', label: 'Name' },
					{ key: 'BrandId', label: 'ID' },
					{ key: 'BrandStatus', label: 'Status', render: brandStatusCell },
					{ key: 'LastUpdatedTime', label: 'Updated', render: brandUpdatedCell },
					{ key: 'actions', label: '', render: brandActionsCell }
				])}
				<DataTable
					rows={filteredBrands}
					rowKey={(b) => b.BrandId ?? ''}
					columns={brandColumns}
					loading={tabLoader.isLoading('brands')}
					emptyMessage="No brands found"
				/>
				<LoadMore hasMore={!!brandsNextToken} loading={loadingMoreBrands} onLoadMore={loadMoreBrands} />
			{:else if activeTab === 'actionConnectors'}
				{#snippet acStatusCell(a: ActionConnectorSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(a.Status === 'CREATION_SUCCESSFUL')}"
						>{a.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet acUpdatedCell(a: ActionConnectorSummary)}
					{formatDate(a.LastUpdatedTime)}
				{/snippet}
				{#snippet acActionsCell(a: ActionConnectorSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openActionConnectorDetail(a)}
							title="View"
							aria-label="View action connector {a.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditActionConnectorModal(a)}
							title="Edit"
							aria-label="Edit action connector {a.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteActionConnector(a)}
							title="Delete"
							aria-label="Delete action connector {a.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const acColumns = defineColumns<ActionConnectorSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'ActionConnectorId', label: 'ID' },
					{ key: 'Type', label: 'Type' },
					{ key: 'Status', label: 'Status', render: acStatusCell },
					{ key: 'LastUpdatedTime', label: 'Updated', render: acUpdatedCell },
					{ key: 'actions', label: '', render: acActionsCell }
				])}
				<DataTable
					rows={filteredActionConnectors}
					rowKey={(a) => a.ActionConnectorId ?? ''}
					columns={acColumns}
					loading={tabLoader.isLoading('actionConnectors')}
					emptyMessage="No action connectors found"
				/>
				<LoadMore
					hasMore={!!actionConnectorsNextToken}
					loading={loadingMoreActionConnectors}
					onLoadMore={loadMoreActionConnectors}
				/>
			{:else if activeTab === 'agents'}
				{#snippet agentUpdatedCell(a: AgentSummary)}
					{formatDate(a.UpdatedAt)}
				{/snippet}
				{#snippet agentActionsCell(a: AgentSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openAgentDetail(a)}
							title="View"
							aria-label="View agent {a.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditAgentModal(a)}
							title="Edit"
							aria-label="Edit agent {a.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteAgent(a)}
							title="Delete"
							aria-label="Delete agent {a.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const agentColumns = defineColumns<AgentSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'AgentId', label: 'ID' },
					{ key: 'Description', label: 'Description' },
					{ key: 'UpdatedAt', label: 'Updated', render: agentUpdatedCell },
					{ key: 'actions', label: '', render: agentActionsCell }
				])}
				<DataTable
					rows={filteredAgents}
					rowKey={(a) => a.AgentId ?? ''}
					columns={agentColumns}
					loading={tabLoader.isLoading('agents')}
					emptyMessage="No agents found"
				/>
				<LoadMore hasMore={!!agentsNextToken} loading={loadingMoreAgents} onLoadMore={loadMoreAgents} />
			{:else if activeTab === 'knowledgeBases'}
				{#snippet kbStatusCell(k: KnowledgeBaseSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(k.Status === 'ACTIVE')}"
						>{k.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet kbUpdatedCell(k: KnowledgeBaseSummary)}
					{formatDate(k.UpdatedAt)}
				{/snippet}
				{#snippet kbActionsCell(k: KnowledgeBaseSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openKnowledgeBaseDetail(k)}
							title="View"
							aria-label="View knowledge base {k.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditKnowledgeBaseModal(k)}
							title="Edit"
							aria-label="Edit knowledge base {k.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteKnowledgeBase(k)}
							title="Delete"
							aria-label="Delete knowledge base {k.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const kbColumns = defineColumns<KnowledgeBaseSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'KnowledgeBaseId', label: 'ID' },
					{ key: 'Status', label: 'Status', render: kbStatusCell },
					{ key: 'UpdatedAt', label: 'Updated', render: kbUpdatedCell },
					{ key: 'actions', label: '', render: kbActionsCell }
				])}
				<DataTable
					rows={filteredKnowledgeBases}
					rowKey={(k) => k.KnowledgeBaseId ?? ''}
					columns={kbColumns}
					loading={tabLoader.isLoading('knowledgeBases')}
					emptyMessage="No knowledge bases found"
				/>
				<LoadMore
					hasMore={!!knowledgeBasesNextToken}
					loading={loadingMoreKnowledgeBases}
					onLoadMore={loadMoreKnowledgeBases}
				/>
			{:else if activeTab === 'spaces'}
				{#snippet spaceUpdatedCell(s: SpaceSummary)}
					{formatDate(s.updatedAt)}
				{/snippet}
				{#snippet spaceActionsCell(s: SpaceSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openSpaceDetail(s)}
							title="View"
							aria-label="View space {s.name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditSpaceModal(s)}
							title="Edit"
							aria-label="Edit space {s.name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteSpace(s)}
							title="Delete"
							aria-label="Delete space {s.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const spaceColumns = defineColumns<SpaceSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'spaceId', label: 'ID' },
					{ key: 'resourcesCount', label: 'Resources' },
					{ key: 'updatedAt', label: 'Updated', render: spaceUpdatedCell },
					{ key: 'actions', label: '', render: spaceActionsCell }
				])}
				<DataTable
					rows={filteredSpaces}
					rowKey={(s) => s.spaceId ?? ''}
					columns={spaceColumns}
					loading={tabLoader.isLoading('spaces')}
					emptyMessage="No spaces found"
				/>
				<LoadMore hasMore={!!spacesNextToken} loading={loadingMoreSpaces} onLoadMore={loadMoreSpaces} />
			{/if}
		</div>
	</div>
</div>

<!-- ==================== Dashboards modals ==================== -->

<Modal bind:this={createDashboardModal} title="Create Dashboard">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dashboard-id" class="text-sm text-slate-600 dark:text-slate-300">Dashboard ID</label>
				<input
					id="dashboard-id"
					bind:value={newDashboardId}
					placeholder="my-dashboard"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dashboard-name" class="text-sm text-slate-600 dark:text-slate-300">Dashboard name</label>
				<input
					id="dashboard-name"
					bind:value={newDashboardName}
					placeholder="Sales Overview"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dashboard-definition" class="text-sm text-slate-600 dark:text-slate-300"
					>Definition (JSON, optional)</label
				>
				<textarea
					id="dashboard-definition"
					bind:value={newDashboardDefinition}
					rows="4"
					placeholder="{'{'}}"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createDashboardError}
				<p class="text-sm text-red-600 dark:text-red-400">{createDashboardError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createDashboardModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateDashboard}
			disabled={creatingDashboard}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingDashboard ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editDashboardModal} title="Edit Dashboard">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingDashboard?.DashboardId}</span>
			</p>
			<div>
				<label for="edit-dashboard-name" class="text-sm text-slate-600 dark:text-slate-300">New dashboard name</label>
				<input
					id="edit-dashboard-name"
					bind:value={editingDashboardName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-dashboard-definition" class="text-sm text-slate-600 dark:text-slate-300"
					>Definition (JSON, optional -- leave blank to keep the existing definition)</label
				>
				<textarea
					id="edit-dashboard-definition"
					bind:value={editingDashboardDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editDashboardError}
				<p class="text-sm text-red-600 dark:text-red-400">{editDashboardError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editDashboardModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditDashboard}
			disabled={savingDashboard}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingDashboard ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={dashboardDetailModal} title="Dashboard">
	{#snippet children()}
		{#if dashboardDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedDashboard}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDashboard.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Dashboard ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDashboard.DashboardId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedDashboard.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDashboard.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDashboard.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if dashboardDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{dashboardDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => dashboardDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Analyses modals ==================== -->

<Modal bind:this={createAnalysisModal} title="Create Analysis">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="analysis-id" class="text-sm text-slate-600 dark:text-slate-300">Analysis ID</label>
				<input
					id="analysis-id"
					bind:value={newAnalysisId}
					placeholder="my-analysis"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="analysis-name" class="text-sm text-slate-600 dark:text-slate-300">Analysis name</label>
				<input
					id="analysis-name"
					bind:value={newAnalysisName}
					placeholder="Quarterly Trends"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="analysis-definition" class="text-sm text-slate-600 dark:text-slate-300"
					>Definition (JSON, optional)</label
				>
				<textarea
					id="analysis-definition"
					bind:value={newAnalysisDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createAnalysisError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAnalysisError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createAnalysisModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateAnalysis}
			disabled={creatingAnalysis}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingAnalysis ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editAnalysisModal} title="Edit Analysis">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingAnalysis?.AnalysisId}</span>
			</p>
			<div>
				<label for="edit-analysis-name" class="text-sm text-slate-600 dark:text-slate-300">New analysis name</label>
				<input
					id="edit-analysis-name"
					bind:value={editingAnalysisName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-analysis-definition" class="text-sm text-slate-600 dark:text-slate-300"
					>Definition (JSON, optional -- leave blank to keep the existing definition)</label
				>
				<textarea
					id="edit-analysis-definition"
					bind:value={editingAnalysisDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editAnalysisError}
				<p class="text-sm text-red-600 dark:text-red-400">{editAnalysisError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editAnalysisModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditAnalysis}
			disabled={savingAnalysis}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingAnalysis ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={analysisDetailModal} title="Analysis">
	{#snippet children()}
		{#if analysisDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedAnalysis}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalysis.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Analysis ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalysis.AnalysisId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedAnalysis.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalysis.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedAnalysis.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedAnalysis.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if analysisDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{analysisDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => analysisDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Data Sets modals ==================== -->

<Modal bind:this={createDataSetModal} title="Create Data Set">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dataset-id" class="text-sm text-slate-600 dark:text-slate-300">Data set ID</label>
				<input
					id="dataset-id"
					bind:value={newDataSetId}
					placeholder="my-dataset"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dataset-name" class="text-sm text-slate-600 dark:text-slate-300">Data set name</label>
				<input
					id="dataset-name"
					bind:value={newDataSetName}
					placeholder="Orders"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dataset-import-mode" class="text-sm text-slate-600 dark:text-slate-300"
					>Import mode</label
				>
				<select
					id="dataset-import-mode"
					bind:value={newDataSetImportMode}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SPICE">SPICE</option>
					<option value="DIRECT_QUERY">Direct Query</option>
				</select>
			</div>
			{#if createDataSetError}
				<p class="text-sm text-red-600 dark:text-red-400">{createDataSetError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createDataSetModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateDataSet}
			disabled={creatingDataSet}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingDataSet ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editDataSetModal} title="Edit Data Set">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingDataSet?.DataSetId}</span>
			</p>
			<div>
				<label for="edit-dataset-name" class="text-sm text-slate-600 dark:text-slate-300">New data set name</label>
				<input
					id="edit-dataset-name"
					bind:value={editingDataSetName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-dataset-import-mode" class="text-sm text-slate-600 dark:text-slate-300"
					>Import mode</label
				>
				<select
					id="edit-dataset-import-mode"
					bind:value={editingDataSetImportMode}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SPICE">SPICE</option>
					<option value="DIRECT_QUERY">Direct Query</option>
				</select>
			</div>
			{#if editDataSetError}
				<p class="text-sm text-red-600 dark:text-red-400">{editDataSetError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editDataSetModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditDataSet}
			disabled={savingDataSet}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingDataSet ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={dataSetDetailModal} title="Data Set">
	{#snippet children()}
		{#if dataSetDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedDataSet}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSet.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Data set ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSet.DataSetId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedDataSet.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Import mode</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSet.ImportMode ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDataSet.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDataSet.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if dataSetDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{dataSetDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => dataSetDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Data Sources modals ==================== -->

<Modal bind:this={createDataSourceModal} title="Create Data Source">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="datasource-id" class="text-sm text-slate-600 dark:text-slate-300">Data source ID</label>
				<input
					id="datasource-id"
					bind:value={newDataSourceId}
					placeholder="my-datasource"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="datasource-name" class="text-sm text-slate-600 dark:text-slate-300">Data source name</label>
				<input
					id="datasource-name"
					bind:value={newDataSourceName}
					placeholder="Production MySQL"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="datasource-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select
					id="datasource-type"
					bind:value={newDataSourceType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					{#each dataSourceTypes as t (t)}
						<option value={t}>{t}</option>
					{/each}
				</select>
			</div>
			{#if createDataSourceError}
				<p class="text-sm text-red-600 dark:text-red-400">{createDataSourceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createDataSourceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateDataSource}
			disabled={creatingDataSource}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingDataSource ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editDataSourceModal} title="Edit Data Source">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingDataSource?.DataSourceId}</span> ({editingDataSource?.Type})
			</p>
			<div>
				<label for="edit-datasource-name" class="text-sm text-slate-600 dark:text-slate-300">New data source name</label>
				<input
					id="edit-datasource-name"
					bind:value={editingDataSourceName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editDataSourceError}
				<p class="text-sm text-red-600 dark:text-red-400">{editDataSourceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editDataSourceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditDataSource}
			disabled={savingDataSource}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingDataSource ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={dataSourceDetailModal} title="Data Source">
	{#snippet children()}
		{#if dataSourceDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedDataSource}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSource.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Data source ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSource.DataSourceId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedDataSource.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSource.Type ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSource.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDataSource.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDataSource.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if dataSourceDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{dataSourceDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => dataSourceDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Folders modals ==================== -->

<Modal bind:this={createFolderModal} title="Create Folder">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="folder-id" class="text-sm text-slate-600 dark:text-slate-300">Folder ID</label>
				<input
					id="folder-id"
					bind:value={newFolderId}
					placeholder="my-folder"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="folder-name" class="text-sm text-slate-600 dark:text-slate-300">Folder name</label>
				<input
					id="folder-name"
					bind:value={newFolderName}
					placeholder="Marketing"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="folder-type" class="text-sm text-slate-600 dark:text-slate-300">Folder type</label>
				<select
					id="folder-type"
					bind:value={newFolderType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SHARED">Shared</option>
					<option value="RESTRICTED">Restricted</option>
				</select>
			</div>
			<div>
				<label for="folder-parent-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>Parent folder ARN (optional -- omit for a root-level folder)</label
				>
				<input
					id="folder-parent-arn"
					bind:value={newFolderParentArn}
					placeholder="arn:aws:quicksight:...:folder/parent-id"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="folder-sharing-model" class="text-sm text-slate-600 dark:text-slate-300"
					>Sharing model (optional -- defaults to Account)</label
				>
				<select
					id="folder-sharing-model"
					bind:value={newFolderSharingModel}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">(default)</option>
					<option value="ACCOUNT">Account</option>
					<option value="NAMESPACE">Namespace</option>
				</select>
			</div>
			{#if createFolderError}
				<p class="text-sm text-red-600 dark:text-red-400">{createFolderError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createFolderModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateFolder}
			disabled={creatingFolder}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingFolder ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editFolderModal} title="Edit Folder">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingFolder?.FolderId}</span>
			</p>
			<div>
				<label for="edit-folder-name" class="text-sm text-slate-600 dark:text-slate-300">New folder name</label>
				<input
					id="edit-folder-name"
					bind:value={editingFolderName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editFolderError}
				<p class="text-sm text-red-600 dark:text-red-400">{editFolderError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editFolderModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditFolder}
			disabled={savingFolder}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingFolder ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={folderDetailModal} title="Folder">
	{#snippet children()}
		{#if folderDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedFolder}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFolder.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Folder ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFolder.FolderId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedFolder.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFolder.FolderType ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Sharing model</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFolder.SharingModel ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedFolder.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedFolder.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if folderDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{folderDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => folderDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== VPC Connections modals ==================== -->

<Modal bind:this={createVpcConnectionModal} title="Create VPC Connection">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="vpc-connection-id" class="text-sm text-slate-600 dark:text-slate-300"
					>VPC connection ID</label
				>
				<input
					id="vpc-connection-id"
					bind:value={newVpcConnectionId}
					placeholder="my-vpc-connection"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="vpc-connection-name" class="text-sm text-slate-600 dark:text-slate-300">VPC connection name</label>
				<input
					id="vpc-connection-name"
					bind:value={newVpcConnectionName}
					placeholder="Private VPC"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="vpc-connection-subnets" class="text-sm text-slate-600 dark:text-slate-300"
					>Subnet IDs (comma-separated)</label
				>
				<input
					id="vpc-connection-subnets"
					bind:value={newVpcConnectionSubnetIds}
					placeholder="subnet-0123, subnet-0456"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="vpc-connection-sgs" class="text-sm text-slate-600 dark:text-slate-300"
					>Security group IDs (comma-separated)</label
				>
				<input
					id="vpc-connection-sgs"
					bind:value={newVpcConnectionSecurityGroupIds}
					placeholder="sg-0123"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="vpc-connection-dns" class="text-sm text-slate-600 dark:text-slate-300"
					>DNS resolver IPs (comma-separated, optional)</label
				>
				<input
					id="vpc-connection-dns"
					bind:value={newVpcConnectionDnsResolvers}
					placeholder="10.0.0.2"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="vpc-connection-role-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>IAM role ARN</label
				>
				<input
					id="vpc-connection-role-arn"
					bind:value={newVpcConnectionRoleArn}
					placeholder="arn:aws:iam::123456789012:role/quicksight-vpc"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createVpcConnectionError}
				<p class="text-sm text-red-600 dark:text-red-400">{createVpcConnectionError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createVpcConnectionModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateVpcConnection}
			disabled={creatingVpcConnection}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingVpcConnection ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editVpcConnectionModal} title="Edit VPC Connection">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingVpcConnection?.VPCConnectionId}</span>
			</p>
			<div>
				<label for="edit-vpc-connection-name" class="text-sm text-slate-600 dark:text-slate-300">New VPC connection name</label>
				<input
					id="edit-vpc-connection-name"
					bind:value={editingVpcConnectionName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vpc-connection-subnets" class="text-sm text-slate-600 dark:text-slate-300"
					>Subnet IDs (comma-separated -- not returned by Describe/List, re-enter the desired set)</label
				>
				<input
					id="edit-vpc-connection-subnets"
					bind:value={editingVpcConnectionSubnetIds}
					placeholder="subnet-0123, subnet-0456"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vpc-connection-sgs" class="text-sm text-slate-600 dark:text-slate-300"
					>Security group IDs (comma-separated)</label
				>
				<input
					id="edit-vpc-connection-sgs"
					bind:value={editingVpcConnectionSecurityGroupIds}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vpc-connection-dns" class="text-sm text-slate-600 dark:text-slate-300"
					>DNS resolver IPs (comma-separated, optional)</label
				>
				<input
					id="edit-vpc-connection-dns"
					bind:value={editingVpcConnectionDnsResolvers}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vpc-connection-role-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>IAM role ARN</label
				>
				<input
					id="edit-vpc-connection-role-arn"
					bind:value={editingVpcConnectionRoleArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editVpcConnectionError}
				<p class="text-sm text-red-600 dark:text-red-400">{editVpcConnectionError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editVpcConnectionModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditVpcConnection}
			disabled={savingVpcConnection}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingVpcConnection ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={vpcConnectionDetailModal} title="VPC Connection">
	{#snippet children()}
		{#if vpcConnectionDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedVpcConnection}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedVpcConnection.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">VPC connection ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedVpcConnection.VPCConnectionId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedVpcConnection.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">VPC ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedVpcConnection.VPCId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Security group IDs</dt>
					<dd class="text-slate-900 dark:text-white"
						>{(viewedVpcConnection.SecurityGroupIds ?? []).join(', ') || '—'}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">DNS resolvers</dt>
					<dd class="text-slate-900 dark:text-white"
						>{(viewedVpcConnection.DnsResolvers ?? []).join(', ') || '—'}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Role ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedVpcConnection.RoleArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedVpcConnection.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Availability</dt>
					<dd class="text-slate-900 dark:text-white">{viewedVpcConnection.AvailabilityStatus ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedVpcConnection.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedVpcConnection.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if vpcConnectionDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{vpcConnectionDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => vpcConnectionDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Templates modals ==================== -->

<Modal bind:this={createTemplateModal} title="Create Template">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="template-id" class="text-sm text-slate-600 dark:text-slate-300">Template ID</label>
				<input
					id="template-id"
					bind:value={newTemplateId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="template-name" class="text-sm text-slate-600 dark:text-slate-300">Template name</label>
				<input
					id="template-name"
					bind:value={newTemplateName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="template-definition" class="text-sm text-slate-600 dark:text-slate-300">Definition (JSON)</label>
				<textarea
					id="template-definition"
					bind:value={newTemplateDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
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
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingTemplate ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editTemplateModal} title="Edit Template">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingTemplate?.TemplateId}</span>
			</p>
			<div>
				<label for="edit-template-name" class="text-sm text-slate-600 dark:text-slate-300">New template name</label>
				<input
					id="edit-template-name"
					bind:value={editingTemplateName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-template-definition" class="text-sm text-slate-600 dark:text-slate-300"
					>Definition (JSON, optional -- leave blank to keep the existing definition)</label
				>
				<textarea
					id="edit-template-definition"
					bind:value={editingTemplateDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editTemplateError}
				<p class="text-sm text-red-600 dark:text-red-400">{editTemplateError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editTemplateModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditTemplate}
			disabled={savingTemplate}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingTemplate ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={templateDetailModal} title="Template">
	{#snippet children()}
		{#if templateDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedTemplate}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedTemplate.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Template ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedTemplate.TemplateId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedTemplate.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedTemplate.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if templateDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{templateDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => templateDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Themes modals ==================== -->

<Modal bind:this={createThemeModal} title="Create Theme">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="theme-id" class="text-sm text-slate-600 dark:text-slate-300">Theme ID</label>
				<input
					id="theme-id"
					bind:value={newThemeId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="theme-name" class="text-sm text-slate-600 dark:text-slate-300">Theme name</label>
				<input
					id="theme-name"
					bind:value={newThemeName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="theme-base-id" class="text-sm text-slate-600 dark:text-slate-300">Base theme ID</label>
				<input
					id="theme-base-id"
					bind:value={newThemeBaseThemeId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="theme-configuration" class="text-sm text-slate-600 dark:text-slate-300">Configuration (JSON)</label>
				<textarea
					id="theme-configuration"
					bind:value={newThemeConfiguration}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createThemeError}
				<p class="text-sm text-red-600 dark:text-red-400">{createThemeError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createThemeModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateTheme}
			disabled={creatingTheme}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingTheme ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editThemeModal} title="Edit Theme">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingTheme?.ThemeId}</span>
			</p>
			<div>
				<label for="edit-theme-name" class="text-sm text-slate-600 dark:text-slate-300">New theme name</label>
				<input
					id="edit-theme-name"
					bind:value={editingThemeName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-theme-base-id" class="text-sm text-slate-600 dark:text-slate-300">Base theme ID</label>
				<input
					id="edit-theme-base-id"
					bind:value={editingThemeBaseThemeId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-theme-configuration" class="text-sm text-slate-600 dark:text-slate-300"
					>Configuration (JSON, optional -- leave blank to keep the existing configuration)</label
				>
				<textarea
					id="edit-theme-configuration"
					bind:value={editingThemeConfiguration}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editThemeError}
				<p class="text-sm text-red-600 dark:text-red-400">{editThemeError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editThemeModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditTheme}
			disabled={savingTheme}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingTheme ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={themeDetailModal} title="Theme">
	{#snippet children()}
		{#if themeDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedTheme}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedTheme.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Theme ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedTheme.ThemeId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedTheme.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedTheme.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if themeDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{themeDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => themeDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Topics modals ==================== -->

<Modal bind:this={createTopicModal} title="Create Topic">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="topic-id" class="text-sm text-slate-600 dark:text-slate-300">Topic ID</label>
				<input
					id="topic-id"
					bind:value={newTopicId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="topic-definition" class="text-sm text-slate-600 dark:text-slate-300">Topic definition (JSON)</label>
				<textarea
					id="topic-definition"
					bind:value={newTopicDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createTopicError}
				<p class="text-sm text-red-600 dark:text-red-400">{createTopicError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createTopicModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateTopic}
			disabled={creatingTopic}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingTopic ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editTopicModal} title="Edit Topic">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingTopic?.TopicId}</span>
			</p>
			<div>
				<label for="edit-topic-definition" class="text-sm text-slate-600 dark:text-slate-300">Topic definition (JSON)</label>
				<textarea
					id="edit-topic-definition"
					bind:value={editingTopicDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editTopicError}
				<p class="text-sm text-red-600 dark:text-red-400">{editTopicError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editTopicModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditTopic}
			disabled={savingTopic}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingTopic ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={topicDetailModal} title="Topic">
	{#snippet children()}
		{#if topicDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedTopic}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedTopic.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Topic ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedTopic.TopicId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedTopic.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Description</dt>
					<dd class="text-slate-900 dark:text-white">{viewedTopic.Description ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Data sets</dt>
					<dd class="text-slate-900 dark:text-white">{(viewedTopic.DataSets ?? []).length}</dd>
				</div>
			</dl>
			{#if topicDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{topicDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => topicDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Namespaces modals ==================== -->

<Modal bind:this={createNamespaceModal} title="Create Namespace">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="namespace-name" class="text-sm text-slate-600 dark:text-slate-300">Namespace name</label>
				<input
					id="namespace-name"
					bind:value={newNamespaceName}
					placeholder="default"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="namespace-identity-store" class="text-sm text-slate-600 dark:text-slate-300">Identity store</label>
				<select
					id="namespace-identity-store"
					bind:value={newNamespaceIdentityStore}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="QUICKSIGHT">QUICKSIGHT</option>
				</select>
			</div>
			{#if createNamespaceError}
				<p class="text-sm text-red-600 dark:text-red-400">{createNamespaceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createNamespaceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateNamespace}
			disabled={creatingNamespace}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingNamespace ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={namespaceDetailModal} title="Namespace">
	{#snippet children()}
		{#if namespaceDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedNamespace}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedNamespace.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedNamespace.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Capacity region</dt>
					<dd class="text-slate-900 dark:text-white">{viewedNamespace.CapacityRegion ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedNamespace.CreationStatus ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Identity store</dt>
					<dd class="text-slate-900 dark:text-white">{viewedNamespace.IdentityStore ?? '—'}</dd>
				</div>
			</dl>
			{#if namespaceDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{namespaceDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => namespaceDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Groups modals ==================== -->

<Modal bind:this={createGroupModal} title="Create Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="group-name" class="text-sm text-slate-600 dark:text-slate-300">Group name</label>
				<input
					id="group-name"
					bind:value={newGroupName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="group-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input
					id="group-description"
					bind:value={newGroupDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createGroupError}
				<p class="text-sm text-red-600 dark:text-red-400">{createGroupError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createGroupModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateGroup}
			disabled={creatingGroup}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingGroup ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editGroupModal} title="Edit Group">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingGroup?.GroupName}</span>
			</p>
			<div>
				<label for="edit-group-description" class="text-sm text-slate-600 dark:text-slate-300">New description</label>
				<input
					id="edit-group-description"
					bind:value={editingGroupDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editGroupError}
				<p class="text-sm text-red-600 dark:text-red-400">{editGroupError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editGroupModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditGroup}
			disabled={savingGroup}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingGroup ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={groupDetailModal} title="Group">
	{#snippet children()}
		{#if groupDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedGroup}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedGroup.GroupName ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedGroup.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Description</dt>
					<dd class="text-slate-900 dark:text-white">{viewedGroup.Description ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Principal ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedGroup.PrincipalId ?? '—'}</dd>
				</div>
			</dl>
			{#if groupDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{groupDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => groupDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Users modals ==================== -->

<Modal bind:this={createUserModal} title="Register User">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="user-name" class="text-sm text-slate-600 dark:text-slate-300">Username (optional)</label>
				<input
					id="user-name"
					bind:value={newUserName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="user-email" class="text-sm text-slate-600 dark:text-slate-300">Email</label>
				<input
					id="user-email"
					bind:value={newUserEmail}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="user-identity-type" class="text-sm text-slate-600 dark:text-slate-300">Identity type</label>
				<select
					id="user-identity-type"
					bind:value={newUserIdentityType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="QUICKSIGHT">QUICKSIGHT</option>
					<option value="IAM">IAM</option>
					<option value="IAM_IDENTITY_CENTER">IAM_IDENTITY_CENTER</option>
				</select>
			</div>
			{#if newUserIdentityType === 'IAM'}
				<div>
					<label for="user-iam-arn" class="text-sm text-slate-600 dark:text-slate-300">IAM ARN</label>
					<input
						id="user-iam-arn"
						bind:value={newUserIamArn}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			{/if}
			<div>
				<label for="user-role" class="text-sm text-slate-600 dark:text-slate-300">Role</label>
				<select
					id="user-role"
					bind:value={newUserRole}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ADMIN">ADMIN</option>
					<option value="AUTHOR">AUTHOR</option>
					<option value="READER">READER</option>
					<option value="RESTRICTED_AUTHOR">RESTRICTED_AUTHOR</option>
					<option value="RESTRICTED_READER">RESTRICTED_READER</option>
				</select>
			</div>
			{#if createUserError}
				<p class="text-sm text-red-600 dark:text-red-400">{createUserError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createUserModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateUser}
			disabled={creatingUser}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingUser ? 'Registering…' : 'Register'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editUserModal} title="Edit User">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingUser?.UserName}</span>
			</p>
			<div>
				<label for="edit-user-email" class="text-sm text-slate-600 dark:text-slate-300">Email</label>
				<input
					id="edit-user-email"
					bind:value={editingUserEmail}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-user-role" class="text-sm text-slate-600 dark:text-slate-300">Role</label>
				<select
					id="edit-user-role"
					bind:value={editingUserRole}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ADMIN">ADMIN</option>
					<option value="AUTHOR">AUTHOR</option>
					<option value="READER">READER</option>
					<option value="RESTRICTED_AUTHOR">RESTRICTED_AUTHOR</option>
					<option value="RESTRICTED_READER">RESTRICTED_READER</option>
				</select>
			</div>
			{#if editUserError}
				<p class="text-sm text-red-600 dark:text-red-400">{editUserError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editUserModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditUser}
			disabled={savingUser}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingUser ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={userDetailModal} title="User">
	{#snippet children()}
		{#if userDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedUser}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Username</dt>
					<dd class="text-slate-900 dark:text-white">{viewedUser.UserName ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedUser.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Email</dt>
					<dd class="text-slate-900 dark:text-white">{viewedUser.Email ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Role</dt>
					<dd class="text-slate-900 dark:text-white">{viewedUser.Role ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Identity type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedUser.IdentityType ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Active</dt>
					<dd class="text-slate-900 dark:text-white">{viewedUser.Active ? 'Yes' : 'No'}</dd>
				</div>
			</dl>
			{#if userDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{userDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => userDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== IAM Policy Assignments modals ==================== -->

<Modal bind:this={createIamPolicyAssignmentModal} title="Create IAM Policy Assignment">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="assignment-name" class="text-sm text-slate-600 dark:text-slate-300">Assignment name</label>
				<input
					id="assignment-name"
					bind:value={newAssignmentName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="assignment-status" class="text-sm text-slate-600 dark:text-slate-300">Status</label>
				<select
					id="assignment-status"
					bind:value={newAssignmentStatus}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ENABLED">ENABLED</option>
					<option value="DRAFT">DRAFT</option>
					<option value="DISABLED">DISABLED</option>
				</select>
			</div>
			<div>
				<label for="assignment-policy-arn" class="text-sm text-slate-600 dark:text-slate-300">Policy ARN (optional)</label>
				<input
					id="assignment-policy-arn"
					bind:value={newAssignmentPolicyArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createIamPolicyAssignmentError}
				<p class="text-sm text-red-600 dark:text-red-400">{createIamPolicyAssignmentError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createIamPolicyAssignmentModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateIamPolicyAssignment}
			disabled={creatingIamPolicyAssignment}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingIamPolicyAssignment ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editIamPolicyAssignmentModal} title="Edit IAM Policy Assignment">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingIamPolicyAssignment?.AssignmentName}</span>
			</p>
			<div>
				<label for="edit-assignment-status" class="text-sm text-slate-600 dark:text-slate-300">Status</label>
				<select
					id="edit-assignment-status"
					bind:value={editingAssignmentStatus}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ENABLED">ENABLED</option>
					<option value="DRAFT">DRAFT</option>
					<option value="DISABLED">DISABLED</option>
				</select>
			</div>
			<div>
				<label for="edit-assignment-policy-arn" class="text-sm text-slate-600 dark:text-slate-300">Policy ARN (optional)</label>
				<input
					id="edit-assignment-policy-arn"
					bind:value={editingAssignmentPolicyArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editIamPolicyAssignmentError}
				<p class="text-sm text-red-600 dark:text-red-400">{editIamPolicyAssignmentError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editIamPolicyAssignmentModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditIamPolicyAssignment}
			disabled={savingIamPolicyAssignment}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingIamPolicyAssignment ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={iamPolicyAssignmentDetailModal} title="IAM Policy Assignment">
	{#snippet children()}
		{#if iamPolicyAssignmentDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedIamPolicyAssignment}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedIamPolicyAssignment.AssignmentName ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedIamPolicyAssignment.AssignmentStatus ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Policy ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white"
						>{'PolicyArn' in viewedIamPolicyAssignment ? (viewedIamPolicyAssignment.PolicyArn ?? '—') : '—'}</dd
					>
				</div>
			</dl>
			{#if iamPolicyAssignmentDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{iamPolicyAssignmentDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => iamPolicyAssignmentDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Custom Permissions modals ==================== -->

<Modal bind:this={createCustomPermissionsModal} title="Create Custom Permissions">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="custom-perm-name" class="text-sm text-slate-600 dark:text-slate-300">Custom permissions name</label>
				<input
					id="custom-perm-name"
					bind:value={newCustomPermissionsName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createCustomPermissionsError}
				<p class="text-sm text-red-600 dark:text-red-400">{createCustomPermissionsError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createCustomPermissionsModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateCustomPermissions}
			disabled={creatingCustomPermissions}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingCustomPermissions ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={customPermissionsDetailModal} title="Custom Permissions">
	{#snippet children()}
		{#if customPermissionsDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedCustomPermissions}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedCustomPermissions.CustomPermissionsName ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedCustomPermissions.Arn ?? '—'}</dd>
				</div>
			</dl>
			{#if customPermissionsDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{customPermissionsDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => customPermissionsDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Brands modals ==================== -->

<Modal bind:this={createBrandModal} title="Create Brand">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="brand-id" class="text-sm text-slate-600 dark:text-slate-300">Brand ID</label>
				<input
					id="brand-id"
					bind:value={newBrandId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="brand-definition" class="text-sm text-slate-600 dark:text-slate-300">Brand definition (JSON, optional)</label>
				<textarea
					id="brand-definition"
					bind:value={newBrandDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createBrandError}
				<p class="text-sm text-red-600 dark:text-red-400">{createBrandError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createBrandModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateBrand}
			disabled={creatingBrand}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingBrand ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editBrandModal} title="Edit Brand">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingBrand?.BrandId}</span>
			</p>
			<div>
				<label for="edit-brand-definition" class="text-sm text-slate-600 dark:text-slate-300">Brand definition (JSON)</label>
				<textarea
					id="edit-brand-definition"
					bind:value={editingBrandDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editBrandError}
				<p class="text-sm text-red-600 dark:text-red-400">{editBrandError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editBrandModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditBrand}
			disabled={savingBrand}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingBrand ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={brandDetailModal} title="Brand">
	{#snippet children()}
		{#if brandDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedBrand}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedBrand.BrandName ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Brand ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedBrand.BrandId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedBrand.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedBrand.BrandStatus ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Version status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedBrand.VersionStatus ?? '—'}</dd>
				</div>
			</dl>
			{#if brandDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{brandDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => brandDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Action Connectors modals ==================== -->

<Modal bind:this={createActionConnectorModal} title="Create Action Connector">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ac-id" class="text-sm text-slate-600 dark:text-slate-300">Action connector ID</label>
				<input
					id="ac-id"
					bind:value={newActionConnectorId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="ac-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="ac-name"
					bind:value={newActionConnectorName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="ac-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select
					id="ac-type"
					bind:value={newActionConnectorType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					{#each actionConnectorTypes as t (t)}
						<option value={t}>{t}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="ac-description" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input
					id="ac-description"
					bind:value={newActionConnectorDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="ac-auth-config" class="text-sm text-slate-600 dark:text-slate-300">Authentication config (JSON)</label>
				<textarea
					id="ac-auth-config"
					bind:value={newActionConnectorAuthConfig}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createActionConnectorError}
				<p class="text-sm text-red-600 dark:text-red-400">{createActionConnectorError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createActionConnectorModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateActionConnector}
			disabled={creatingActionConnector}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingActionConnector ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editActionConnectorModal} title="Edit Action Connector">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingActionConnector?.ActionConnectorId}</span>
			</p>
			<div>
				<label for="edit-ac-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="edit-ac-name"
					bind:value={editingActionConnectorName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-ac-description" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input
					id="edit-ac-description"
					bind:value={editingActionConnectorDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-ac-auth-config" class="text-sm text-slate-600 dark:text-slate-300">Authentication config (JSON)</label>
				<textarea
					id="edit-ac-auth-config"
					bind:value={editingActionConnectorAuthConfig}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editActionConnectorError}
				<p class="text-sm text-red-600 dark:text-red-400">{editActionConnectorError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editActionConnectorModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditActionConnector}
			disabled={savingActionConnector}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingActionConnector ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={actionConnectorDetailModal} title="Action Connector">
	{#snippet children()}
		{#if actionConnectorDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedActionConnector}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedActionConnector.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Action connector ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedActionConnector.ActionConnectorId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedActionConnector.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedActionConnector.Type ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedActionConnector.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedActionConnector.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if actionConnectorDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{actionConnectorDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => actionConnectorDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Agents modals ==================== -->

<Modal bind:this={createAgentModal} title="Create Agent">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="agent-id" class="text-sm text-slate-600 dark:text-slate-300">Agent ID</label>
				<input
					id="agent-id"
					bind:value={newAgentId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="agent-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="agent-name"
					bind:value={newAgentName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="agent-description" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input
					id="agent-description"
					bind:value={newAgentDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createAgentError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAgentError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createAgentModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateAgent}
			disabled={creatingAgent}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingAgent ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editAgentModal} title="Edit Agent">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingAgent?.AgentId}</span>
			</p>
			<div>
				<label for="edit-agent-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="edit-agent-name"
					bind:value={editingAgentName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-agent-description" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input
					id="edit-agent-description"
					bind:value={editingAgentDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editAgentError}
				<p class="text-sm text-red-600 dark:text-red-400">{editAgentError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editAgentModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditAgent}
			disabled={savingAgent}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingAgent ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={agentDetailModal} title="Agent">
	{#snippet children()}
		{#if agentDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedAgent}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAgent.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Agent ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAgent.AgentId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedAgent.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Description</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAgent.Description ?? '—'}</dd>
				</div>
			</dl>
			{#if agentDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{agentDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => agentDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Knowledge Bases modals ==================== -->

<Modal bind:this={createKnowledgeBaseModal} title="Create Knowledge Base">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="kb-id" class="text-sm text-slate-600 dark:text-slate-300">Knowledge base ID</label>
				<input
					id="kb-id"
					bind:value={newKnowledgeBaseId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="kb-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="kb-name"
					bind:value={newKnowledgeBaseName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="kb-datasource-arn" class="text-sm text-slate-600 dark:text-slate-300">Data source ARN</label>
				<input
					id="kb-datasource-arn"
					bind:value={newKnowledgeBaseDataSourceArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="kb-configuration" class="text-sm text-slate-600 dark:text-slate-300">Configuration (JSON)</label>
				<textarea
					id="kb-configuration"
					bind:value={newKnowledgeBaseConfiguration}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createKnowledgeBaseError}
				<p class="text-sm text-red-600 dark:text-red-400">{createKnowledgeBaseError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createKnowledgeBaseModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateKnowledgeBase}
			disabled={creatingKnowledgeBase}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingKnowledgeBase ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editKnowledgeBaseModal} title="Edit Knowledge Base">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingKnowledgeBase?.KnowledgeBaseId}</span>
			</p>
			<div>
				<label for="edit-kb-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="edit-kb-name"
					bind:value={editingKnowledgeBaseName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-kb-description" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input
					id="edit-kb-description"
					bind:value={editingKnowledgeBaseDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editKnowledgeBaseError}
				<p class="text-sm text-red-600 dark:text-red-400">{editKnowledgeBaseError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editKnowledgeBaseModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditKnowledgeBase}
			disabled={savingKnowledgeBase}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingKnowledgeBase ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={knowledgeBaseDetailModal} title="Knowledge Base">
	{#snippet children()}
		{#if knowledgeBaseDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedKnowledgeBase}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedKnowledgeBase.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Knowledge base ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedKnowledgeBase.KnowledgeBaseId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedKnowledgeBase.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Data source ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedKnowledgeBase.DataSourceArn ?? '—'}</dd>
				</div>
			</dl>
			{#if knowledgeBaseDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{knowledgeBaseDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => knowledgeBaseDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Spaces modals ==================== -->

<Modal bind:this={createSpaceModal} title="Create Space">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="space-id" class="text-sm text-slate-600 dark:text-slate-300">Space ID</label>
				<input
					id="space-id"
					bind:value={newSpaceId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="space-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="space-name"
					bind:value={newSpaceName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="space-description" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input
					id="space-description"
					bind:value={newSpaceDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createSpaceError}
				<p class="text-sm text-red-600 dark:text-red-400">{createSpaceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createSpaceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateSpace}
			disabled={creatingSpace}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingSpace ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editSpaceModal} title="Edit Space">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingSpace?.spaceId}</span>
			</p>
			<div>
				<label for="edit-space-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="edit-space-name"
					bind:value={editingSpaceName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-space-description" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input
					id="edit-space-description"
					bind:value={editingSpaceDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editSpaceError}
				<p class="text-sm text-red-600 dark:text-red-400">{editSpaceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editSpaceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditSpace}
			disabled={savingSpace}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingSpace ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={spaceDetailModal} title="Space">
	{#snippet children()}
		{#if spaceDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedSpace}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedSpace.name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Space ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedSpace.spaceId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedSpace.spaceArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Description</dt>
					<dd class="text-slate-900 dark:text-white">{viewedSpace.description ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedSpace.updatedAt)}</dd>
				</div>
			</dl>
			{#if spaceDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{spaceDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => spaceDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>
