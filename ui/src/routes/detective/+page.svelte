<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getDetectiveClient } from '$lib/aws-client';
	import { currentRegion } from '$lib/region.svelte';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import {
		ListGraphsCommand,
		CreateGraphCommand,
		DeleteGraphCommand,
		ListMembersCommand,
		CreateMembersCommand,
		DeleteMembersCommand,
		GetMembersCommand,
		ListInvitationsCommand,
		AcceptInvitationCommand,
		RejectInvitationCommand,
		ListInvestigationsCommand,
		StartInvestigationCommand,
		GetInvestigationCommand,
		ListIndicatorsCommand,
		TagResourceCommand,
		UntagResourceCommand,
		ListTagsForResourceCommand,
		StartMonitoringMemberCommand,
		DisassociateMembershipCommand,
		ListDatasourcePackagesCommand,
		UpdateDatasourcePackagesCommand,
		BatchGetGraphMemberDatasourcesCommand,
		BatchGetMembershipDatasourcesCommand,
		UpdateInvestigationStateCommand,
		DescribeOrganizationConfigurationCommand,
		UpdateOrganizationConfigurationCommand,
		EnableOrganizationAdminAccountCommand,
		DisableOrganizationAdminAccountCommand,
		ListOrganizationAdminAccountsCommand,
		DatasourcePackage,
		type Graph,
		type MemberDetail,
		type InvestigationDetail,
		type Indicator,
		type DatasourcePackageIngestDetail,
		type Administrator,
		type State as InvestigationState
	} from '@aws-sdk/client-detective';
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
	import { Search, Plus, Trash2, Eye, Check, X, Play, LogOut } from 'lucide-svelte';

	const client = regionalClient(getDetectiveClient);

	type TabId = 'graphs' | 'members' | 'invitations' | 'investigations' | 'organization';

	const tabs: TabDef[] = [
		{ id: 'graphs', label: 'Behavior Graphs' },
		{ id: 'members', label: 'Members' },
		{ id: 'invitations', label: 'Invitations' },
		{ id: 'investigations', label: 'Investigations' },
		{ id: 'organization', label: 'Organization' }
	];

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

	let activeTab = $state<TabId>('graphs');
	let searchQuery = $state('');

	let graphs = $state<Graph[]>([]);
	let graphsNextToken = $state<string | undefined>();
	let loadingMoreGraphs = $state(false);

	let selectedGraphArn = $state('');

	let members = $state<MemberDetail[]>([]);
	let membersNextToken = $state<string | undefined>();
	let loadingMoreMembers = $state(false);

	let invitations = $state<MemberDetail[]>([]);
	let invitationsNextToken = $state<string | undefined>();
	let loadingMoreInvitations = $state(false);

	let investigations = $state<InvestigationDetail[]>([]);
	let investigationsNextToken = $state<string | undefined>();
	let loadingMoreInvestigations = $state(false);

	let admins = $state<Administrator[]>([]);
	let adminsNextToken = $state<string | undefined>();
	let loadingMoreAdmins = $state(false);

	async function fetchGraphs(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListGraphsCommand({ NextToken: reset ? undefined : graphsNextToken })
		);
		graphs = reset ? (resp.GraphList ?? []) : [...graphs, ...(resp.GraphList ?? [])];
		graphsNextToken = resp.NextToken;
		if (!selectedGraphArn && graphs.length > 0) {
			selectedGraphArn = graphs[0].Arn ?? '';
		}
	}

	async function fetchMembers(reset: boolean): Promise<void> {
		if (!selectedGraphArn) {
			members = [];
			membersNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListMembersCommand({
				GraphArn: selectedGraphArn,
				NextToken: reset ? undefined : membersNextToken
			})
		);
		members = reset ? (resp.MemberDetails ?? []) : [...members, ...(resp.MemberDetails ?? [])];
		membersNextToken = resp.NextToken;
	}

	async function fetchInvitations(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListInvitationsCommand({ NextToken: reset ? undefined : invitationsNextToken })
		);
		invitations = reset ? (resp.Invitations ?? []) : [...invitations, ...(resp.Invitations ?? [])];
		invitationsNextToken = resp.NextToken;
	}

	async function fetchInvestigations(reset: boolean): Promise<void> {
		if (!selectedGraphArn) {
			investigations = [];
			investigationsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListInvestigationsCommand({
				GraphArn: selectedGraphArn,
				NextToken: reset ? undefined : investigationsNextToken
			})
		);
		investigations = reset
			? (resp.InvestigationDetails ?? [])
			: [...investigations, ...(resp.InvestigationDetails ?? [])];
		investigationsNextToken = resp.NextToken;
	}

	async function fetchAdmins(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListOrganizationAdminAccountsCommand({ NextToken: reset ? undefined : adminsNextToken })
		);
		admins = reset ? (resp.Administrators ?? []) : [...admins, ...(resp.Administrators ?? [])];
		adminsNextToken = resp.NextToken;
	}

	// Wrap so a failure's message (captured by tab-loader as err.message)
	// already contains the AWS error code/status rather than a bare string.
	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	const tabLoader = createTabLoader<TabId>({
		graphs: () => fetchGraphs(true).catch(rethrowDescribed),
		members: () => fetchMembers(true).catch(rethrowDescribed),
		invitations: () => fetchInvitations(true).catch(rethrowDescribed),
		investigations: () => fetchInvestigations(true).catch(rethrowDescribed),
		organization: () => fetchAdmins(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	function onGraphSelect(arn: string): void {
		selectedGraphArn = arn;
		if (activeTab === 'members' || activeTab === 'investigations') {
			tabLoader.refresh(activeTab);
		}
	}

	// Behavior graphs are the parent resource for Members/Investigations: on a
	// region change the previously selected GraphArn belongs to the old
	// region and must not be reused, so reload graphs first (which
	// re-selects a graph for the new region) before reloading whichever tab
	// is active.
	onRegionChange(() => {
		selectedGraphArn = '';
		graphs = [];
		graphsNextToken = undefined;
		void tabLoader.refresh('graphs').then(() => {
			if (activeTab !== 'graphs') {
				tabLoader.refresh(activeTab);
			}
		});
	});

	const filteredGraphs = $derived(
		graphs.filter((g) => (g.Arn ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredMembers = $derived(
		members.filter((m) => {
			const q = searchQuery.toLowerCase();
			return (
				(m.AccountId ?? '').toLowerCase().includes(q) ||
				(m.EmailAddress ?? '').toLowerCase().includes(q) ||
				(m.Status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredInvitations = $derived(
		invitations.filter((i) => {
			const q = searchQuery.toLowerCase();
			return (
				(i.GraphArn ?? '').toLowerCase().includes(q) ||
				(i.AdministratorId ?? '').toLowerCase().includes(q) ||
				(i.Status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredInvestigations = $derived(
		investigations.filter((inv) => {
			const q = searchQuery.toLowerCase();
			return (
				(inv.InvestigationId ?? '').toLowerCase().includes(q) ||
				(inv.EntityArn ?? '').toLowerCase().includes(q) ||
				(inv.Status ?? '').toLowerCase().includes(q) ||
				(inv.State ?? '').toLowerCase().includes(q) ||
				(inv.Severity ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredAdmins = $derived(
		admins.filter((a) => (a.AccountId ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreGraphs(): Promise<void> {
		loadingMoreGraphs = true;
		try {
			await fetchGraphs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGraphs = false;
		}
	}

	async function loadMoreMembers(): Promise<void> {
		loadingMoreMembers = true;
		try {
			await fetchMembers(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreMembers = false;
		}
	}

	async function loadMoreInvitations(): Promise<void> {
		loadingMoreInvitations = true;
		try {
			await fetchInvitations(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreInvitations = false;
		}
	}

	async function loadMoreInvestigations(): Promise<void> {
		loadingMoreInvestigations = true;
		try {
			await fetchInvestigations(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreInvestigations = false;
		}
	}

	async function loadMoreAdmins(): Promise<void> {
		loadingMoreAdmins = true;
		try {
			await fetchAdmins(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAdmins = false;
		}
	}

	// --- Behavior graphs: create / delete / detail ---

	let createGraphModal = $state<Modal | null>(null);
	let creatingGraph = $state(false);
	let createGraphError = $state<string | null>(null);

	function openCreateGraphModal(): void {
		createGraphError = null;
		createGraphModal?.open();
	}

	async function submitCreateGraph(): Promise<void> {
		creatingGraph = true;
		createGraphError = null;
		try {
			await client().send(new CreateGraphCommand({}));
			toast.success('Behavior graph created');
			createGraphModal?.close();
			await tabLoader.refresh('graphs');
		} catch (e) {
			const msg = describeError(e);
			createGraphError = msg;
			toast.error(msg);
		} finally {
			creatingGraph = false;
		}
	}

	async function handleDeleteGraph(g: Graph): Promise<void> {
		if (!g.Arn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete behavior graph',
			message: `Delete behavior graph ${g.Arn}? This removes all of its members, investigations, and datasource state.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteGraphCommand({ GraphArn: g.Arn }));
			toast.success('Behavior graph deleted');
			if (selectedGraphArn === g.Arn) {
				selectedGraphArn = '';
			}
			await tabLoader.refresh('graphs');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let graphDetailModal = $state<Modal | null>(null);
	let viewedGraph = $state<Graph | null>(null);
	let graphDetailLoading = $state(false);
	let graphTags = $state<Record<string, string>>({});
	let graphTagsError = $state<string | null>(null);
	let newGraphTagKey = $state('');
	let newGraphTagValue = $state('');
	let graphPackages = $state<Record<string, DatasourcePackageIngestDetail>>({});
	let graphPackagesError = $state<string | null>(null);
	let newGraphPackage = $state<DatasourcePackage>(DatasourcePackage.DETECTIVE_CORE);
	let graphAutoEnable = $state(false);
	let graphOrgConfigError = $state<string | null>(null);
	let updatingAutoEnable = $state(false);

	async function openGraphDetail(g: Graph): Promise<void> {
		viewedGraph = g;
		graphTags = {};
		graphTagsError = null;
		newGraphTagKey = '';
		newGraphTagValue = '';
		graphPackages = {};
		graphPackagesError = null;
		graphAutoEnable = false;
		graphOrgConfigError = null;
		graphDetailModal?.open();
		if (!g.Arn) return;
		graphDetailLoading = true;
		try {
			await Promise.all([loadGraphTags(g.Arn), loadGraphPackages(g.Arn), loadGraphOrgConfig(g.Arn)]);
		} finally {
			graphDetailLoading = false;
		}
	}

	async function loadGraphTags(graphArn: string): Promise<void> {
		try {
			const resp = await client().send(new ListTagsForResourceCommand({ ResourceArn: graphArn }));
			graphTags = resp.Tags ?? {};
		} catch (e) {
			graphTagsError = describeError(e);
		}
	}

	async function addGraphTag(): Promise<void> {
		if (!viewedGraph?.Arn || !newGraphTagKey) return;
		graphTagsError = null;
		try {
			await client().send(
				new TagResourceCommand({
					ResourceArn: viewedGraph.Arn,
					Tags: { [newGraphTagKey]: newGraphTagValue }
				})
			);
			newGraphTagKey = '';
			newGraphTagValue = '';
			toast.success('Tag added');
			await loadGraphTags(viewedGraph.Arn);
		} catch (e) {
			const msg = describeError(e);
			graphTagsError = msg;
			toast.error(msg);
		}
	}

	async function removeGraphTag(key: string): Promise<void> {
		if (!viewedGraph?.Arn) return;
		graphTagsError = null;
		try {
			await client().send(
				new UntagResourceCommand({ ResourceArn: viewedGraph.Arn, TagKeys: [key] })
			);
			toast.success('Tag removed');
			await loadGraphTags(viewedGraph.Arn);
		} catch (e) {
			const msg = describeError(e);
			graphTagsError = msg;
			toast.error(msg);
		}
	}

	async function loadGraphPackages(graphArn: string): Promise<void> {
		try {
			const resp = await client().send(new ListDatasourcePackagesCommand({ GraphArn: graphArn }));
			graphPackages = resp.DatasourcePackages ?? {};
		} catch (e) {
			graphPackagesError = describeError(e);
		}
	}

	async function addGraphPackage(): Promise<void> {
		if (!viewedGraph?.Arn) return;
		graphPackagesError = null;
		try {
			await client().send(
				new UpdateDatasourcePackagesCommand({
					GraphArn: viewedGraph.Arn,
					DatasourcePackages: [newGraphPackage]
				})
			);
			toast.success('Datasource package started');
			await loadGraphPackages(viewedGraph.Arn);
		} catch (e) {
			const msg = describeError(e);
			graphPackagesError = msg;
			toast.error(msg);
		}
	}

	async function loadGraphOrgConfig(graphArn: string): Promise<void> {
		try {
			const resp = await client().send(
				new DescribeOrganizationConfigurationCommand({ GraphArn: graphArn })
			);
			graphAutoEnable = resp.AutoEnable ?? false;
		} catch (e) {
			graphOrgConfigError = describeError(e);
		}
	}

	async function toggleGraphAutoEnable(autoEnable: boolean): Promise<void> {
		if (!viewedGraph?.Arn) return;
		graphOrgConfigError = null;
		updatingAutoEnable = true;
		try {
			await client().send(
				new UpdateOrganizationConfigurationCommand({
					GraphArn: viewedGraph.Arn,
					AutoEnable: autoEnable
				})
			);
			graphAutoEnable = autoEnable;
			toast.success('Organization configuration updated');
		} catch (e) {
			const msg = describeError(e);
			graphOrgConfigError = msg;
			toast.error(msg);
		} finally {
			updatingAutoEnable = false;
		}
	}

	// --- Members: create (invite) / delete (remove) / detail ---

	let createMemberModal = $state<Modal | null>(null);
	let creatingMember = $state(false);
	let createMemberError = $state<string | null>(null);
	let newMemberAccountId = $state('');
	let newMemberEmail = $state('');
	let newMemberMessage = $state('');
	let newMemberDisableEmail = $state(false);

	function openCreateMemberModal(): void {
		createMemberError = selectedGraphArn ? null : 'Select or create a behavior graph first.';
		newMemberAccountId = '';
		newMemberEmail = '';
		newMemberMessage = '';
		newMemberDisableEmail = false;
		createMemberModal?.open();
	}

	async function submitCreateMember(): Promise<void> {
		if (!selectedGraphArn) {
			createMemberError = 'Select or create a behavior graph first.';
			return;
		}
		if (!newMemberAccountId || !newMemberEmail) {
			createMemberError = 'Account ID and email address are required.';
			return;
		}
		creatingMember = true;
		createMemberError = null;
		try {
			const resp = await client().send(
				new CreateMembersCommand({
					GraphArn: selectedGraphArn,
					Accounts: [{ AccountId: newMemberAccountId, EmailAddress: newMemberEmail }],
					Message: newMemberMessage || undefined,
					DisableEmailNotification: newMemberDisableEmail
				})
			);
			const unprocessed = resp.UnprocessedAccounts ?? [];
			if (unprocessed.length > 0) {
				const reason = unprocessed[0].Reason ?? 'unknown reason';
				createMemberError = `Account not processed: ${reason}`;
				toast.error(`Member invite failed: ${reason}`);
				return;
			}
			toast.success('Member invited');
			createMemberModal?.close();
			await tabLoader.refresh('members');
		} catch (e) {
			const msg = describeError(e);
			createMemberError = msg;
			toast.error(msg);
		} finally {
			creatingMember = false;
		}
	}

	async function handleDeleteMember(m: MemberDetail): Promise<void> {
		if (!m.AccountId || !selectedGraphArn) return;
		const confirmed = await confirmDestructive({
			title: 'Remove member',
			message: `Remove account ${m.AccountId} from this behavior graph?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteMembersCommand({ GraphArn: selectedGraphArn, AccountIds: [m.AccountId] })
			);
			toast.success('Member removed');
			await tabLoader.refresh('members');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleStartMonitoring(m: MemberDetail): Promise<void> {
		if (!m.AccountId || !selectedGraphArn) return;
		try {
			await client().send(
				new StartMonitoringMemberCommand({ GraphArn: selectedGraphArn, AccountId: m.AccountId })
			);
			toast.success('Monitoring started');
			await tabLoader.refresh('members');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let memberDetailModal = $state<Modal | null>(null);
	let viewedMember = $state<MemberDetail | null>(null);
	let memberDetailLoading = $state(false);
	let memberDetailError = $state<string | null>(null);
	let memberDatasourceHistory = $state<Record<string, unknown>>({});

	async function openMemberDetail(m: MemberDetail): Promise<void> {
		viewedMember = m;
		memberDetailError = null;
		memberDatasourceHistory = {};
		memberDetailModal?.open();
		if (!m.AccountId || !selectedGraphArn) return;
		memberDetailLoading = true;
		try {
			const [membersResp, datasourcesResp] = await Promise.all([
				client().send(
					new GetMembersCommand({ GraphArn: selectedGraphArn, AccountIds: [m.AccountId] })
				),
				client().send(
					new BatchGetGraphMemberDatasourcesCommand({
						GraphArn: selectedGraphArn,
						AccountIds: [m.AccountId]
					})
				)
			]);
			const unprocessed = membersResp.UnprocessedAccounts ?? [];
			if (unprocessed.length > 0) {
				memberDetailError = unprocessed[0].Reason ?? 'Unable to load full member detail.';
			}
			viewedMember = membersResp.MemberDetails?.[0] ?? m;
			memberDatasourceHistory =
				datasourcesResp.MemberDatasources?.[0]?.DatasourcePackageIngestHistory ?? {};
		} catch (e) {
			memberDetailError = describeError(e);
		} finally {
			memberDetailLoading = false;
		}
	}

	// --- Invitations: list / accept / reject / detail ---

	let invitationDetailModal = $state<Modal | null>(null);
	let viewedInvitation = $state<MemberDetail | null>(null);
	let invitationDatasourceHistory = $state<Record<string, unknown>>({});
	let invitationDatasourceError = $state<string | null>(null);

	async function openInvitationDetail(i: MemberDetail): Promise<void> {
		viewedInvitation = i;
		invitationDatasourceHistory = {};
		invitationDatasourceError = null;
		invitationDetailModal?.open();
		if (!i.GraphArn) return;
		try {
			const resp = await client().send(
				new BatchGetMembershipDatasourcesCommand({ GraphArns: [i.GraphArn] })
			);
			invitationDatasourceHistory = resp.MembershipDatasources?.[0]?.DatasourcePackageIngestHistory ?? {};
		} catch (e) {
			invitationDatasourceError = describeError(e);
		}
	}

	async function handleAcceptInvitation(i: MemberDetail): Promise<void> {
		if (!i.GraphArn) return;
		try {
			await client().send(new AcceptInvitationCommand({ GraphArn: i.GraphArn }));
			toast.success('Invitation accepted');
			await tabLoader.refresh('invitations');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleRejectInvitation(i: MemberDetail): Promise<void> {
		if (!i.GraphArn) return;
		const confirmed = await confirmDestructive({
			title: 'Reject invitation',
			message: `Reject the invitation to join ${i.GraphArn}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new RejectInvitationCommand({ GraphArn: i.GraphArn }));
			toast.success('Invitation rejected');
			await tabLoader.refresh('invitations');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleDisassociateMembership(i: MemberDetail): Promise<void> {
		if (!i.GraphArn) return;
		const confirmed = await confirmDestructive({
			title: 'Leave behavior graph',
			message: `Leave behavior graph ${i.GraphArn}? You will need a new invitation to rejoin.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DisassociateMembershipCommand({ GraphArn: i.GraphArn }));
			toast.success('Left behavior graph');
			await tabLoader.refresh('invitations');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Investigations: start / list / detail (with indicators) ---

	let startInvestigationModal = $state<Modal | null>(null);
	let startingInvestigation = $state(false);
	let startInvestigationError = $state<string | null>(null);
	let newEntityArn = $state('');
	let newScopeStart = $state('');
	let newScopeEnd = $state('');

	function openStartInvestigationModal(): void {
		startInvestigationError = selectedGraphArn
			? null
			: 'Select or create a behavior graph first.';
		newEntityArn = '';
		newScopeStart = '';
		newScopeEnd = '';
		startInvestigationModal?.open();
	}

	async function submitStartInvestigation(): Promise<void> {
		if (!selectedGraphArn) {
			startInvestigationError = 'Select or create a behavior graph first.';
			return;
		}
		if (!newEntityArn || !newScopeStart || !newScopeEnd) {
			startInvestigationError = 'Entity ARN, scope start, and scope end are required.';
			return;
		}
		startingInvestigation = true;
		startInvestigationError = null;
		try {
			await client().send(
				new StartInvestigationCommand({
					GraphArn: selectedGraphArn,
					EntityArn: newEntityArn,
					ScopeStartTime: new Date(newScopeStart),
					ScopeEndTime: new Date(newScopeEnd)
				})
			);
			toast.success('Investigation started');
			startInvestigationModal?.close();
			await tabLoader.refresh('investigations');
		} catch (e) {
			const msg = describeError(e);
			startInvestigationError = msg;
			toast.error(msg);
		} finally {
			startingInvestigation = false;
		}
	}

	let investigationDetailModal = $state<Modal | null>(null);
	let viewedInvestigation = $state<InvestigationDetail | null>(null);
	let viewedIndicators = $state<Indicator[]>([]);
	let investigationDetailLoading = $state(false);
	let investigationDetailError = $state<string | null>(null);

	async function openInvestigationDetail(inv: InvestigationDetail): Promise<void> {
		viewedInvestigation = inv;
		viewedIndicators = [];
		investigationDetailError = null;
		investigationDetailModal?.open();
		if (!inv.InvestigationId || !selectedGraphArn) return;
		investigationDetailLoading = true;
		try {
			const [detailResp, indicatorsResp] = await Promise.all([
				client().send(
					new GetInvestigationCommand({
						GraphArn: selectedGraphArn,
						InvestigationId: inv.InvestigationId
					})
				),
				client().send(
					new ListIndicatorsCommand({
						GraphArn: selectedGraphArn,
						InvestigationId: inv.InvestigationId
					})
				)
			]);
			viewedInvestigation = { ...inv, ...detailResp };
			viewedIndicators = indicatorsResp.Indicators ?? [];
		} catch (e) {
			investigationDetailError = describeError(e);
		} finally {
			investigationDetailLoading = false;
		}
	}

	let updatingInvestigationState = $state(false);

	async function updateInvestigationState(state: InvestigationState): Promise<void> {
		if (!viewedInvestigation?.InvestigationId || !selectedGraphArn) return;
		updatingInvestigationState = true;
		try {
			await client().send(
				new UpdateInvestigationStateCommand({
					GraphArn: selectedGraphArn,
					InvestigationId: viewedInvestigation.InvestigationId,
					State: state
				})
			);
			viewedInvestigation = { ...viewedInvestigation, State: state };
			toast.success(`Investigation ${state === 'ARCHIVED' ? 'archived' : 'reactivated'}`);
			await tabLoader.refresh('investigations');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			updatingInvestigationState = false;
		}
	}

	type IndicatorDetailValue = NonNullable<Indicator['IndicatorDetail']>;

	function flaggedIpSummary(d: IndicatorDetailValue): string | undefined {
		if (!d.FlaggedIpAddressDetail) return undefined;
		return `IP ${d.FlaggedIpAddressDetail.IpAddress ?? '—'} (${d.FlaggedIpAddressDetail.Reason ?? '—'})`;
	}

	function impossibleTravelSummary(d: IndicatorDetailValue): string | undefined {
		if (!d.ImpossibleTravelDetail) return undefined;
		return `${d.ImpossibleTravelDetail.StartingLocation ?? '—'} → ${d.ImpossibleTravelDetail.EndingLocation ?? '—'}`;
	}

	function newAsoSummary(d: IndicatorDetailValue): string | undefined {
		if (!d.NewAsoDetail) return undefined;
		return `ASO ${d.NewAsoDetail.Aso ?? '—'}`;
	}

	function newGeolocationSummary(d: IndicatorDetailValue): string | undefined {
		if (!d.NewGeolocationDetail) return undefined;
		return `Location ${d.NewGeolocationDetail.Location ?? '—'}`;
	}

	function newUserAgentSummary(d: IndicatorDetailValue): string | undefined {
		if (!d.NewUserAgentDetail) return undefined;
		return `User agent ${d.NewUserAgentDetail.UserAgent ?? '—'}`;
	}

	function relatedFindingSummary(d: IndicatorDetailValue): string | undefined {
		if (!d.RelatedFindingDetail) return undefined;
		return `Finding ${d.RelatedFindingDetail.Type ?? '—'}`;
	}

	function relatedFindingGroupSummary(d: IndicatorDetailValue): string | undefined {
		if (!d.RelatedFindingGroupDetail) return undefined;
		return `Finding group ${d.RelatedFindingGroupDetail.Id ?? '—'}`;
	}

	function ttpsObservedSummary(d: IndicatorDetailValue): string | undefined {
		if (!d.TTPsObservedDetail) return undefined;
		return `${d.TTPsObservedDetail.Tactic ?? '—'} / ${d.TTPsObservedDetail.Technique ?? '—'}`;
	}

	function indicatorSummary(ind: Indicator): string {
		const d = ind.IndicatorDetail;
		if (!d) return '—';
		return (
			flaggedIpSummary(d) ??
			impossibleTravelSummary(d) ??
			newAsoSummary(d) ??
			newGeolocationSummary(d) ??
			newUserAgentSummary(d) ??
			relatedFindingSummary(d) ??
			relatedFindingGroupSummary(d) ??
			ttpsObservedSummary(d) ??
			'—'
		);
	}

	// --- Organization: enable/disable delegated admin / list ---

	let enableAdminModal = $state<Modal | null>(null);
	let enablingAdmin = $state(false);
	let enableAdminError = $state<string | null>(null);
	let newAdminAccountId = $state('');

	function openEnableAdminModal(): void {
		enableAdminError = null;
		newAdminAccountId = '';
		enableAdminModal?.open();
	}

	async function submitEnableAdmin(): Promise<void> {
		if (!newAdminAccountId) {
			enableAdminError = 'Account ID is required.';
			return;
		}
		enablingAdmin = true;
		enableAdminError = null;
		try {
			await client().send(new EnableOrganizationAdminAccountCommand({ AccountId: newAdminAccountId }));
			toast.success('Organization admin account enabled');
			enableAdminModal?.close();
			await tabLoader.refresh('organization');
		} catch (e) {
			const msg = describeError(e);
			enableAdminError = msg;
			toast.error(msg);
		} finally {
			enablingAdmin = false;
		}
	}

	async function handleDisableAdmin(a: Administrator): Promise<void> {
		const confirmed = await confirmDestructive({
			title: 'Disable organization admin account',
			message: `Disable the Detective delegated administrator account ${a.AccountId ?? ''}? This deletes its organization behavior graph.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DisableOrganizationAdminAccountCommand({}));
			toast.success('Organization admin account disabled');
			await tabLoader.refresh('organization');
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Search}
		title="Amazon Detective"
		description="Investigate and analyze security findings"
		onRefresh={handleRefresh}
		color="rose"
	>
		{#snippet actions()}
			{#if activeTab === 'graphs'}
				<button
					onclick={openCreateGraphModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create graph
				</button>
			{:else if activeTab === 'members'}
				<button
					onclick={openCreateMemberModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Invite member
				</button>
			{:else if activeTab === 'investigations'}
				<button
					onclick={openStartInvestigationModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Start investigation
				</button>
			{:else if activeTab === 'organization'}
				<button
					onclick={openEnableAdminModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-rose-600 text-white hover:bg-rose-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Enable admin account
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="rose" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTab === 'members' || activeTab === 'investigations'}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="graph-select" class="text-sm text-gray-500 dark:text-gray-400"
						>Behavior graph</label
					>
					<select
						id="graph-select"
						value={selectedGraphArn}
						onchange={(e) => onGraphSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if graphs.length === 0}
							<option value="">No behavior graphs</option>
						{/if}
						{#each graphs as g (g.Arn)}
							<option value={g.Arn}>{g.Arn}</option>
						{/each}
					</select>
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

			{#if activeTab === 'graphs'}
				{#snippet graphRegionCell(_g: Graph)}
					<RegionChip region={currentRegion()} />
				{/snippet}
				{#snippet graphCreatedCell(g: Graph)}
					{formatDate(g.CreatedTime)}
				{/snippet}
				{#snippet graphActionsCell(g: Graph)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openGraphDetail(g)}
							title="View"
							aria-label="View graph {g.Arn}"
							class="text-gray-400 hover:text-rose-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteGraph(g)}
							title="Delete"
							aria-label="Delete graph {g.Arn}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const graphColumns = defineColumns<Graph>([
					{ key: 'Arn', label: 'ARN' },
					{ key: 'region', label: 'Region', render: graphRegionCell },
					{ key: 'CreatedTime', label: 'Created', render: graphCreatedCell },
					{ key: 'actions', label: '', render: graphActionsCell }
				])}
				<DataTable
					rows={filteredGraphs}
					rowKey={(g) => g.Arn ?? ''}
					columns={graphColumns}
					loading={tabLoader.isLoading('graphs')}
					emptyMessage="No behavior graphs found"
				/>
				<LoadMore
					hasMore={!!graphsNextToken}
					loading={loadingMoreGraphs}
					onLoadMore={loadMoreGraphs}
				/>
			{:else if activeTab === 'members'}
				{#snippet memberRegionCell(_m: MemberDetail)}
					<RegionChip region={currentRegion()} />
				{/snippet}
				{#snippet memberStatusCell(m: MemberDetail)}
					<span
						class="text-xs px-2 py-1 rounded-full {m.Status === 'ENABLED'
							? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
							: m.Status === 'VERIFICATION_FAILED'
								? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
								: m.Status === 'ACCEPTED_BUT_DISABLED'
									? 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400'
									: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}"
						>{m.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet memberInvitedCell(m: MemberDetail)}
					{formatDate(m.InvitedTime)}
				{/snippet}
				{#snippet memberActionsCell(m: MemberDetail)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openMemberDetail(m)}
							title="View"
							aria-label="View member {m.AccountId}"
							class="text-gray-400 hover:text-rose-500"><Eye class="w-4 h-4" /></button
						>
						{#if m.Status === 'ACCEPTED_BUT_DISABLED'}
							<button
								onclick={() => handleStartMonitoring(m)}
								title="Start monitoring"
								aria-label="Start monitoring member {m.AccountId}"
								class="text-gray-400 hover:text-green-500"><Play class="w-4 h-4" /></button
							>
						{/if}
						<button
							onclick={() => handleDeleteMember(m)}
							title="Remove"
							aria-label="Remove member {m.AccountId}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const memberColumns = defineColumns<MemberDetail>([
					{ key: 'AccountId', label: 'Account ID' },
					{ key: 'EmailAddress', label: 'Email' },
					{ key: 'region', label: 'Region', render: memberRegionCell },
					{ key: 'Status', label: 'Status', render: memberStatusCell },
					{ key: 'InvitedTime', label: 'Invited', render: memberInvitedCell },
					{ key: 'actions', label: '', render: memberActionsCell }
				])}
				<DataTable
					rows={filteredMembers}
					rowKey={(m) => m.AccountId ?? ''}
					columns={memberColumns}
					loading={tabLoader.isLoading('members')}
					emptyMessage="No member accounts found"
				/>
				<LoadMore
					hasMore={!!membersNextToken}
					loading={loadingMoreMembers}
					onLoadMore={loadMoreMembers}
				/>
			{:else if activeTab === 'invitations'}
				{#snippet invitationRegionCell(_i: MemberDetail)}
					<RegionChip region={currentRegion()} />
				{/snippet}
				{#snippet invitationStatusCell(i: MemberDetail)}
					<span
						class="text-xs px-2 py-1 rounded-full {i.Status === 'ENABLED'
							? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
							: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}"
						>{i.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet invitationInvitedCell(i: MemberDetail)}
					{formatDate(i.InvitedTime)}
				{/snippet}
				{#snippet invitationActionsCell(i: MemberDetail)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openInvitationDetail(i)}
							title="View"
							aria-label="View invitation to {i.GraphArn}"
							class="text-gray-400 hover:text-rose-500"><Eye class="w-4 h-4" /></button
						>
						{#if i.Status === 'INVITED'}
							<button
								onclick={() => handleAcceptInvitation(i)}
								title="Accept"
								aria-label="Accept invitation to {i.GraphArn}"
								class="text-gray-400 hover:text-green-500"><Check class="w-4 h-4" /></button
							>
							<button
								onclick={() => handleRejectInvitation(i)}
								title="Reject"
								aria-label="Reject invitation to {i.GraphArn}"
								class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button
							>
						{:else if i.Status === 'ENABLED'}
							<button
								onclick={() => handleDisassociateMembership(i)}
								title="Leave"
								aria-label="Leave behavior graph {i.GraphArn}"
								class="text-gray-400 hover:text-red-500"><LogOut class="w-4 h-4" /></button
							>
						{/if}
					</div>
				{/snippet}
				{@const invitationColumns = defineColumns<MemberDetail>([
					{ key: 'GraphArn', label: 'Behavior Graph' },
					{ key: 'AdministratorId', label: 'Administrator' },
					{ key: 'region', label: 'Region', render: invitationRegionCell },
					{ key: 'Status', label: 'Status', render: invitationStatusCell },
					{ key: 'InvitedTime', label: 'Invited', render: invitationInvitedCell },
					{ key: 'actions', label: '', render: invitationActionsCell }
				])}
				<DataTable
					rows={filteredInvitations}
					rowKey={(i) => i.GraphArn ?? ''}
					columns={invitationColumns}
					loading={tabLoader.isLoading('invitations')}
					emptyMessage="No invitations found"
				/>
				<LoadMore
					hasMore={!!invitationsNextToken}
					loading={loadingMoreInvitations}
					onLoadMore={loadMoreInvitations}
				/>
			{:else if activeTab === 'investigations'}
				{#snippet investigationRegionCell(_inv: InvestigationDetail)}
					<RegionChip region={currentRegion()} />
				{/snippet}
				{#snippet investigationCreatedCell(inv: InvestigationDetail)}
					{formatDate(inv.CreatedTime)}
				{/snippet}
				{#snippet investigationSeverityCell(inv: InvestigationDetail)}
					<span
						class="text-xs px-2 py-1 rounded-full {inv.Severity === 'CRITICAL' ||
						inv.Severity === 'HIGH'
							? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'
							: inv.Severity === 'MEDIUM'
								? 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400'
								: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}"
						>{inv.Severity ?? '—'}</span
					>
				{/snippet}
				{#snippet investigationActionsCell(inv: InvestigationDetail)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => openInvestigationDetail(inv)}
							title="View"
							aria-label="View investigation {inv.InvestigationId}"
							class="text-gray-400 hover:text-rose-500"><Eye class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const investigationColumns = defineColumns<InvestigationDetail>([
					{ key: 'InvestigationId', label: 'Investigation ID' },
					{ key: 'EntityArn', label: 'Entity' },
					{ key: 'region', label: 'Region', render: investigationRegionCell },
					{ key: 'Severity', label: 'Severity', render: investigationSeverityCell },
					{ key: 'Status', label: 'Status' },
					{ key: 'State', label: 'State' },
					{ key: 'CreatedTime', label: 'Created', render: investigationCreatedCell },
					{ key: 'actions', label: '', render: investigationActionsCell }
				])}
				<DataTable
					rows={filteredInvestigations}
					rowKey={(inv) => inv.InvestigationId ?? ''}
					columns={investigationColumns}
					loading={tabLoader.isLoading('investigations')}
					emptyMessage="No investigations found"
				/>
				<LoadMore
					hasMore={!!investigationsNextToken}
					loading={loadingMoreInvestigations}
					onLoadMore={loadMoreInvestigations}
				/>
			{:else if activeTab === 'organization'}
				{#snippet adminDelegatedCell(a: Administrator)}
					{formatDate(a.DelegationTime)}
				{/snippet}
				{#snippet adminActionsCell(a: Administrator)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => handleDisableAdmin(a)}
							title="Disable"
							aria-label="Disable admin account {a.AccountId}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const adminColumns = defineColumns<Administrator>([
					{ key: 'AccountId', label: 'Account ID' },
					{ key: 'GraphArn', label: 'Organization Behavior Graph' },
					{ key: 'DelegationTime', label: 'Delegated', render: adminDelegatedCell },
					{ key: 'actions', label: '', render: adminActionsCell }
				])}
				<DataTable
					rows={filteredAdmins}
					rowKey={(a) => a.AccountId ?? ''}
					columns={adminColumns}
					loading={tabLoader.isLoading('organization')}
					emptyMessage="No organization admin account is delegated"
				/>
				<LoadMore
					hasMore={!!adminsNextToken}
					loading={loadingMoreAdmins}
					onLoadMore={loadMoreAdmins}
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createGraphModal} title="Create Behavior Graph">
	{#snippet children()}
		<p class="text-sm text-slate-600 dark:text-slate-300">
			Create a new Detective behavior graph for this account. Detective is idempotent per
			account: if one already exists, its ARN is returned instead of creating a duplicate.
		</p>
		{#if createGraphError}
			<p class="text-sm text-red-600 dark:text-red-400">{createGraphError}</p>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createGraphModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateGraph}
			disabled={creatingGraph}
			class="rounded-lg bg-rose-600 px-4 py-2 text-sm font-semibold text-white hover:bg-rose-700 disabled:opacity-50"
			>{creatingGraph ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={graphDetailModal} title="Behavior Graph">
	{#snippet children()}
		{#if viewedGraph}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedGraph.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedGraph.CreatedTime)}</dd>
				</div>
				{#if graphDetailLoading}
					<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
				{:else}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Tags</dt>
						<dd class="text-slate-900 dark:text-white">
							{#if Object.keys(graphTags).length === 0}
								<span class="text-slate-500 dark:text-slate-400">No tags</span>
							{:else}
								<ul class="space-y-1">
									{#each Object.entries(graphTags) as [key, value] (key)}
										<li class="flex items-center gap-2">
											<span
												class="px-2 py-0.5 rounded-full bg-gray-100 dark:bg-slate-700 text-xs"
												>{key} = {value}</span
											>
											<button
												onclick={() => removeGraphTag(key)}
												aria-label="Remove tag {key}"
												class="text-gray-400 hover:text-red-500"><X class="w-3 h-3" /></button
											>
										</li>
									{/each}
								</ul>
							{/if}
							<div class="mt-2 flex items-center gap-2">
								<input
									bind:value={newGraphTagKey}
									placeholder="Key"
									aria-label="New tag key"
									class="w-1/3 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
								<input
									bind:value={newGraphTagValue}
									placeholder="Value"
									aria-label="New tag value"
									class="w-1/3 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
								<button
									type="button"
									onclick={addGraphTag}
									class="px-2 py-1 text-xs rounded-lg bg-rose-600 text-white hover:bg-rose-700"
									>Add</button
								>
							</div>
							{#if graphTagsError}
								<p class="mt-1 text-sm text-red-600 dark:text-red-400">{graphTagsError}</p>
							{/if}
						</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Datasource packages</dt>
						<dd class="text-slate-900 dark:text-white">
							{#if Object.keys(graphPackages).length === 0}
								<span class="text-slate-500 dark:text-slate-400">No datasource packages started</span>
							{:else}
								<ul class="space-y-1">
									{#each Object.entries(graphPackages) as [pkg, detail] (pkg)}
										<li>
											<span class="px-2 py-0.5 rounded-full bg-gray-100 dark:bg-slate-700 text-xs"
												>{pkg}: {detail.DatasourcePackageIngestState ?? '—'}</span
											>
										</li>
									{/each}
								</ul>
							{/if}
							<div class="mt-2 flex items-center gap-2">
								<select
									bind:value={newGraphPackage}
									aria-label="Datasource package to start"
									class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								>
									{#each Object.values(DatasourcePackage) as pkg (pkg)}
										<option value={pkg}>{pkg}</option>
									{/each}
								</select>
								<button
									type="button"
									onclick={addGraphPackage}
									class="px-2 py-1 text-xs rounded-lg bg-rose-600 text-white hover:bg-rose-700"
									>Start</button
								>
							</div>
							{#if graphPackagesError}
								<p class="mt-1 text-sm text-red-600 dark:text-red-400">{graphPackagesError}</p>
							{/if}
						</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Organization configuration</dt>
						<dd class="text-slate-900 dark:text-white">
							<label class="flex items-center gap-2">
								<input
									type="checkbox"
									checked={graphAutoEnable}
									disabled={updatingAutoEnable}
									onchange={(e) => toggleGraphAutoEnable((e.target as HTMLInputElement).checked)}
								/>
								Auto-enable new organization accounts
							</label>
							{#if graphOrgConfigError}
								<p class="mt-1 text-sm text-red-600 dark:text-red-400">{graphOrgConfigError}</p>
							{/if}
						</dd>
					</div>
				{/if}
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => graphDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createMemberModal} title="Invite Member Account">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="member-account-id" class="text-sm text-slate-600 dark:text-slate-300"
					>Account ID</label
				>
				<input
					id="member-account-id"
					bind:value={newMemberAccountId}
					placeholder="123456789012"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="member-email" class="text-sm text-slate-600 dark:text-slate-300"
					>Email address</label
				>
				<input
					id="member-email"
					type="email"
					bind:value={newMemberEmail}
					placeholder="admin@example.com"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="member-message" class="text-sm text-slate-600 dark:text-slate-300"
					>Invitation message (optional)</label
				>
				<textarea
					id="member-message"
					bind:value={newMemberMessage}
					rows="2"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newMemberDisableEmail} />
				Do not send an email notification
			</label>
			{#if createMemberError}
				<p class="text-sm text-red-600 dark:text-red-400">{createMemberError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createMemberModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateMember}
			disabled={creatingMember}
			class="rounded-lg bg-rose-600 px-4 py-2 text-sm font-semibold text-white hover:bg-rose-700 disabled:opacity-50"
			>{creatingMember ? 'Inviting…' : 'Invite'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={memberDetailModal} title="Member Account">
	{#snippet children()}
		{#if memberDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedMember}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Account ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedMember.AccountId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Email</dt>
					<dd class="text-slate-900 dark:text-white">{viewedMember.EmailAddress ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedMember.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Administrator account</dt>
					<dd class="text-slate-900 dark:text-white">{viewedMember.AdministratorId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Invitation type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedMember.InvitationType ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Invited</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedMember.InvitedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedMember.UpdatedTime)}</dd>
				</div>
			</dl>
			{#if memberDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{memberDetailError}</p>
			{/if}
			<div class="mt-4">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white">
					Datasource ingest history
				</h3>
				{#if Object.keys(memberDatasourceHistory).length === 0}
					<p class="text-sm text-slate-500 dark:text-slate-400">No datasource history found.</p>
				{:else}
					<pre
						class="mt-1 max-h-48 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(
							memberDatasourceHistory,
							null,
							2
						)}</pre>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => memberDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={invitationDetailModal} title="Invitation">
	{#snippet children()}
		{#if viewedInvitation}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Behavior graph</dt>
					<dd class="break-all text-slate-900 dark:text-white">
						{viewedInvitation.GraphArn ?? '—'}
					</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Administrator account</dt>
					<dd class="text-slate-900 dark:text-white">{viewedInvitation.AdministratorId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedInvitation.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Invited</dt>
					<dd class="text-slate-900 dark:text-white">
						{formatDate(viewedInvitation.InvitedTime)}
					</dd>
				</div>
			</dl>
			<div class="mt-4">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white">
					Datasource ingest history
				</h3>
				{#if invitationDatasourceError}
					<p class="text-sm text-red-600 dark:text-red-400">{invitationDatasourceError}</p>
				{:else if Object.keys(invitationDatasourceHistory).length === 0}
					<p class="text-sm text-slate-500 dark:text-slate-400">No datasource history found.</p>
				{:else}
					<pre
						class="mt-1 max-h-48 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(
							invitationDatasourceHistory,
							null,
							2
						)}</pre>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => invitationDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={startInvestigationModal} title="Start Investigation">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="investigation-entity-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>Entity ARN (IAM user or role)</label
				>
				<input
					id="investigation-entity-arn"
					bind:value={newEntityArn}
					placeholder="arn:aws:iam::123456789012:user/example"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
				<div>
					<label for="investigation-scope-start" class="text-sm text-slate-600 dark:text-slate-300"
						>Scope start</label
					>
					<input
						id="investigation-scope-start"
						type="datetime-local"
						bind:value={newScopeStart}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="investigation-scope-end" class="text-sm text-slate-600 dark:text-slate-300"
						>Scope end</label
					>
					<input
						id="investigation-scope-end"
						type="datetime-local"
						bind:value={newScopeEnd}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			{#if startInvestigationError}
				<p class="text-sm text-red-600 dark:text-red-400">{startInvestigationError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => startInvestigationModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitStartInvestigation}
			disabled={startingInvestigation}
			class="rounded-lg bg-rose-600 px-4 py-2 text-sm font-semibold text-white hover:bg-rose-700 disabled:opacity-50"
			>{startingInvestigation ? 'Starting…' : 'Start'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={investigationDetailModal} title="Investigation">
	{#snippet children()}
		{#if investigationDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedInvestigation}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Investigation ID</dt>
					<dd class="text-slate-900 dark:text-white">
						{viewedInvestigation.InvestigationId ?? '—'}
					</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Entity</dt>
					<dd class="break-all text-slate-900 dark:text-white">
						{viewedInvestigation.EntityArn ?? '—'}
					</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Severity</dt>
					<dd class="text-slate-900 dark:text-white">{viewedInvestigation.Severity ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedInvestigation.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">State</dt>
					<dd class="text-slate-900 dark:text-white flex items-center gap-2">
						{viewedInvestigation.State ?? '—'}
						{#if viewedInvestigation.State === 'ACTIVE'}
							<button
								type="button"
								onclick={() => updateInvestigationState('ARCHIVED')}
								disabled={updatingInvestigationState}
								class="px-2 py-0.5 text-xs rounded-lg bg-rose-600 text-white hover:bg-rose-700 disabled:opacity-50"
								>Archive</button
							>
						{:else if viewedInvestigation.State === 'ARCHIVED'}
							<button
								type="button"
								onclick={() => updateInvestigationState('ACTIVE')}
								disabled={updatingInvestigationState}
								class="px-2 py-0.5 text-xs rounded-lg bg-rose-600 text-white hover:bg-rose-700 disabled:opacity-50"
								>Reactivate</button
							>
						{/if}
					</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">
						{formatDate(viewedInvestigation.CreatedTime)}
					</dd>
				</div>
			</dl>
			<div class="mt-4">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white">Indicators</h3>
				{#if investigationDetailError}
					<p class="text-sm text-red-600 dark:text-red-400">{investigationDetailError}</p>
				{:else if viewedIndicators.length === 0}
					<p class="text-sm text-slate-500 dark:text-slate-400">No indicators found.</p>
				{:else}
					<ul class="mt-2 space-y-1 text-sm">
						{#each viewedIndicators as ind, index (index)}
							<li class="text-slate-700 dark:text-slate-300">
								<span class="font-medium">{ind.IndicatorType ?? 'UNKNOWN'}</span>: {indicatorSummary(
									ind
								)}
							</li>
						{/each}
					</ul>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => investigationDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={enableAdminModal} title="Enable Organization Admin Account">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Delegate a Detective administrator account for the organization in this Region. If the
				account is not yet enabled for Detective, this also creates its behavior graph.
			</p>
			<div>
				<label for="admin-account-id" class="text-sm text-slate-600 dark:text-slate-300"
					>Account ID</label
				>
				<input
					id="admin-account-id"
					bind:value={newAdminAccountId}
					placeholder="123456789012"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if enableAdminError}
				<p class="text-sm text-red-600 dark:text-red-400">{enableAdminError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => enableAdminModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEnableAdmin}
			disabled={enablingAdmin}
			class="rounded-lg bg-rose-600 px-4 py-2 text-sm font-semibold text-white hover:bg-rose-700 disabled:opacity-50"
			>{enablingAdmin ? 'Enabling…' : 'Enable'}</button
		>
	{/snippet}
</Modal>
