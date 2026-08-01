<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getManagedBlockchainClient } from '$lib/aws-client';
	import {
		ListNetworksCommand,
		CreateNetworkCommand,
		ListMembersCommand,
		CreateMemberCommand,
		DeleteMemberCommand,
		UpdateMemberCommand,
		ListNodesCommand,
		CreateNodeCommand,
		DeleteNodeCommand,
		UpdateNodeCommand,
		ListAccessorsCommand,
		CreateAccessorCommand,
		DeleteAccessorCommand,
		ListProposalsCommand,
		CreateProposalCommand,
		GetProposalCommand,
		ListProposalVotesCommand,
		VoteOnProposalCommand,
		ListInvitationsCommand,
		RejectInvitationCommand,
		AccessorNetworkType,
		type NetworkSummary,
		type MemberSummary,
		type NodeSummary,
		type Proposal,
		type ProposalSummary,
		type VoteSummary,
		type AccessorSummary,
		type Invitation
	} from '@aws-sdk/client-managedblockchain';
	import { toast } from 'svelte-sonner';
	import {
		Network,
		Users,
		Server,
		Key,
		Mail,
		RefreshCw,
		Plus,
		Trash2,
		ChevronRight,
		ChevronDown,
		Globe,
		CheckCircle,
		XCircle,
		Clock,
		Edit2,
		Vote,
		Activity
	} from 'lucide-svelte';

	const client = regionalClient(getManagedBlockchainClient);

	type Tab = 'networks' | 'accessors' | 'proposals' | 'invitations' | 'topology';
	let activeTab = $state<Tab>('networks');

	// ── Networks ─────────────────────────────────────────────────────────────
	let networks = $state<NetworkSummary[]>([]);
	let loadingNetworks = $state(false);
	let selectedNetworkId = $state<string | null>(null);
	let expandedNetworks = $state<Set<string>>(new Set());

	// Create network form
	let showCreateNetwork = $state(false);
	let creatingNetwork = $state(false);
	let newNetworkName = $state('');
	let newNetworkDesc = $state('');
	let newNetworkFramework = $state('HYPERLEDGER_FABRIC');
	let newNetworkVersion = $state('2.2');
	let newMemberName = $state('');
	let newMemberDesc = $state('');

	// ── Members ───────────────────────────────────────────────────────────────
	let membersByNetwork = $state<Record<string, MemberSummary[]>>({});
	let loadingMembers = $state<Record<string, boolean>>({});

	// Create member form
	let showCreateMember = $state(false);
	let createMemberNetworkId = $state('');
	let newMemberFormName = $state('');
	let newMemberFormDesc = $state('');
	let creatingMember = $state(false);

	// Edit member
	let showEditMember = $state(false);
	let editMemberNetworkId = $state('');
	let editMemberId = $state('');
	let editingMember = $state(false);

	// ── Nodes ─────────────────────────────────────────────────────────────────
	let nodesByMember = $state<Record<string, NodeSummary[]>>({});
	let loadingNodes = $state<Record<string, boolean>>({});
	let expandedMembers = $state<Set<string>>(new Set());

	// Create node form
	let showCreateNode = $state(false);
	let createNodeNetworkId = $state('');
	let createNodeMemberId = $state('');
	let newNodeInstanceType = $state('bc.t3.small');
	let newNodeAZ = $state('us-east-1a');
	let creatingNode = $state(false);

	// ── Accessors ─────────────────────────────────────────────────────────────
	let accessors = $state<AccessorSummary[]>([]);
	let loadingAccessors = $state(false);
	let showCreateAccessor = $state(false);
	let newAccessorType = $state('BILLING_TOKEN');
	let newAccessorNetworkType = $state<AccessorNetworkType>(AccessorNetworkType.ETHEREUM_MAINNET);
	let creatingAccessor = $state(false);

	// ── Proposals ─────────────────────────────────────────────────────────────
	let proposalNetworkId = $state('');
	let proposals = $state<ProposalSummary[]>([]);
	let loadingProposals = $state(false);
	let selectedProposal = $state<Proposal | null>(null);
	let proposalVotes = $state<VoteSummary[]>([]);
	let loadingVotes = $state(false);
	let showCreateProposal = $state(false);
	let newProposalMemberId = $state('');
	let newProposalDesc = $state('');
	let creatingProposal = $state(false);
	let showVoteForm = $state(false);
	let voteProposalId = $state('');
	let voteMemberId = $state('');
	let voteChoice = $state<'YES' | 'NO'>('YES');
	let castingVote = $state(false);

	// ── Invitations ───────────────────────────────────────────────────────────
	let invitations = $state<Invitation[]>([]);
	let loadingInvitations = $state(false);

	// ── Topology ──────────────────────────────────────────────────────────────
	let topoNetworks = $state<Array<{ id: string; name: string; status: string; memberCount: number }>>([]);
	let loadingTopo = $state(false);

	// ── Helpers ───────────────────────────────────────────────────────────────
	function nodeKey(networkId: string, memberId: string) {
		return `${networkId}/${memberId}`;
	}

	function statusColor(status: string | undefined): string {
		if (!status) return 'text-gray-400';
		switch (status.toUpperCase()) {
			case 'AVAILABLE':
				return 'text-green-500';
			case 'IN_PROGRESS':
			case 'PENDING':
				return 'text-yellow-500';
			case 'DELETED':
			case 'FAILED':
				return 'text-red-500';
			default:
				return 'text-gray-400';
		}
	}

	function proposalStatusColor(status: string | undefined): string {
		if (!status) return 'text-gray-400';
		switch (status) {
			case 'APPROVED':
				return 'text-green-500';
			case 'IN_PROGRESS':
				return 'text-yellow-500';
			case 'REJECTED':
			case 'EXPIRED':
				return 'text-red-500';
			default:
				return 'text-gray-400';
		}
	}

	// ── Load functions ────────────────────────────────────────────────────────
	async function loadNetworks() {
		loadingNetworks = true;
		try {
			const r = await client().send(new ListNetworksCommand({}));
			networks = r.Networks ?? [];
		} catch (e) {
			toast.error('Failed to load networks: ' + String(e));
		} finally {
			loadingNetworks = false;
		}
	}

	async function loadMembers(networkId: string) {
		loadingMembers = { ...loadingMembers, [networkId]: true };
		try {
			const r = await client().send(new ListMembersCommand({ NetworkId: networkId }));
			membersByNetwork = { ...membersByNetwork, [networkId]: r.Members ?? [] };
		} catch (e) {
			toast.error('Failed to load members: ' + String(e));
		} finally {
			loadingMembers = { ...loadingMembers, [networkId]: false };
		}
	}

	async function loadNodes(networkId: string, memberId: string) {
		const key = nodeKey(networkId, memberId);
		loadingNodes = { ...loadingNodes, [key]: true };
		try {
			const r = await client().send(new ListNodesCommand({ NetworkId: networkId, MemberId: memberId }));
			nodesByMember = { ...nodesByMember, [key]: r.Nodes ?? [] };
		} catch (e) {
			toast.error('Failed to load nodes: ' + String(e));
		} finally {
			loadingNodes = { ...loadingNodes, [key]: false };
		}
	}

	async function loadAccessors() {
		loadingAccessors = true;
		try {
			const r = await client().send(new ListAccessorsCommand({}));
			accessors = r.Accessors ?? [];
		} catch (e) {
			toast.error('Failed to load accessors: ' + String(e));
		} finally {
			loadingAccessors = false;
		}
	}

	async function loadProposals() {
		if (!proposalNetworkId) return;
		loadingProposals = true;
		try {
			const r = await client().send(new ListProposalsCommand({ NetworkId: proposalNetworkId }));
			proposals = r.Proposals ?? [];
		} catch (e) {
			toast.error('Failed to load proposals: ' + String(e));
		} finally {
			loadingProposals = false;
		}
	}

	async function selectProposal(p: ProposalSummary) {
		loadingVotes = true;
		try {
			const proposalId = p.ProposalId ?? '';
			const [fullR, votesR] = await Promise.all([
				client().send(new GetProposalCommand({ NetworkId: proposalNetworkId, ProposalId: proposalId })),
				client().send(new ListProposalVotesCommand({ NetworkId: proposalNetworkId, ProposalId: proposalId }))
			]);
			selectedProposal = fullR.Proposal ?? null;
			proposalVotes = votesR.ProposalVotes ?? [];
		} catch (e) {
			toast.error('Failed to load proposal details: ' + String(e));
		} finally {
			loadingVotes = false;
		}
	}

	async function loadInvitations() {
		loadingInvitations = true;
		try {
			const r = await client().send(new ListInvitationsCommand({}));
			invitations = r.Invitations ?? [];
		} catch (e) {
			toast.error('Failed to load invitations: ' + String(e));
		} finally {
			loadingInvitations = false;
		}
	}

	async function loadTopology() {
		loadingTopo = true;
		try {
			const r = await client().send(new ListNetworksCommand({}));
			const nets = r.Networks ?? [];
			const results = await Promise.all(
				nets.map(async (n) => {
					try {
						const m = await client().send(new ListMembersCommand({ NetworkId: n.Id! }));
						return { id: n.Id!, name: n.Name!, status: n.Status!, memberCount: (m.Members ?? []).length };
					} catch {
						return { id: n.Id!, name: n.Name!, status: n.Status!, memberCount: 0 };
					}
				})
			);
			topoNetworks = results;
		} catch (e) {
			toast.error('Failed to load topology: ' + String(e));
		} finally {
			loadingTopo = false;
		}
	}

	// ── Actions ───────────────────────────────────────────────────────────────
	async function createNetwork() {
		if (!newNetworkName || !newMemberName) {
			toast.error('Network name and initial member name are required');
			return;
		}
		creatingNetwork = true;
		try {
			await client().send(
				new CreateNetworkCommand({
					Name: newNetworkName,
					Description: newNetworkDesc,
					Framework: newNetworkFramework as 'HYPERLEDGER_FABRIC' | 'ETHEREUM',
					FrameworkVersion: newNetworkVersion,
					MemberConfiguration: {
						Name: newMemberName,
						Description: newMemberDesc,
						FrameworkConfiguration: { Fabric: { AdminUsername: 'admin', AdminPassword: 'Admin12345!' } }
					},
					VotingPolicy: { ApprovalThresholdPolicy: { ThresholdPercentage: 50, ProposalDurationInHours: 24, ThresholdComparator: 'GREATER_THAN_OR_EQUAL_TO' } }
				})
			);
			toast.success('Network created');
			showCreateNetwork = false;
			newNetworkName = '';
			newNetworkDesc = '';
			newMemberName = '';
			newMemberDesc = '';
			await loadNetworks();
		} catch (e) {
			toast.error('Failed to create network: ' + String(e));
		} finally {
			creatingNetwork = false;
		}
	}

	async function toggleNetworkExpanded(networkId: string) {
		const next = new Set(expandedNetworks);
		if (next.has(networkId)) {
			next.delete(networkId);
		} else {
			next.add(networkId);
			if (!membersByNetwork[networkId]) {
				await loadMembers(networkId);
			}
		}
		expandedNetworks = next;
	}

	async function toggleMemberExpanded(networkId: string, memberId: string) {
		const key = nodeKey(networkId, memberId);
		const next = new Set(expandedMembers);
		if (next.has(key)) {
			next.delete(key);
		} else {
			next.add(key);
			if (!nodesByMember[key]) {
				await loadNodes(networkId, memberId);
			}
		}
		expandedMembers = next;
	}

	async function createMember() {
		if (!newMemberFormName) {
			toast.error('Member name is required');
			return;
		}
		creatingMember = true;
		try {
			await client().send(
				new CreateMemberCommand({
					NetworkId: createMemberNetworkId,
					InvitationId: 'N/A',
					MemberConfiguration: {
						Name: newMemberFormName,
						Description: newMemberFormDesc,
						FrameworkConfiguration: { Fabric: { AdminUsername: 'admin', AdminPassword: 'Admin12345!' } }
					}
				})
			);
			toast.success('Member created');
			showCreateMember = false;
			newMemberFormName = '';
			newMemberFormDesc = '';
			await loadMembers(createMemberNetworkId);
		} catch (e) {
			toast.error('Failed to create member: ' + String(e));
		} finally {
			creatingMember = false;
		}
	}

	async function deleteMember(networkId: string, memberId: string) {
		const confirmed = await confirmDestructive({
			title: 'Delete Member',
			message: 'This will permanently delete the member and all its nodes.',
			confirmLabel: 'Delete'
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteMemberCommand({ NetworkId: networkId, MemberId: memberId }));
			toast.success('Member deleted');
			await loadMembers(networkId);
		} catch (e) {
			toast.error('Failed to delete member: ' + String(e));
		}
	}

	async function updateMember() {
		editingMember = true;
		try {
			await client().send(
				new UpdateMemberCommand({
					NetworkId: editMemberNetworkId,
					MemberId: editMemberId
				})
			);
			toast.success('Member updated');
			showEditMember = false;
			await loadMembers(editMemberNetworkId);
		} catch (e) {
			toast.error('Failed to update member: ' + String(e));
		} finally {
			editingMember = false;
		}
	}

	async function createNode() {
		if (!newNodeInstanceType) {
			toast.error('Instance type is required');
			return;
		}
		creatingNode = true;
		try {
			await client().send(
				new CreateNodeCommand({
					NetworkId: createNodeNetworkId,
					MemberId: createNodeMemberId,
					NodeConfiguration: { InstanceType: newNodeInstanceType, AvailabilityZone: newNodeAZ }
				})
			);
			toast.success('Node created');
			showCreateNode = false;
			newNodeInstanceType = 'bc.t3.small';
			newNodeAZ = 'us-east-1a';
			await loadNodes(createNodeNetworkId, createNodeMemberId);
		} catch (e) {
			toast.error('Failed to create node: ' + String(e));
		} finally {
			creatingNode = false;
		}
	}

	async function deleteNode(networkId: string, memberId: string, nodeId: string) {
		const confirmed = await confirmDestructive({
			title: 'Delete Node',
			message: 'This will permanently delete the peer node.',
			confirmLabel: 'Delete'
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteNodeCommand({ NetworkId: networkId, MemberId: memberId, NodeId: nodeId })
			);
			toast.success('Node deleted');
			await loadNodes(networkId, memberId);
		} catch (e) {
			toast.error('Failed to delete node: ' + String(e));
		}
	}

	async function patchNode(networkId: string, memberId: string, nodeId: string) {
		try {
			await client().send(
				new UpdateNodeCommand({ NetworkId: networkId, MemberId: memberId, NodeId: nodeId })
			);
			toast.success('Node updated');
		} catch (e) {
			toast.error('Failed to update node: ' + String(e));
		}
	}

	async function createAccessor() {
		creatingAccessor = true;
		try {
			await client().send(
				new CreateAccessorCommand({
					AccessorType: newAccessorType as 'BILLING_TOKEN',
					NetworkType: newAccessorNetworkType
				})
			);
			toast.success('Accessor created');
			showCreateAccessor = false;
			await loadAccessors();
		} catch (e) {
			toast.error('Failed to create accessor: ' + String(e));
		} finally {
			creatingAccessor = false;
		}
	}

	async function deleteAccessor(accessorId: string) {
		const confirmed = await confirmDestructive({
			title: 'Delete Accessor',
			message: 'This will permanently delete the accessor token.',
			confirmLabel: 'Delete'
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAccessorCommand({ AccessorId: accessorId }));
			toast.success('Accessor deleted');
			await loadAccessors();
		} catch (e) {
			toast.error('Failed to delete accessor: ' + String(e));
		}
	}

	async function createProposal() {
		if (!newProposalMemberId) {
			toast.error('Proposing member ID is required');
			return;
		}
		creatingProposal = true;
		try {
			await client().send(
				new CreateProposalCommand({
					NetworkId: proposalNetworkId,
					MemberId: newProposalMemberId,
					Description: newProposalDesc,
					Actions: {}
				})
			);
			toast.success('Proposal created');
			showCreateProposal = false;
			newProposalMemberId = '';
			newProposalDesc = '';
			await loadProposals();
		} catch (e) {
			toast.error('Failed to create proposal: ' + String(e));
		} finally {
			creatingProposal = false;
		}
	}

	async function castVote() {
		if (!voteMemberId) {
			toast.error('Voter member ID is required');
			return;
		}
		castingVote = true;
		try {
			await client().send(
				new VoteOnProposalCommand({
					NetworkId: proposalNetworkId,
					ProposalId: voteProposalId,
					VoterMemberId: voteMemberId,
					Vote: voteChoice
				})
			);
			toast.success('Vote cast');
			showVoteForm = false;
			voteMemberId = '';
			if (selectedProposal?.ProposalId) {
				const proposalId = selectedProposal.ProposalId;
				const [fullR, votesR] = await Promise.all([
					client().send(new GetProposalCommand({ NetworkId: proposalNetworkId, ProposalId: proposalId })),
					client().send(new ListProposalVotesCommand({ NetworkId: proposalNetworkId, ProposalId: proposalId }))
				]);
				if (fullR.Proposal) selectedProposal = fullR.Proposal;
				proposalVotes = votesR.ProposalVotes ?? [];
			}
		} catch (e) {
			toast.error('Failed to cast vote: ' + String(e));
		} finally {
			castingVote = false;
		}
	}

	async function rejectInvitation(invitationId: string) {
		const confirmed = await confirmDestructive({
			title: 'Reject Invitation',
			message: 'Reject this network invitation?',
			confirmLabel: 'Reject'
		});
		if (!confirmed) return;
		try {
			await client().send(new RejectInvitationCommand({ InvitationId: invitationId }));
			toast.success('Invitation rejected');
			await loadInvitations();
		} catch (e) {
			toast.error('Failed to reject invitation: ' + String(e));
		}
	}

	// ── Lifecycle ─────────────────────────────────────────────────────────────
	// Networks and everything nested under them (members, nodes), plus
	// accessors, proposals, invitations, and topology, are all region-scoped,
	// so a region switch must clear them (not just reload) and re-run whatever
	// tab is currently active. `activeTab` is read with `untrack` because it is
	// also written by the tab-switch buttons below: without it, this effect
	// would depend on `activeTab` too and wipe all this state on every tab
	// click, not just on a region change.
	onRegionChange(() => {
		networks = [];
		membersByNetwork = {};
		nodesByMember = {};
		expandedNetworks = new Set();
		expandedMembers = new Set();
		selectedNetworkId = null;
		accessors = [];
		proposalNetworkId = '';
		proposals = [];
		selectedProposal = null;
		proposalVotes = [];
		invitations = [];
		topoNetworks = [];
		showCreateNetwork = false;
		showCreateMember = false;
		showEditMember = false;
		showCreateNode = false;
		showCreateAccessor = false;
		showCreateProposal = false;
		showVoteForm = false;
		void loadNetworks();
		const tab = untrack(() => activeTab);
		if (tab === 'accessors') void loadAccessors();
		else if (tab === 'invitations') void loadInvitations();
		else if (tab === 'topology') void loadTopology();
	});

	$effect(() => {
		if (activeTab === 'accessors') loadAccessors();
		else if (activeTab === 'invitations') loadInvitations();
		else if (activeTab === 'topology') loadTopology();
	});
</script>

<div class="mx-auto max-w-7xl px-4 py-6">
	<div class="mb-6 flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Network class="h-7 w-7 text-blue-500" />
			<h1 class="text-2xl font-bold">Managed Blockchain</h1>
		</div>
	</div>

	<!-- Tabs -->
	<div class="mb-6 flex gap-1 border-b border-gray-700">
		{#each [
			{ id: 'networks', label: 'Networks', icon: Globe },
			{ id: 'accessors', label: 'Accessors', icon: Key },
			{ id: 'proposals', label: 'Proposals', icon: Vote },
			{ id: 'invitations', label: 'Invitations', icon: Mail },
			{ id: 'topology', label: 'Topology', icon: Activity }
		] as tab (tab.id)}
			<button
				class="flex items-center gap-2 border-b-2 px-4 py-2 text-sm font-medium transition-colors {activeTab === tab.id
					? 'border-blue-500 text-blue-400'
					: 'border-transparent text-gray-400 hover:text-gray-200'}"
				onclick={() => (activeTab = tab.id as Tab)}
			>
				<tab.icon class="h-4 w-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Networks Tab -->
	{#if activeTab === 'networks'}
		<div class="space-y-4">
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-2">
					<span class="text-sm text-gray-400">{networks.length} network{networks.length !== 1 ? 's' : ''}</span>
					<button
						class="rounded p-1 text-gray-400 hover:text-gray-200 disabled:opacity-50"
						onclick={loadNetworks}
						disabled={loadingNetworks}
					>
						<RefreshCw class="h-4 w-4 {loadingNetworks ? 'animate-spin' : ''}" />
					</button>
				</div>
				<button
					class="flex items-center gap-2 rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700"
					onclick={() => (showCreateNetwork = true)}
				>
					<Plus class="h-4 w-4" />
					Create Network
				</button>
			</div>

			<!-- Create Network Form -->
			{#if showCreateNetwork}
				<div class="rounded-lg border border-gray-700 bg-gray-800 p-4">
					<h3 class="mb-4 font-semibold">Create Network</h3>
					<div class="grid grid-cols-2 gap-4">
						<div>
							<label class="mb-1 block text-xs text-gray-400">Network Name *</label>
							<input
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newNetworkName}
								placeholder="my-network"
							/>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-400">Description</label>
							<input
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newNetworkDesc}
								placeholder="Optional description"
							/>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-400">Framework</label>
							<select
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newNetworkFramework}
							>
								<option value="HYPERLEDGER_FABRIC">Hyperledger Fabric</option>
								<option value="ETHEREUM">Ethereum</option>
							</select>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-400">Framework Version</label>
							<input
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newNetworkVersion}
								placeholder="2.2"
							/>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-400">Initial Member Name *</label>
							<input
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newMemberName}
								placeholder="member1"
							/>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-400">Initial Member Description</label>
							<input
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newMemberDesc}
								placeholder="Optional"
							/>
						</div>
					</div>
					<div class="mt-4 flex justify-end gap-2">
						<button
							class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200"
							onclick={() => (showCreateNetwork = false)}
						>
							Cancel
						</button>
						<button
							class="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
							onclick={createNetwork}
							disabled={creatingNetwork}
						>
							{creatingNetwork ? 'Creating…' : 'Create'}
						</button>
					</div>
				</div>
			{/if}

			<!-- Network list -->
			{#if loadingNetworks}
				<div class="text-center text-gray-400">Loading networks…</div>
			{:else if networks.length === 0}
				<div class="rounded-lg border border-dashed border-gray-700 p-8 text-center text-gray-400">
					No networks found. Create one to get started.
				</div>
			{:else}
				{#each networks as network (network.Id)}
					<div class="rounded-lg border border-gray-700 bg-gray-800">
						<!-- Network header -->
						<div class="flex items-center gap-3 p-4">
							<button
								class="text-gray-400 hover:text-gray-200"
								aria-label="Expand {network.Name}"
								aria-expanded={expandedNetworks.has(network.Id!)}
								onclick={() => toggleNetworkExpanded(network.Id!)}
							>
								{#if expandedNetworks.has(network.Id!)}
									<ChevronDown class="h-4 w-4" />
								{:else}
									<ChevronRight class="h-4 w-4" />
								{/if}
							</button>
							<Globe class="h-5 w-5 text-blue-400" />
							<div class="flex-1">
								<div class="flex items-center gap-2">
									<span class="font-medium">{network.Name}</span>
									<span class="text-xs {statusColor(network.Status)}">{network.Status}</span>
								</div>
								<div class="text-xs text-gray-500">{network.Id} · {network.Framework} {network.FrameworkVersion}</div>
							</div>
							<button
								class="flex items-center gap-1 rounded border border-gray-600 px-2 py-1 text-xs hover:bg-gray-700"
								onclick={() => {
									createMemberNetworkId = network.Id!;
									showCreateMember = true;
								}}
							>
								<Plus class="h-3 w-3" />
								Add Member
							</button>
						</div>

						<!-- Expanded members -->
						{#if expandedNetworks.has(network.Id!)}
							<div class="border-t border-gray-700 p-4 pt-3">
								{#if loadingMembers[network.Id!]}
									<div class="text-sm text-gray-400">Loading members…</div>
								{:else}
									{@const members = membersByNetwork[network.Id!] ?? []}
									{#if members.length === 0}
										<div class="text-sm text-gray-500">No members</div>
									{:else}
										{#each members as member (member.Id)}
											{@const memberKey = nodeKey(network.Id!, member.Id!)}
											<div class="mb-2 rounded border border-gray-600 bg-gray-900">
												<div class="flex items-center gap-2 p-3">
													<button
														class="text-gray-400 hover:text-gray-200"
														onclick={() => toggleMemberExpanded(network.Id!, member.Id!)}
													>
														{#if expandedMembers.has(memberKey)}
															<ChevronDown class="h-3.5 w-3.5" />
														{:else}
															<ChevronRight class="h-3.5 w-3.5" />
														{/if}
													</button>
													<Users class="h-4 w-4 text-purple-400" />
													<div class="flex-1">
														<div class="flex items-center gap-2">
															<span class="text-sm font-medium">{member.Name}</span>
															<span class="text-xs {statusColor(member.Status)}">{member.Status}</span>
														</div>
														<div class="text-xs text-gray-500">{member.Id}</div>
													</div>
													<div class="flex items-center gap-1">
														<button
															class="rounded p-1 text-gray-400 hover:text-blue-400"
															title="Edit member"
															onclick={() => {
																editMemberNetworkId = network.Id!;
																editMemberId = member.Id!;
																showEditMember = true;
															}}
														>
															<Edit2 class="h-3.5 w-3.5" />
														</button>
														<button
															class="flex items-center gap-1 rounded border border-gray-700 px-2 py-0.5 text-xs hover:bg-gray-800"
															onclick={() => {
																createNodeNetworkId = network.Id!;
																createNodeMemberId = member.Id!;
																showCreateNode = true;
															}}
														>
															<Plus class="h-3 w-3" />
															Node
														</button>
														<button
															class="rounded p-1 text-gray-400 hover:text-red-400"
															title="Delete member"
															onclick={() => deleteMember(network.Id!, member.Id!)}
														>
															<Trash2 class="h-3.5 w-3.5" />
														</button>
													</div>
												</div>

												<!-- Expanded nodes -->
												{#if expandedMembers.has(memberKey)}
													<div class="border-t border-gray-700 px-3 pb-3 pt-2">
														{#if loadingNodes[memberKey]}
															<div class="text-xs text-gray-400">Loading nodes…</div>
														{:else}
															{@const nodes = nodesByMember[memberKey] ?? []}
															{#if nodes.length === 0}
																<div class="text-xs text-gray-500">No nodes</div>
															{:else}
																{#each nodes as node (node.Id)}
																	<div class="mb-1 flex items-center gap-2 rounded bg-gray-800 px-3 py-2">
																		<Server class="h-3.5 w-3.5 text-green-400" />
																		<div class="flex-1">
																			<div class="flex items-center gap-2">
																				<span class="text-xs font-medium">{node.InstanceType}</span>
																				<span class="text-xs {statusColor(node.Status)}">{node.Status}</span>
																			</div>
																			<div class="text-xs text-gray-500">{node.Id}</div>
																		</div>
																		<div class="flex items-center gap-1">
																			<button
																				class="rounded p-1 text-gray-400 hover:text-blue-400"
																				title="Update node"
																				onclick={() => patchNode(network.Id!, member.Id!, node.Id!)}
																			>
																				<Edit2 class="h-3 w-3" />
																			</button>
																			<button
																				class="rounded p-1 text-gray-400 hover:text-red-400"
																				title="Delete node"
																				onclick={() => deleteNode(network.Id!, member.Id!, node.Id!)}
																			>
																				<Trash2 class="h-3 w-3" />
																			</button>
																		</div>
																	</div>
																{/each}
															{/if}
														{/if}
													</div>
												{/if}
											</div>
										{/each}
									{/if}
								{/if}
							</div>
						{/if}
					</div>
				{/each}
			{/if}
		</div>

		<!-- Create member modal -->
		{#if showCreateMember}
			<div
				class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
				role="presentation"
				onclick={() => (showCreateMember = false)}
				onkeydown={(e) => e.key === 'Escape' && (showCreateMember = false)}
			>
				<div
					class="w-96 rounded-lg border border-gray-700 bg-gray-800 p-6"
					role="dialog"
					aria-modal="true"
					aria-labelledby="create-member-title"
					onclick={(e) => e.stopPropagation()}
					onkeydown={(e) => e.stopPropagation()}
				>
					<h3 id="create-member-title" class="mb-4 font-semibold">Add Member to Network</h3>
					<div class="space-y-3">
						<div>
							<label class="mb-1 block text-xs text-gray-400">Member Name *</label>
							<input
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newMemberFormName}
								placeholder="member-name"
							/>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-400">Description</label>
							<input
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newMemberFormDesc}
								placeholder="Optional"
							/>
						</div>
					</div>
					<div class="mt-4 flex justify-end gap-2">
						<button
							class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200"
							onclick={() => (showCreateMember = false)}
						>
							Cancel
						</button>
						<button
							class="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
							onclick={createMember}
							disabled={creatingMember}
						>
							{creatingMember ? 'Creating…' : 'Add Member'}
						</button>
					</div>
				</div>
			</div>
		{/if}

		<!-- Edit member modal -->
		{#if showEditMember}
			<div
				class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
				role="presentation"
				onclick={() => (showEditMember = false)}
				onkeydown={(e) => e.key === 'Escape' && (showEditMember = false)}
			>
				<div
					class="w-96 rounded-lg border border-gray-700 bg-gray-800 p-6"
					role="dialog"
					aria-modal="true"
					aria-labelledby="edit-member-title"
					onclick={(e) => e.stopPropagation()}
					onkeydown={(e) => e.stopPropagation()}
				>
					<h3 id="edit-member-title" class="mb-4 font-semibold">Update Member</h3>
					<div>
						<label class="mb-1 block text-xs text-gray-400">Member ID</label>
						<div class="text-sm text-gray-300">{editMemberId}</div>
					</div>
					<div class="mt-4 flex justify-end gap-2">
						<button
							class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200"
							onclick={() => (showEditMember = false)}
						>
							Cancel
						</button>
						<button
							class="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
							onclick={updateMember}
							disabled={editingMember}
						>
							{editingMember ? 'Saving…' : 'Update'}
						</button>
					</div>
				</div>
			</div>
		{/if}

		<!-- Create node modal -->
		{#if showCreateNode}
			<div
				class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
				role="presentation"
				onclick={() => (showCreateNode = false)}
				onkeydown={(e) => e.key === 'Escape' && (showCreateNode = false)}
			>
				<div
					class="w-96 rounded-lg border border-gray-700 bg-gray-800 p-6"
					role="dialog"
					aria-modal="true"
					aria-labelledby="create-node-title"
					onclick={(e) => e.stopPropagation()}
					onkeydown={(e) => e.stopPropagation()}
				>
					<h3 id="create-node-title" class="mb-4 font-semibold">Create Node</h3>
					<div class="space-y-3">
						<div>
							<label class="mb-1 block text-xs text-gray-400">Instance Type</label>
							<select
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newNodeInstanceType}
							>
								<option value="bc.t3.small">bc.t3.small</option>
								<option value="bc.t3.medium">bc.t3.medium</option>
								<option value="bc.m5.xlarge">bc.m5.xlarge</option>
								<option value="bc.m5.2xlarge">bc.m5.2xlarge</option>
							</select>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-400">Availability Zone</label>
							<input
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newNodeAZ}
								placeholder="us-east-1a"
							/>
						</div>
					</div>
					<div class="mt-4 flex justify-end gap-2">
						<button
							class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200"
							onclick={() => (showCreateNode = false)}
						>
							Cancel
						</button>
						<button
							class="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
							onclick={createNode}
							disabled={creatingNode}
						>
							{creatingNode ? 'Creating…' : 'Create Node'}
						</button>
					</div>
				</div>
			</div>
		{/if}
	{/if}

	<!-- Accessors Tab -->
	{#if activeTab === 'accessors'}
		<div class="space-y-4">
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-2">
					<span class="text-sm text-gray-400">{accessors.length} accessor{accessors.length !== 1 ? 's' : ''}</span>
					<button
						class="rounded p-1 text-gray-400 hover:text-gray-200 disabled:opacity-50"
						onclick={loadAccessors}
						disabled={loadingAccessors}
					>
						<RefreshCw class="h-4 w-4 {loadingAccessors ? 'animate-spin' : ''}" />
					</button>
				</div>
				<button
					class="flex items-center gap-2 rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700"
					onclick={() => (showCreateAccessor = true)}
				>
					<Plus class="h-4 w-4" />
					Create Accessor
				</button>
			</div>

			{#if showCreateAccessor}
				<div class="rounded-lg border border-gray-700 bg-gray-800 p-4">
					<h3 class="mb-4 font-semibold">Create Accessor</h3>
					<div class="grid grid-cols-2 gap-4">
						<div>
							<label class="mb-1 block text-xs text-gray-400">Accessor Type</label>
							<select
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newAccessorType}
							>
								<option value="BILLING_TOKEN">BILLING_TOKEN</option>
							</select>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-400">Network Type</label>
							<select
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newAccessorNetworkType}
							>
								<option value={AccessorNetworkType.ETHEREUM_MAINNET}>Ethereum Mainnet</option>
								<option value={AccessorNetworkType.ETHEREUM_GOERLI}>Ethereum Goerli</option>
								<option value={AccessorNetworkType.POLYGON_MAINNET}>Polygon Mainnet</option>
								<option value={AccessorNetworkType.POLYGON_MUMBAI}>Polygon Mumbai</option>
							</select>
						</div>
					</div>
					<div class="mt-4 flex justify-end gap-2">
						<button
							class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200"
							onclick={() => (showCreateAccessor = false)}
						>
							Cancel
						</button>
						<button
							class="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
							onclick={createAccessor}
							disabled={creatingAccessor}
						>
							{creatingAccessor ? 'Creating…' : 'Create'}
						</button>
					</div>
				</div>
			{/if}

			{#if loadingAccessors}
				<div class="text-center text-gray-400">Loading accessors…</div>
			{:else if accessors.length === 0}
				<div class="rounded-lg border border-dashed border-gray-700 p-8 text-center text-gray-400">
					No accessors found.
				</div>
			{:else}
				<div class="overflow-hidden rounded-lg border border-gray-700">
					<table class="w-full text-sm">
						<thead class="border-b border-gray-700 bg-gray-800 text-xs text-gray-400">
							<tr>
								<th class="px-4 py-2 text-left">ID</th>
								<th class="px-4 py-2 text-left">Type</th>
								<th class="px-4 py-2 text-left">Network Type</th>
								<th class="px-4 py-2 text-left">Status</th>
								<th class="px-4 py-2 text-left">Created</th>
								<th class="px-4 py-2"></th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-700">
							{#each accessors as accessor (accessor.Id)}
								<tr class="bg-gray-900 hover:bg-gray-800">
									<td class="px-4 py-2 font-mono text-xs">{accessor.Id}</td>
									<td class="px-4 py-2">{accessor.Type}</td>
									<td class="px-4 py-2">{accessor.NetworkType ?? '—'}</td>
									<td class="px-4 py-2 {statusColor(accessor.Status)}">{accessor.Status}</td>
									<td class="px-4 py-2 text-gray-400">{accessor.CreationDate ? new Date(accessor.CreationDate).toLocaleDateString() : '—'}</td>
									<td class="px-4 py-2 text-right">
										<button
											class="rounded p-1 text-gray-400 hover:text-red-400"
											title="Delete accessor"
											onclick={() => deleteAccessor(accessor.Id!)}
										>
											<Trash2 class="h-4 w-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Proposals Tab -->
	{#if activeTab === 'proposals'}
		<div class="space-y-4">
			<div class="flex items-center gap-4">
				<div class="flex flex-1 items-center gap-2">
					<label class="text-sm text-gray-400">Network ID:</label>
					<input
						class="flex-1 rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
						bind:value={proposalNetworkId}
						placeholder="Enter network ID"
						onkeydown={(e) => e.key === 'Enter' && loadProposals()}
					/>
					<button
						class="flex items-center gap-2 rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
						onclick={loadProposals}
						disabled={!proposalNetworkId || loadingProposals}
					>
						<RefreshCw class="h-4 w-4 {loadingProposals ? 'animate-spin' : ''}" />
						Load
					</button>
				</div>
				{#if proposalNetworkId}
					<button
						class="flex items-center gap-2 rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700"
						onclick={() => (showCreateProposal = true)}
					>
						<Plus class="h-4 w-4" />
						Create Proposal
					</button>
				{/if}
			</div>

			{#if showCreateProposal}
				<div class="rounded-lg border border-gray-700 bg-gray-800 p-4">
					<h3 class="mb-4 font-semibold">Create Proposal</h3>
					<div class="grid grid-cols-2 gap-4">
						<div>
							<label class="mb-1 block text-xs text-gray-400">Proposing Member ID *</label>
							<input
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newProposalMemberId}
								placeholder="member-id"
							/>
						</div>
						<div>
							<label class="mb-1 block text-xs text-gray-400">Description</label>
							<input
								class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
								bind:value={newProposalDesc}
								placeholder="Optional description"
							/>
						</div>
					</div>
					<div class="mt-4 flex justify-end gap-2">
						<button
							class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200"
							onclick={() => (showCreateProposal = false)}
						>
							Cancel
						</button>
						<button
							class="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
							onclick={createProposal}
							disabled={creatingProposal}
						>
							{creatingProposal ? 'Creating…' : 'Create'}
						</button>
					</div>
				</div>
			{/if}

			<div class="grid grid-cols-3 gap-4">
				<!-- Proposal list -->
				<div class="col-span-1 space-y-2">
					{#if proposals.length === 0 && proposalNetworkId && !loadingProposals}
						<div class="rounded-lg border border-dashed border-gray-700 p-4 text-center text-sm text-gray-400">
							No proposals
						</div>
					{:else}
						{#each proposals as proposal (proposal.ProposalId)}
							{@const proposalId = proposal.ProposalId ?? ''}
							<button
								class="w-full rounded-lg border p-3 text-left transition-colors {selectedProposal?.ProposalId === proposalId
									? 'border-blue-500 bg-gray-800'
									: 'border-gray-700 bg-gray-900 hover:border-gray-600'}"
								onclick={() => selectProposal(proposal)}
							>
								<div class="flex items-center justify-between">
									<span class="text-sm font-medium truncate">{proposalId.slice(0, 12)}…</span>
									<span class="text-xs {proposalStatusColor(proposal.Status ?? '')}">
										{proposal.Status}
									</span>
								</div>
								<div class="mt-1 text-xs text-gray-400">{proposal.Description || '(no description)'}</div>
								<div class="mt-1 text-xs text-gray-400">
									by {proposal.ProposedByMemberName || proposal.ProposedByMemberId || '—'}
								</div>
							</button>
						{/each}
					{/if}
				</div>

				<!-- Proposal detail -->
				<div class="col-span-2">
					{#if selectedProposal}
						<div class="rounded-lg border border-gray-700 bg-gray-800 p-4">
							<div class="mb-4 flex items-center justify-between">
								<h3 class="font-semibold">Proposal Detail</h3>
								<button
									class="flex items-center gap-1 rounded border border-blue-600 px-2 py-1 text-xs text-blue-400 hover:bg-blue-600/20"
									onclick={() => {
										voteProposalId = selectedProposal?.ProposalId ?? '';
										showVoteForm = true;
									}}
								>
									<Vote class="h-3.5 w-3.5" />
									Vote
								</button>
							</div>
							<dl class="space-y-2 text-sm">
								<div class="flex gap-2">
									<dt class="w-36 text-gray-400">ID</dt>
									<dd class="font-mono text-xs">{selectedProposal.ProposalId}</dd>
								</div>
								<div class="flex gap-2">
									<dt class="w-36 text-gray-400">Status</dt>
									<dd class="{proposalStatusColor(selectedProposal.Status ?? '')}">{selectedProposal.Status}</dd>
								</div>
								<div class="flex gap-2">
									<dt class="w-36 text-gray-400">Description</dt>
									<dd>{selectedProposal.Description || '—'}</dd>
								</div>
								<div class="flex gap-2">
									<dt class="w-36 text-gray-400">Proposed By</dt>
									<dd>{selectedProposal.ProposedByMemberName || '—'}</dd>
								</div>
								<div class="flex gap-4">
									<span class="text-green-500">YES: {selectedProposal.YesVoteCount ?? 0}</span>
									<span class="text-red-500">NO: {selectedProposal.NoVoteCount ?? 0}</span>
									<span class="text-gray-400">Outstanding: {selectedProposal.OutstandingVoteCount ?? 0}</span>
								</div>
							</dl>

							<!-- Votes -->
							<div class="mt-4">
								<h4 class="mb-2 text-sm font-medium text-gray-300">Votes</h4>
								{#if loadingVotes}
									<div class="text-sm text-gray-400">Loading votes…</div>
								{:else if proposalVotes.length === 0}
									<div class="text-sm text-gray-500">No votes cast yet</div>
								{:else}
									{#each proposalVotes as v (v.MemberId)}
										<div class="flex items-center gap-3 py-1">
											{#if v.Vote === 'YES'}
												<CheckCircle class="h-4 w-4 text-green-500" />
											{:else}
												<XCircle class="h-4 w-4 text-red-500" />
											{/if}
											<span class="text-sm">{v.MemberName || v.MemberId}</span>
											<span class="text-xs {v.Vote === 'YES' ? 'text-green-500' : 'text-red-500'}">{v.Vote}</span>
										</div>
									{/each}
								{/if}
							</div>
						</div>
					{:else}
						<div class="flex h-full items-center justify-center rounded-lg border border-dashed border-gray-700 p-8 text-gray-500">
							Select a proposal to view details
						</div>
					{/if}
				</div>
			</div>

			<!-- Vote modal -->
			{#if showVoteForm}
				<div
					class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
					role="presentation"
					onclick={() => (showVoteForm = false)}
					onkeydown={(e) => e.key === 'Escape' && (showVoteForm = false)}
				>
					<div
						class="w-80 rounded-lg border border-gray-700 bg-gray-800 p-6"
						role="dialog"
						aria-modal="true"
						aria-labelledby="cast-vote-title"
						onclick={(e) => e.stopPropagation()}
						onkeydown={(e) => e.stopPropagation()}
					>
						<h3 id="cast-vote-title" class="mb-4 font-semibold">Cast Vote</h3>
						<div class="space-y-3">
							<div>
								<label class="mb-1 block text-xs text-gray-400">Voter Member ID *</label>
								<input
									class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm"
									bind:value={voteMemberId}
									placeholder="member-id"
								/>
							</div>
							<div>
								<label class="mb-1 block text-xs text-gray-400">Vote</label>
								<div class="flex gap-3">
									<label for="vote-yes" class="flex items-center gap-2 text-sm">
										<input id="vote-yes" type="radio" bind:group={voteChoice} value="YES" />
										<span class="text-green-400">YES</span>
									</label>
									<label for="vote-no" class="flex items-center gap-2 text-sm">
										<input id="vote-no" type="radio" bind:group={voteChoice} value="NO" />
										<span class="text-red-400">NO</span>
									</label>
								</div>
							</div>
						</div>
						<div class="mt-4 flex justify-end gap-2">
							<button
								class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200"
								onclick={() => (showVoteForm = false)}
							>
								Cancel
							</button>
							<button
								class="rounded bg-blue-600 px-3 py-1.5 text-sm font-medium hover:bg-blue-700 disabled:opacity-50"
								onclick={castVote}
								disabled={castingVote}
							>
								{castingVote ? 'Casting…' : 'Submit Vote'}
							</button>
						</div>
					</div>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Invitations Tab -->
	{#if activeTab === 'invitations'}
		<div class="space-y-4">
			<div class="flex items-center gap-2">
				<span class="text-sm text-gray-400">{invitations.length} invitation{invitations.length !== 1 ? 's' : ''}</span>
				<button
					class="rounded p-1 text-gray-400 hover:text-gray-200 disabled:opacity-50"
					onclick={loadInvitations}
					disabled={loadingInvitations}
				>
					<RefreshCw class="h-4 w-4 {loadingInvitations ? 'animate-spin' : ''}" />
				</button>
			</div>

			{#if loadingInvitations}
				<div class="text-center text-gray-400">Loading invitations…</div>
			{:else if invitations.length === 0}
				<div class="rounded-lg border border-dashed border-gray-700 p-8 text-center text-gray-400">
					No pending invitations.
				</div>
			{:else}
				<div class="overflow-hidden rounded-lg border border-gray-700">
					<table class="w-full text-sm">
						<thead class="border-b border-gray-700 bg-gray-800 text-xs text-gray-400">
							<tr>
								<th class="px-4 py-2 text-left">ID</th>
								<th class="px-4 py-2 text-left">Network</th>
								<th class="px-4 py-2 text-left">Status</th>
								<th class="px-4 py-2 text-left">Created</th>
								<th class="px-4 py-2 text-left">Expires</th>
								<th class="px-4 py-2"></th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-700">
							{#each invitations as inv (inv.InvitationId)}
								<tr class="bg-gray-900 hover:bg-gray-800">
									<td class="px-4 py-2 font-mono text-xs">{inv.InvitationId}</td>
									<td class="px-4 py-2">{(inv as { NetworkSummary?: { Name?: string } }).NetworkSummary?.Name ?? inv.InvitationId}</td>
									<td class="px-4 py-2 {statusColor(inv.Status)}">{inv.Status}</td>
									<td class="px-4 py-2 text-gray-400">{inv.CreationDate ? new Date(inv.CreationDate).toLocaleDateString() : '—'}</td>
									<td class="px-4 py-2 text-gray-400">{inv.ExpirationDate ? new Date(inv.ExpirationDate).toLocaleDateString() : '—'}</td>
									<td class="px-4 py-2 text-right">
										{#if inv.Status === 'PENDING'}
											<button
												class="rounded px-2 py-1 text-xs text-red-400 hover:bg-red-900/20"
												onclick={() => rejectInvitation(inv.InvitationId!)}
											>
												Reject
											</button>
										{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Topology Tab -->
	{#if activeTab === 'topology'}
		<div class="space-y-4">
			<div class="flex items-center gap-2">
				<h2 class="text-lg font-semibold">Network Topology</h2>
				<button
					class="rounded p-1 text-gray-400 hover:text-gray-200 disabled:opacity-50"
					onclick={loadTopology}
					disabled={loadingTopo}
				>
					<RefreshCw class="h-4 w-4 {loadingTopo ? 'animate-spin' : ''}" />
				</button>
			</div>

			{#if loadingTopo}
				<div class="text-center text-gray-400">Building topology…</div>
			{:else if topoNetworks.length === 0}
				<div class="rounded-lg border border-dashed border-gray-700 p-8 text-center text-gray-400">
					No networks to visualize. Create a network first.
				</div>
			{:else}
				<div class="rounded-lg border border-gray-700 bg-gray-800 p-6">
					<div class="flex flex-wrap gap-8">
						{#each topoNetworks as net (net.id)}
							<div class="flex flex-col items-center gap-3">
								<!-- Network node -->
								<div class="flex flex-col items-center gap-1">
									<div class="flex h-16 w-16 items-center justify-center rounded-full border-2 border-blue-500 bg-blue-500/10">
										<Globe class="h-7 w-7 text-blue-400" />
									</div>
									<div class="max-w-24 text-center text-xs font-medium">{net.name}</div>
									<div class="text-xs {statusColor(net.status)}">{net.status}</div>
								</div>

								{#if net.memberCount > 0}
									<!-- Connector line -->
									<div class="h-6 w-0.5 bg-gray-600"></div>

									<!-- Members indicator -->
									<div class="flex flex-col items-center gap-1">
										<div class="flex h-12 w-12 items-center justify-center rounded-full border-2 border-purple-500 bg-purple-500/10">
											<Users class="h-5 w-5 text-purple-400" />
										</div>
										<div class="text-xs text-purple-300">{net.memberCount} member{net.memberCount !== 1 ? 's' : ''}</div>
									</div>
								{:else}
									<div class="text-xs text-gray-500">No members</div>
								{/if}
							</div>
						{/each}
					</div>

					<!-- Legend -->
					<div class="mt-6 flex items-center gap-6 border-t border-gray-700 pt-4 text-xs text-gray-400">
						<div class="flex items-center gap-2">
							<div class="h-4 w-4 rounded-full border-2 border-blue-500 bg-blue-500/10"></div>
							<span>Network</span>
						</div>
						<div class="flex items-center gap-2">
							<div class="h-4 w-4 rounded-full border-2 border-purple-500 bg-purple-500/10"></div>
							<span>Members</span>
						</div>
						<div class="flex items-center gap-2">
							<div class="h-4 w-4 rounded-full border-2 border-green-500 bg-green-500/10"></div>
							<span>Nodes (expand in Networks tab)</span>
						</div>
					</div>
				</div>
			{/if}
		</div>
	{/if}
</div>
