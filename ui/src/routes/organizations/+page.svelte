<script lang="ts">
	import { onMount } from 'svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { getOrganizationsClient } from '$lib/aws-client';
	import {
		DescribeOrganizationCommand,
		ListAccountsCommand,
		ListOrganizationalUnitsForParentCommand,
		ListRootsCommand,
		ListPoliciesCommand,
		CreateAccountCommand,
		MoveAccountCommand,
		AttachPolicyCommand,
		DetachPolicyCommand,
		ListParentsCommand,
		ListPoliciesForTargetCommand,
		CloseAccountCommand,
		type OrganizationsClient,
		type Organization,
		type Account,
		type OrganizationalUnit,
		type Root,
		type PolicySummary
	} from '@aws-sdk/client-organizations';
	import { toast } from 'svelte-sonner';
	import {
		Building2,
		Search,
		RefreshCw,
		Plus,
		Users,
		FolderTree,
		Shield,
		CheckCircle,
		XCircle,
		Clock,
		MoveRight,
		Link2,
		Unlink,
		Ban
	} from 'lucide-svelte';

	let org: OrganizationsClient | undefined;

	function client(): OrganizationsClient {
		return (org ??= getOrganizationsClient());
	}

	let loading = $state(false);
	let activeTab = $state<'overview' | 'accounts' | 'ous' | 'policies'>('overview');
	let searchQuery = $state('');

	// Organization overview
	let organization = $state<Organization | null>(null);
	let roots = $state<Root[]>([]);

	// Accounts
	let accounts = $state<Account[]>([]);
	let showCreateAccountModal = $state(false);
	let creatingAccount = $state(false);
	let newAccountName = $state('');
	let newAccountEmail = $state('');
	let newAccountRoleName = $state('OrganizationAccountAccessRole');

	// OUs
	let ous = $state<OrganizationalUnit[]>([]);

	// Policies
	let policies = $state<PolicySummary[]>([]);
	let policyType = $state<'SERVICE_CONTROL_POLICY' | 'TAG_POLICY'>('SERVICE_CONTROL_POLICY');

	const filteredAccounts = $derived(
		accounts.filter(
			(a) =>
				(a.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(a.Email ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(a.Id ?? '').includes(searchQuery)
		)
	);

	const filteredPolicies = $derived(
		policies.filter((p) => (p.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	function accountStatusBadge(status?: string) {
		if (status === 'ACTIVE') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (status === 'SUSPENDED') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadOverview() {
		loading = true;
		try {
			const [orgRes, rootsRes] = await Promise.all([
				client().send(new DescribeOrganizationCommand({})),
				client().send(new ListRootsCommand({}))
			]);
			organization = orgRes.Organization ?? null;
			roots = rootsRes.Roots ?? [];
		} catch (e) {
			toast.error(`Failed to load organization: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadAccounts() {
		loading = true;
		try {
			const res = await client().send(new ListAccountsCommand({ MaxResults: 100 }));
			accounts = res.Accounts ?? [];
		} catch (e) {
			toast.error(`Failed to load accounts: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadOUs() {
		loading = true;
		try {
			// First get roots, then OUs for root
			const rootsRes = await client().send(new ListRootsCommand({}));
			const root = rootsRes.Roots?.[0];
			if (root?.Id) {
				const ouRes = await client().send(
					new ListOrganizationalUnitsForParentCommand({ ParentId: root.Id, MaxResults: 100 })
				);
				ous = ouRes.OrganizationalUnits ?? [];
			}
		} catch (e) {
			toast.error(`Failed to load OUs: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadPolicies() {
		loading = true;
		try {
			const res = await client().send(
				new ListPoliciesCommand({ Filter: policyType, MaxResults: 100 })
			);
			policies = res.Policies ?? [];
		} catch (e) {
			toast.error(`Failed to load policies: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function createAccount() {
		if (!newAccountName.trim() || !newAccountEmail.trim()) return;
		creatingAccount = true;
		try {
			await client().send(
				new CreateAccountCommand({
					AccountName: newAccountName.trim(),
					Email: newAccountEmail.trim(),
					RoleName: newAccountRoleName.trim()
				})
			);
			toast.success(`Account creation initiated for "${newAccountName}"`);
			showCreateAccountModal = false;
			newAccountName = '';
			newAccountEmail = '';
			await loadAccounts();
		} catch (e) {
			toast.error(`Failed to create account: ${e}`);
		} finally {
			creatingAccount = false;
		}
	}

	// Move account
	let showMoveModal = $state(false);
	let moveAccount = $state<Account | null>(null);
	let moveParents = $state<{ Id?: string; Type?: string }[]>([]);
	let moveSourceParentId = $state('');
	let moveDestParentId = $state('');
	let moving = $state(false);
	let moveTargets = $state<{ Id: string; label: string }[]>([]);

	// Close account
	let closingId = $state<string | null>(null);

	async function openMoveModal(account: Account) {
		moveAccount = account;
		moveDestParentId = '';
		showMoveModal = true;
		moving = true;
		try {
			const parentsRes = await client().send(
				new ListParentsCommand({ ChildId: account.Id })
			);
			moveParents = parentsRes.Parents ?? [];
			moveSourceParentId = moveParents[0]?.Id ?? '';
			// Build the list of move targets: roots + their OUs.
			const rootsRes = await client().send(new ListRootsCommand({}));
			const targets: { Id: string; label: string }[] = [];
			for (const root of rootsRes.Roots ?? []) {
				if (root.Id) {
					targets.push({ Id: root.Id, label: `Root: ${root.Name ?? root.Id}` });
					const ouRes = await client().send(
						new ListOrganizationalUnitsForParentCommand({ ParentId: root.Id, MaxResults: 100 })
					);
					for (const ou of ouRes.OrganizationalUnits ?? []) {
						if (ou.Id) targets.push({ Id: ou.Id, label: `OU: ${ou.Name ?? ou.Id}` });
					}
				}
			}
			moveTargets = targets;
		} catch (e) {
			toast.error(`Failed to load parents: ${e}`);
		} finally {
			moving = false;
		}
	}

	async function doMoveAccount() {
		if (!moveAccount?.Id || !moveSourceParentId || !moveDestParentId) return;
		if (moveSourceParentId === moveDestParentId) {
			toast.error('Destination must differ from current parent');
			return;
		}
		moving = true;
		try {
			await client().send(
				new MoveAccountCommand({
					AccountId: moveAccount.Id,
					SourceParentId: moveSourceParentId,
					DestinationParentId: moveDestParentId
				})
			);
			toast.success(`Moved account ${moveAccount.Id}`);
			showMoveModal = false;
			await loadAccounts();
		} catch (e) {
			toast.error(`Failed to move account: ${e}`);
		} finally {
			moving = false;
		}
	}

	async function closeAccount(account: Account) {
		if (!account.Id) return;
		if (!(await confirmDestructive({ title: 'Close Account', message: `Close account ${account.Name ?? account.Id}? This cannot be undone.`, confirmLabel: 'Close Account' }))) return;
		closingId = account.Id;
		try {
			await client().send(new CloseAccountCommand({ AccountId: account.Id }));
			toast.success(`Close requested for ${account.Id}`);
			await loadAccounts();
		} catch (e) {
			toast.error(`Failed to close account: ${e}`);
		} finally {
			closingId = null;
		}
	}

	// Policy attach/detach. A target's currently-attached policies can be
	// inspected via ListPoliciesForTarget once a target ID is entered.
	let showAttachModal = $state(false);
	let attachPolicy = $state<PolicySummary | null>(null);
	let attachTargetId = $state('');
	let attaching = $state(false);
	let targetPolicies = $state<PolicySummary[]>([]);

	function openAttachModal(policy: PolicySummary) {
		attachPolicy = policy;
		attachTargetId = '';
		targetPolicies = [];
		showAttachModal = true;
	}

	async function loadTargetPolicies() {
		if (!attachTargetId.trim()) {
			targetPolicies = [];
			return;
		}
		attaching = true;
		try {
			const res = await client().send(
				new ListPoliciesForTargetCommand({ TargetId: attachTargetId.trim(), Filter: policyType })
			);
			targetPolicies = res.Policies ?? [];
		} catch (e) {
			toast.error(`Failed to load target policies: ${e}`);
		} finally {
			attaching = false;
		}
	}

	async function doAttachPolicy(detach = false) {
		if (!attachPolicy?.Id || !attachTargetId.trim()) return;
		attaching = true;
		try {
			if (detach) {
				await client().send(
					new DetachPolicyCommand({ PolicyId: attachPolicy.Id, TargetId: attachTargetId.trim() })
				);
				toast.success(`Detached policy from ${attachTargetId}`);
			} else {
				await client().send(
					new AttachPolicyCommand({ PolicyId: attachPolicy.Id, TargetId: attachTargetId.trim() })
				);
				toast.success(`Attached policy to ${attachTargetId}`);
			}
			await loadTargetPolicies();
		} catch (e) {
			toast.error(`Failed to ${detach ? 'detach' : 'attach'} policy: ${e}`);
		} finally {
			attaching = false;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'overview') await loadOverview();
		else if (tab === 'accounts') await loadAccounts();
		else if (tab === 'ous') await loadOUs();
		else await loadPolicies();
	}

	onMount(() => loadOverview());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Building2 class="h-8 w-8 text-indigo-600" />
			<div>
				<h1 class="text-2xl font-bold">AWS Organizations</h1>
				<p class="text-sm text-muted-foreground">Manage multiple AWS accounts centrally</p>
			</div>
		</div>
		<button
			onclick={() => onTabChange(activeTab)}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'overview', label: 'Overview', icon: Building2 }, { id: 'accounts', label: 'Accounts', icon: Users }, { id: 'ous', label: 'Org Units', icon: FolderTree }, { id: 'policies', label: 'Policies', icon: Shield }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				<tab.icon class="h-4 w-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Overview Tab -->
	{#if activeTab === 'overview'}
		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if organization}
			<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
				<div class="rounded-lg border p-4 space-y-2">
					<p class="text-sm font-medium text-muted-foreground">Organization ID</p>
					<p class="font-mono text-sm">{organization.Id}</p>
				</div>
				<div class="rounded-lg border p-4 space-y-2">
					<p class="text-sm font-medium text-muted-foreground">Master Account</p>
					<p class="font-mono text-sm">{organization.MasterAccountId}</p>
					<p class="text-xs text-muted-foreground">{organization.MasterAccountEmail}</p>
				</div>
				<div class="rounded-lg border p-4 space-y-2">
					<p class="text-sm font-medium text-muted-foreground">Feature Set</p>
					<p>{organization.FeatureSet ?? '—'}</p>
				</div>
			</div>

			{#if roots.length > 0}
				<div class="rounded-lg border p-4">
					<h3 class="font-semibold mb-3">Organization Roots</h3>
					<div class="divide-y rounded border overflow-hidden">
						{#each roots as root}
							<div class="px-4 py-3 text-sm">
								<div class="font-medium">{root.Name}</div>
								<div class="text-xs text-muted-foreground font-mono">{root.Id}</div>
								<div class="mt-1 flex gap-1">
									{#each root.PolicyTypes ?? [] as pt}
										<span class="rounded bg-muted px-2 py-0.5 text-xs">
											{pt.Type} ({pt.Status})
										</span>
									{/each}
								</div>
							</div>
						{/each}
					</div>
				</div>
			{/if}
		{:else}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Building2 class="h-12 w-12 mb-3 opacity-30" />
				<p>Organization information unavailable</p>
			</div>
		{/if}
	{/if}

	<!-- Accounts Tab -->
	{#if activeTab === 'accounts'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search accounts..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateAccountModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Account
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredAccounts.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Users class="h-12 w-12 mb-3 opacity-30" />
				<p>No accounts found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Account Name</th>
							<th class="px-4 py-3 text-left font-medium">Account ID</th>
							<th class="px-4 py-3 text-left font-medium">Email</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-left font-medium">Joined</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredAccounts as account}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{account.Name}</td>
								<td class="px-4 py-3 font-mono text-xs">{account.Id}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">{account.Email}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {accountStatusBadge(account.Status)}">
										{account.Status ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{account.JoinedTimestamp ? new Date(account.JoinedTimestamp).toLocaleDateString() : '—'}
								</td>
								<td class="px-4 py-3 text-right whitespace-nowrap">
									<button
										onclick={() => openMoveModal(account)}
										class="inline-flex items-center gap-1 rounded border px-2 py-1 text-xs hover:bg-accent"
										title="Move account to another root or OU"
									>
										<MoveRight class="h-3.5 w-3.5" />
										Move
									</button>
									<button
										onclick={() => closeAccount(account)}
										disabled={closingId === account.Id || account.Status !== 'ACTIVE'}
										class="ml-1 inline-flex items-center gap-1 rounded border px-2 py-1 text-xs text-red-600 hover:bg-red-50 disabled:opacity-40 dark:hover:bg-red-950"
										title="Close account"
									>
										<Ban class="h-3.5 w-3.5" />
										{closingId === account.Id ? 'Closing…' : 'Close'}
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- OUs Tab -->
	{#if activeTab === 'ous'}
		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if ous.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<FolderTree class="h-12 w-12 mb-3 opacity-30" />
				<p>No organizational units found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">ID</th>
							<th class="px-4 py-3 text-left font-medium">ARN</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each ous as ou}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{ou.Name}</td>
								<td class="px-4 py-3 font-mono text-xs">{ou.Id}</td>
								<td class="px-4 py-3 text-xs text-muted-foreground truncate max-w-[300px]">{ou.Arn}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Policies Tab -->
	{#if activeTab === 'policies'}
		<div class="flex items-center gap-3 mb-4">
			<select
				bind:value={policyType}
				onchange={loadPolicies}
				class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			>
				<option value="SERVICE_CONTROL_POLICY">Service Control Policies</option>
				<option value="TAG_POLICY">Tag Policies</option>
			</select>
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search policies..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredPolicies.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Shield class="h-12 w-12 mb-3 opacity-30" />
				<p>No policies found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Description</th>
							<th class="px-4 py-3 text-left font-medium">AWS Managed</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredPolicies as policy}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{policy.Name}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs truncate max-w-[300px]">
									{policy.Description ?? '—'}
								</td>
								<td class="px-4 py-3">
									{#if policy.AwsManaged}
										<CheckCircle class="h-4 w-4 text-blue-500" />
									{:else}
										<XCircle class="h-4 w-4 text-muted-foreground" />
									{/if}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => openAttachModal(policy)}
										class="inline-flex items-center gap-1 rounded border px-2 py-1 text-xs hover:bg-accent"
										title="Attach or detach this policy"
									>
										<Link2 class="h-3.5 w-3.5" />
										Attach / Detach
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Account Modal -->
{#if showCreateAccountModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Member Account</h2>
			<p class="text-sm text-muted-foreground mb-3">
				Creating an account may take a few minutes. The account will appear in the list once ready.
			</p>
			<div class="space-y-3">
				<div>
					<label for="acct-name" class="block text-sm font-medium mb-1">Account Name *</label>
					<input
						id="acct-name"
						type="text"
						bind:value={newAccountName}
						placeholder="My Production Account"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="acct-email" class="block text-sm font-medium mb-1">Root Email *</label>
					<input
						id="acct-email"
						type="email"
						bind:value={newAccountEmail}
						placeholder="prod@example.com"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="acct-role" class="block text-sm font-medium mb-1">IAM Role Name</label>
					<input
						id="acct-role"
						type="text"
						bind:value={newAccountRoleName}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateAccountModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createAccount}
					disabled={creatingAccount || !newAccountName.trim() || !newAccountEmail.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingAccount ? 'Creating...' : 'Create Account'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Move Account Modal -->
{#if showMoveModal && moveAccount}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-1">Move Account</h2>
			<p class="text-sm text-muted-foreground mb-4">
				{moveAccount.Name ?? moveAccount.Id} ({moveAccount.Id})
			</p>
			<div class="space-y-3">
				<div>
					<label for="move-src" class="block text-sm font-medium mb-1">Current Parent</label>
					<select
						id="move-src"
						bind:value={moveSourceParentId}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						{#each moveParents as p}
							<option value={p.Id}>{p.Type}: {p.Id}</option>
						{/each}
					</select>
				</div>
				<div>
					<label for="move-dest" class="block text-sm font-medium mb-1">Destination (Root / OU)</label>
					<select
						id="move-dest"
						bind:value={moveDestParentId}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="" disabled>Select destination…</option>
						{#each moveTargets as t}
							<option value={t.Id}>{t.label} ({t.Id})</option>
						{/each}
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showMoveModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={doMoveAccount}
					disabled={moving || !moveSourceParentId || !moveDestParentId}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{moving ? 'Moving…' : 'Move'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Attach / Detach Policy Modal -->
{#if showAttachModal && attachPolicy}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-1">Attach / Detach Policy</h2>
			<p class="text-sm text-muted-foreground mb-4">{attachPolicy.Name} ({attachPolicy.Id})</p>
			<div class="space-y-3">
				<div>
					<label for="attach-target" class="block text-sm font-medium mb-1">
						Target ID (root / OU / account)
					</label>
					<div class="flex gap-2">
						<input
							id="attach-target"
							type="text"
							bind:value={attachTargetId}
							placeholder="r-xxxx / ou-xxxx / 123456789012"
							class="flex-1 rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
						<button
							onclick={loadTargetPolicies}
							disabled={attaching || !attachTargetId.trim()}
							class="rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
						>
							Inspect
						</button>
					</div>
				</div>
				{#if targetPolicies.length > 0}
					<div class="rounded border p-2 text-xs">
						<p class="font-medium mb-1">Policies currently on target:</p>
						<ul class="list-disc pl-4 space-y-0.5">
							{#each targetPolicies as tp}
								<li class={tp.Id === attachPolicy.Id ? 'font-semibold text-primary' : ''}>
									{tp.Name} ({tp.Id})
								</li>
							{/each}
						</ul>
					</div>
				{/if}
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showAttachModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Close
				</button>
				<button
					onclick={() => doAttachPolicy(true)}
					disabled={attaching || !attachTargetId.trim()}
					class="inline-flex items-center gap-1 rounded-md border px-4 py-2 text-sm text-red-600 hover:bg-red-50 disabled:opacity-50 dark:hover:bg-red-950"
				>
					<Unlink class="h-4 w-4" />
					Detach
				</button>
				<button
					onclick={() => doAttachPolicy(false)}
					disabled={attaching || !attachTargetId.trim()}
					class="inline-flex items-center gap-1 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					<Link2 class="h-4 w-4" />
					Attach
				</button>
			</div>
		</div>
	</div>
{/if}
