<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getELBv2Client } from '$lib/aws-client';
	import {
		DescribeLoadBalancersCommand,
		DescribeTargetGroupsCommand,
		DescribeListenersCommand,
		DescribeTargetHealthCommand,
		CreateLoadBalancerCommand,
		DeleteLoadBalancerCommand,
		type LoadBalancer,
		type TargetGroup,
		type Listener
	} from '@aws-sdk/client-elastic-load-balancing-v2';
	import { toast } from 'svelte-sonner';
	import {
		Network,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		CheckCircle,
		XCircle,
		ChevronRight,
		Server
	} from 'lucide-svelte';

	const elb = getELBv2Client();

	let loading = $state(false);
	let activeTab = $state<'lbs' | 'targets' | 'listeners'>('lbs');
	let searchQuery = $state('');

	// Load Balancers
	let loadBalancers = $state<LoadBalancer[]>([]);
	let selectedLB = $state<LoadBalancer | null>(null);
	let lbListeners = $state<Listener[]>([]);
	let loadingListeners = $state(false);
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newLBName = $state('');
	let newLBType = $state<'application' | 'network' | 'gateway'>('application');
	let newLBScheme = $state<'internet-facing' | 'internal'>('internet-facing');
	let newLBSubnets = $state('');

	// Target Groups
	let targetGroups = $state<TargetGroup[]>([]);
	let selectedTG = $state<TargetGroup | null>(null);
	let tgHealth = $state<Array<{ Target?: { Id?: string; Port?: number }; TargetHealth?: { State?: string; Description?: string } }>>([]);
	let loadingHealth = $state(false);

	const filteredLBs = $derived(
		loadBalancers.filter(
			(lb) =>
				(lb.LoadBalancerName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(lb.DNSName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredTGs = $derived(
		targetGroups.filter(
			(tg) =>
				(tg.TargetGroupName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(tg.Protocol ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function lbStateBadge(state?: string) {
		if (state === 'active') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (state === 'provisioning') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		if (state === 'failed') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		return 'text-muted-foreground bg-muted';
	}

	function healthBadge(state?: string) {
		if (state === 'healthy') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (state === 'unhealthy') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadLoadBalancers() {
		loading = true;
		try {
			const res = await elb.send(new DescribeLoadBalancersCommand({ PageSize: 100 }));
			loadBalancers = res.LoadBalancers ?? [];
		} catch (e) {
			toast.error(`Failed to load load balancers: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadTargetGroups() {
		loading = true;
		try {
			const res = await elb.send(new DescribeTargetGroupsCommand({ PageSize: 100 }));
			targetGroups = res.TargetGroups ?? [];
		} catch (e) {
			toast.error(`Failed to load target groups: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewLBListeners(lb: LoadBalancer) {
		selectedLB = lb;
		loadingListeners = true;
		lbListeners = [];
		try {
			const res = await elb.send(
				new DescribeListenersCommand({ LoadBalancerArn: lb.LoadBalancerArn })
			);
			lbListeners = res.Listeners ?? [];
		} catch (e) {
			toast.error(`Failed to load listeners: ${e}`);
		} finally {
			loadingListeners = false;
		}
	}

	async function viewTGHealth(tg: TargetGroup) {
		selectedTG = tg;
		loadingHealth = true;
		tgHealth = [];
		try {
			const res = await elb.send(
				new DescribeTargetHealthCommand({ TargetGroupArn: tg.TargetGroupArn })
			);
			tgHealth = (res.TargetHealthDescriptions ?? []).map((d) => ({
				Target: d.Target
					? { Id: d.Target.Id, Port: d.Target.Port }
					: undefined,
				TargetHealth: d.TargetHealth
					? { State: d.TargetHealth.State, Description: d.TargetHealth.Description }
					: undefined
			}));
		} catch (e) {
			toast.error(`Failed to load target health: ${e}`);
		} finally {
			loadingHealth = false;
		}
	}

	async function deleteLB(lb: LoadBalancer) {
		if (!lb.LoadBalancerArn || !await confirmDestructive(`Delete load balancer "${lb.LoadBalancerName}"?`)) return;
		try {
			await elb.send(new DeleteLoadBalancerCommand({ LoadBalancerArn: lb.LoadBalancerArn }));
			toast.success(`Deleting "${lb.LoadBalancerName}"…`);
			await loadLoadBalancers();
		} catch (e) {
			toast.error(`Failed to delete: ${e}`);
		}
	}

	async function createLB() {
		if (!newLBName.trim() || !newLBSubnets.trim()) {
			toast.error('Name and subnets are required');
			return;
		}
		creating = true;
		try {
			await elb.send(
				new CreateLoadBalancerCommand({
					Name: newLBName.trim(),
					Type: newLBType,
					Scheme: newLBScheme,
					Subnets: newLBSubnets.split(',').map((s) => s.trim())
				})
			);
			toast.success(`Load balancer "${newLBName}" created`);
			showCreateModal = false;
			newLBName = '';
			newLBSubnets = '';
			await loadLoadBalancers();
		} catch (e) {
			toast.error(`Failed to create load balancer: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		selectedLB = null;
		selectedTG = null;
		if (tab === 'lbs') await loadLoadBalancers();
		else if (tab === 'targets') await loadTargetGroups();
	}

	onMount(() => loadLoadBalancers());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Network class="h-8 w-8 text-teal-600" />
			<div>
				<h1 class="text-2xl font-bold">Elastic Load Balancing</h1>
				<p class="text-sm text-muted-foreground">
					Application, Network, and Gateway Load Balancers
				</p>
			</div>
		</div>
		<button
			onclick={() => (activeTab === 'lbs' ? loadLoadBalancers() : loadTargetGroups())}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'lbs', label: 'Load Balancers' }, { id: 'targets', label: 'Target Groups' }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Load Balancers Tab -->
	{#if activeTab === 'lbs'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search load balancers..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredLBs.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Network class="h-12 w-12 mb-3 opacity-30" />
				<p>No load balancers found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-left font-medium">Scheme</th>
							<th class="px-4 py-3 text-left font-medium">State</th>
							<th class="px-4 py-3 text-left font-medium">DNS Name</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredLBs as lb}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => viewLBListeners(lb)}
							>
								<td class="px-4 py-3 font-medium">{lb.LoadBalancerName}</td>
								<td class="px-4 py-3 text-muted-foreground capitalize">{lb.Type ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground capitalize">{lb.Scheme ?? '—'}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {lbStateBadge(lb.State?.Code)}">
										{lb.State?.Code ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-xs text-muted-foreground truncate max-w-[200px]">
									{lb.DNSName ?? '—'}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={(e) => { e.stopPropagation(); deleteLB(lb); }}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete"
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

		<!-- LB Listeners Drawer -->
		{#if selectedLB}
			<div class="rounded-lg border p-4 space-y-3">
				<div class="flex items-center justify-between">
					<h3 class="font-semibold">{selectedLB.LoadBalancerName} — Listeners</h3>
					<button onclick={() => (selectedLB = null)} class="text-xs text-muted-foreground hover:text-foreground">
						Close
					</button>
				</div>
				{#if loadingListeners}
					<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
				{:else if lbListeners.length === 0}
					<p class="text-sm text-muted-foreground">No listeners configured.</p>
				{:else}
					<div class="divide-y rounded border overflow-hidden">
						{#each lbListeners as listener}
							<div class="px-4 py-3 text-sm flex items-center justify-between">
								<div>
									<span class="font-medium">{listener.Protocol}:{listener.Port}</span>
									<span class="ml-2 text-xs text-muted-foreground">
										{listener.DefaultActions?.map((a) => a.Type).join(', ') ?? '—'}
									</span>
								</div>
								<ChevronRight class="h-4 w-4 text-muted-foreground" />
							</div>
						{/each}
					</div>
				{/if}
			</div>
		{/if}
	{/if}

	<!-- Target Groups Tab -->
	{#if activeTab === 'targets'}
		<div class="relative">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search target groups..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredTGs.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Server class="h-12 w-12 mb-3 opacity-30" />
				<p>No target groups found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Protocol</th>
							<th class="px-4 py-3 text-left font-medium">Port</th>
							<th class="px-4 py-3 text-left font-medium">Target Type</th>
							<th class="px-4 py-3 text-left font-medium">Health Check</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredTGs as tg}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => viewTGHealth(tg)}
							>
								<td class="px-4 py-3 font-medium">{tg.TargetGroupName}</td>
								<td class="px-4 py-3 text-muted-foreground">{tg.Protocol ?? '—'}</td>
								<td class="px-4 py-3">{tg.Port ?? '—'}</td>
								<td class="px-4 py-3 capitalize">{tg.TargetType ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground">{tg.HealthCheckPath ?? '/'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- Target Health -->
			{#if selectedTG}
				<div class="rounded-lg border p-4 space-y-3">
					<div class="flex items-center justify-between">
						<h3 class="font-semibold">{selectedTG.TargetGroupName} — Target Health</h3>
						<button onclick={() => (selectedTG = null)} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>
					{#if loadingHealth}
						<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
					{:else if tgHealth.length === 0}
						<p class="text-sm text-muted-foreground">No registered targets.</p>
					{:else}
						<div class="divide-y rounded border overflow-hidden">
							{#each tgHealth as h}
								<div class="px-4 py-3 text-sm flex items-center justify-between">
									<span>{h.Target?.Id}:{h.Target?.Port}</span>
									<span class="rounded-full px-2 py-0.5 text-xs {healthBadge(h.TargetHealth?.State)}">
										{h.TargetHealth?.State ?? '—'}
									</span>
								</div>
							{/each}
						</div>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}
</div>

<!-- Create LB Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Load Balancer</h2>
			<div class="space-y-3">
				<div>
					<label for="lb-name" class="block text-sm font-medium mb-1">Name *</label>
					<input
						id="lb-name"
						type="text"
						bind:value={newLBName}
						placeholder="my-load-balancer"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="lb-type" class="block text-sm font-medium mb-1">Type</label>
					<select
						id="lb-type"
						bind:value={newLBType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="application">Application</option>
						<option value="network">Network</option>
						<option value="gateway">Gateway</option>
					</select>
				</div>
				<div>
					<label for="lb-scheme" class="block text-sm font-medium mb-1">Scheme</label>
					<select
						id="lb-scheme"
						bind:value={newLBScheme}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="internet-facing">Internet-facing</option>
						<option value="internal">Internal</option>
					</select>
				</div>
				<div>
					<label for="lb-subnets" class="block text-sm font-medium mb-1"
						>Subnets (comma-separated) *</label
					>
					<input
						id="lb-subnets"
						type="text"
						bind:value={newLBSubnets}
						placeholder="subnet-abc123, subnet-def456"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createLB}
					disabled={creating || !newLBName.trim() || !newLBSubnets.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}
