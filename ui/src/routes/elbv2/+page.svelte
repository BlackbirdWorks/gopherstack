<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getELBv2Client } from '$lib/aws-client';
	import {
		DescribeLoadBalancersCommand,
		DescribeTargetGroupsCommand,
		DescribeListenersCommand,
		DescribeTargetHealthCommand,
		DescribeRulesCommand,
		DescribeListenerCertificatesCommand,
		DescribeTrustStoresCommand,
		DescribeTrustStoreRevocationsCommand,
		CreateLoadBalancerCommand,
		DeleteLoadBalancerCommand,
		CreateRuleCommand,
		ModifyRuleCommand,
		DeleteRuleCommand,
		AddListenerCertificatesCommand,
		RemoveListenerCertificatesCommand,
		ModifyListenerCommand,
		type LoadBalancer,
		type TargetGroup,
		type Listener,
		type Rule,
		type Certificate,
		type RuleCondition
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
		ChevronDown,
		Server,
		Shield,
		List,
		Lock
	} from 'lucide-svelte';

	const elb = getELBv2Client();

	type Tab = 'lbs' | 'targets' | 'listeners' | 'truststores';

	let loading = $state(false);
	let activeTab = $state<Tab>('lbs');
	let searchQuery = $state('');

	// ── Load Balancers ──────────────────────────────────────────────────────────
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

	// ── Target Groups ───────────────────────────────────────────────────────────
	let targetGroups = $state<TargetGroup[]>([]);
	let selectedTG = $state<TargetGroup | null>(null);
	let tgHealth = $state<Array<{ Target?: { Id?: string; Port?: number }; TargetHealth?: { State?: string; Description?: string } }>>([]);
	let loadingHealth = $state(false);

	// ── Listeners ───────────────────────────────────────────────────────────────
	let allListeners = $state<Listener[]>([]);
	let loadingAllListeners = $state(false);
	let selectedListener = $state<Listener | null>(null);
	let listenerRules = $state<Rule[]>([]);
	let loadingRules = $state(false);
	let listenerCerts = $state<Certificate[]>([]);
	let loadingCerts = $state(false);

	// cert management
	let showAddCertModal = $state(false);
	let newCertArn = $state('');
	let addingCert = $state(false);

	// rule condition editor
	let showRuleEditor = $state(false);
	let editingRule = $state<Rule | null>(null);
	let ruleCondField = $state('host-header');
	let ruleCondValues = $state('');
	let ruleCondHeaderName = $state('');
	let ruleActionTGArn = $state('');
	let savingRule = $state(false);

	// ── Trust Stores ────────────────────────────────────────────────────────────
	let trustStores = $state<Array<{ TrustStoreArn?: string; Name?: string; Status?: string; TotalRevokedEntries?: number }>>([]);
	let loadingTrustStores = $state(false);
	let selectedTrustStore = $state<{ TrustStoreArn?: string; Name?: string; Status?: string } | null>(null);
	let trustStoreRevocations = $state<Array<{ RevocationId?: string; RevocationType?: string; LastUpdatedAt?: Date }>>([]);
	let loadingRevocations = $state(false);

	// ── derived ─────────────────────────────────────────────────────────────────
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

	const filteredListeners = $derived(
		allListeners.filter((l) =>
			(l.Protocol ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
			String(l.Port ?? '').includes(searchQuery)
		)
	);

	// ── helpers ─────────────────────────────────────────────────────────────────
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

	function conditionLabel(cond: RuleCondition): string {
		const field = cond.Field ?? '?';
		switch (field) {
			case 'host-header': {
				const vals = cond.HostHeaderConfig?.Values ?? [];
				return `Host: ${vals.join(', ')}`;
			}
			case 'path-pattern': {
				const vals = cond.PathPatternConfig?.Values ?? [];
				return `Path: ${vals.join(', ')}`;
			}
			case 'http-header': {
				const name = cond.HttpHeaderConfig?.HttpHeaderName ?? '';
				const vals = cond.HttpHeaderConfig?.Values ?? [];
				return `Header ${name}: ${vals.join(', ')}`;
			}
			case 'http-request-method': {
				const vals = cond.HttpRequestMethodConfig?.Values ?? [];
				return `Method: ${vals.join(', ')}`;
			}
			case 'query-string': {
				const pairs = (cond.QueryStringConfig?.Values ?? []).map(
					(p) => `${p.Key ?? '*'}=${p.Value}`
				);
				return `Query: ${pairs.join(' & ')}`;
			}
			case 'source-ip': {
				const vals = cond.SourceIpConfig?.Values ?? [];
				return `SourceIP: ${vals.join(', ')}`;
			}
			default:
				return field;
		}
	}

	function conditionBadgeColor(field?: string): string {
		switch (field) {
			case 'host-header': return 'bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200';
			case 'path-pattern': return 'bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200';
			case 'http-header': return 'bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200';
			case 'http-request-method': return 'bg-orange-100 text-orange-800 dark:bg-orange-900 dark:text-orange-200';
			case 'query-string': return 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900 dark:text-yellow-200';
			case 'source-ip': return 'bg-red-100 text-red-800 dark:bg-red-900 dark:text-red-200';
			default: return 'bg-muted text-muted-foreground';
		}
	}

	// ── data loaders ────────────────────────────────────────────────────────────
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

	async function loadAllListeners() {
		loadingAllListeners = true;
		allListeners = [];
		try {
			const res = await elb.send(new DescribeListenersCommand({}));
			allListeners = res.Listeners ?? [];
		} catch (e) {
			toast.error(`Failed to load listeners: ${e}`);
		} finally {
			loadingAllListeners = false;
		}
	}

	async function loadTrustStores() {
		loadingTrustStores = true;
		trustStores = [];
		try {
			const res = await elb.send(new DescribeTrustStoresCommand({}));
			trustStores = (res.TrustStores ?? []) as typeof trustStores;
		} catch (e) {
			toast.error(`Failed to load trust stores: ${e}`);
		} finally {
			loadingTrustStores = false;
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

	async function selectListener(listener: Listener) {
		selectedListener = listener;
		await Promise.all([loadListenerRules(listener), loadListenerCerts(listener)]);
	}

	async function loadListenerRules(listener: Listener) {
		loadingRules = true;
		listenerRules = [];
		try {
			const res = await elb.send(
				new DescribeRulesCommand({ ListenerArn: listener.ListenerArn })
			);
			listenerRules = res.Rules ?? [];
		} catch (e) {
			toast.error(`Failed to load rules: ${e}`);
		} finally {
			loadingRules = false;
		}
	}

	async function loadListenerCerts(listener: Listener) {
		loadingCerts = true;
		listenerCerts = [];
		try {
			const res = await elb.send(
				new DescribeListenerCertificatesCommand({ ListenerArn: listener.ListenerArn })
			);
			listenerCerts = res.Certificates ?? [];
		} catch (e) {
			toast.error(`Failed to load certificates: ${e}`);
		} finally {
			loadingCerts = false;
		}
	}

	async function addCert() {
		if (!selectedListener?.ListenerArn || !newCertArn.trim()) return;
		addingCert = true;
		try {
			await elb.send(
				new AddListenerCertificatesCommand({
					ListenerArn: selectedListener.ListenerArn,
					Certificates: [{ CertificateArn: newCertArn.trim() }]
				})
			);
			toast.success('Certificate added');
			newCertArn = '';
			showAddCertModal = false;
			await loadListenerCerts(selectedListener);
		} catch (e) {
			toast.error(`Failed to add certificate: ${e}`);
		} finally {
			addingCert = false;
		}
	}

	async function removeCert(certArn: string) {
		if (!selectedListener?.ListenerArn) return;
		if (!await confirmDestructive({ title: 'Remove Certificate', message: `Remove certificate ${certArn}?` })) return;
		try {
			await elb.send(
				new RemoveListenerCertificatesCommand({
					ListenerArn: selectedListener.ListenerArn,
					Certificates: [{ CertificateArn: certArn }]
				})
			);
			toast.success('Certificate removed');
			await loadListenerCerts(selectedListener);
		} catch (e) {
			toast.error(`Failed to remove certificate: ${e}`);
		}
	}

	function openCreateRule() {
		editingRule = null;
		ruleCondField = 'host-header';
		ruleCondValues = '';
		ruleCondHeaderName = '';
		ruleActionTGArn = '';
		showRuleEditor = true;
	}

	function openEditRule(rule: Rule) {
		editingRule = rule;
		const firstCond = rule.Conditions?.[0];
		ruleCondField = firstCond?.Field ?? 'host-header';
		const firstAction = rule.Actions?.[0];
		ruleActionTGArn = firstAction?.TargetGroupArn ?? '';

		if (ruleCondField === 'host-header') {
			ruleCondValues = (firstCond?.HostHeaderConfig?.Values ?? []).join(', ');
		} else if (ruleCondField === 'path-pattern') {
			ruleCondValues = (firstCond?.PathPatternConfig?.Values ?? []).join(', ');
		} else if (ruleCondField === 'http-header') {
			ruleCondHeaderName = firstCond?.HttpHeaderConfig?.HttpHeaderName ?? '';
			ruleCondValues = (firstCond?.HttpHeaderConfig?.Values ?? []).join(', ');
		} else if (ruleCondField === 'http-request-method') {
			ruleCondValues = (firstCond?.HttpRequestMethodConfig?.Values ?? []).join(', ');
		} else if (ruleCondField === 'source-ip') {
			ruleCondValues = (firstCond?.SourceIpConfig?.Values ?? []).join(', ');
		}
		showRuleEditor = true;
	}

	function buildConditionForSave(): RuleCondition {
		const values = ruleCondValues.split(',').map((v) => v.trim()).filter(Boolean);
		switch (ruleCondField) {
			case 'host-header':
				return { Field: 'host-header', HostHeaderConfig: { Values: values } };
			case 'path-pattern':
				return { Field: 'path-pattern', PathPatternConfig: { Values: values } };
			case 'http-header':
				return { Field: 'http-header', HttpHeaderConfig: { HttpHeaderName: ruleCondHeaderName, Values: values } };
			case 'http-request-method':
				return { Field: 'http-request-method', HttpRequestMethodConfig: { Values: values } };
			case 'source-ip':
				return { Field: 'source-ip', SourceIpConfig: { Values: values } };
			default:
				return { Field: ruleCondField };
		}
	}

	async function saveRule() {
		if (!selectedListener?.ListenerArn) return;
		savingRule = true;
		try {
			const condition = buildConditionForSave();
			const actions = ruleActionTGArn ? [{ Type: 'forward' as const, TargetGroupArn: ruleActionTGArn }] : [];

			if (editingRule?.RuleArn) {
				await elb.send(
					new ModifyRuleCommand({
						RuleArn: editingRule.RuleArn,
						Conditions: [condition],
						Actions: actions.length > 0 ? actions : undefined
					})
				);
				toast.success('Rule updated');
			} else {
				await elb.send(
					new CreateRuleCommand({
						ListenerArn: selectedListener.ListenerArn,
						Priority: Date.now() % 50000,
						Conditions: [condition],
						Actions: actions
					})
				);
				toast.success('Rule created');
			}
			showRuleEditor = false;
			await loadListenerRules(selectedListener);
		} catch (e) {
			toast.error(`Failed to save rule: ${e}`);
		} finally {
			savingRule = false;
		}
	}

	async function deleteRule(rule: Rule) {
		if (!rule.RuleArn || !await confirmDestructive({ title: 'Delete Rule', message: `Delete rule with priority ${rule.Priority}?` })) return;
		try {
			await elb.send(new DeleteRuleCommand({ RuleArn: rule.RuleArn }));
			toast.success('Rule deleted');
			if (selectedListener) await loadListenerRules(selectedListener);
		} catch (e) {
			toast.error(`Failed to delete rule: ${e}`);
		}
	}

	async function viewTrustStoreRevocations(ts: typeof trustStores[number]) {
		selectedTrustStore = ts;
		loadingRevocations = true;
		trustStoreRevocations = [];
		try {
			const res = await elb.send(
				new DescribeTrustStoreRevocationsCommand({ TrustStoreArn: ts.TrustStoreArn })
			);
			trustStoreRevocations = (res.TrustStoreRevocations ?? []) as typeof trustStoreRevocations;
		} catch (e) {
			toast.error(`Failed to load revocations: ${e}`);
		} finally {
			loadingRevocations = false;
		}
	}

	async function deleteLB(lb: LoadBalancer) {
		if (!lb.LoadBalancerArn || !await confirmDestructive({ title: 'Delete Load Balancer', message: `Delete load balancer "${lb.LoadBalancerName}"? All listeners and rules will be removed.` })) return;
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

	async function onTabChange(tab: Tab) {
		activeTab = tab;
		searchQuery = '';
		selectedLB = null;
		selectedTG = null;
		selectedListener = null;
		selectedTrustStore = null;
		if (tab === 'lbs') await loadLoadBalancers();
		else if (tab === 'targets') await loadTargetGroups();
		else if (tab === 'listeners') await loadAllListeners();
		else if (tab === 'truststores') await loadTrustStores();
	}

	onMount(() => loadLoadBalancers());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Network class="h-8 w-8 text-teal-600" />
			<div>
				<h1 class="text-2xl font-bold">Elastic Load Balancing v2</h1>
				<p class="text-sm text-muted-foreground">
					Application, Network, and Gateway Load Balancers
				</p>
			</div>
		</div>
		<button
			onclick={() => {
				if (activeTab === 'lbs') loadLoadBalancers();
				else if (activeTab === 'targets') loadTargetGroups();
				else if (activeTab === 'listeners') loadAllListeners();
				else if (activeTab === 'truststores') loadTrustStores();
			}}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [
			{ id: 'lbs', label: 'Load Balancers' },
			{ id: 'targets', label: 'Target Groups' },
			{ id: 'listeners', label: 'Listeners & Rules' },
			{ id: 'truststores', label: 'Trust Stores' }
		] as tab}
			<button
				onclick={() => onTabChange(tab.id as Tab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- ── Load Balancers Tab ─────────────────────────────────────────────────── -->
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

	<!-- ── Target Groups Tab ──────────────────────────────────────────────────── -->
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

	<!-- ── Listeners & Rules Tab ──────────────────────────────────────────────── -->
	{#if activeTab === 'listeners'}
		<div class="relative">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search by protocol or port..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>

		{#if loadingAllListeners}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredListeners.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<List class="h-12 w-12 mb-3 opacity-30" />
				<p>No listeners found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Protocol:Port</th>
							<th class="px-4 py-3 text-left font-medium">Default Action</th>
							<th class="px-4 py-3 text-left font-medium">Certificates</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredListeners as listener}
							<tr
								class="hover:bg-muted/30 cursor-pointer {selectedListener?.ListenerArn === listener.ListenerArn ? 'bg-muted/40' : ''}"
								onclick={() => selectListener(listener)}
							>
								<td class="px-4 py-3 font-medium">
									<span class="inline-flex items-center gap-1">
										{listener.Protocol}:{listener.Port}
										{#if listener.Protocol === 'HTTPS' || listener.Protocol === 'TLS'}
											<Lock class="h-3 w-3 text-green-500" />
										{/if}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground">
									{listener.DefaultActions?.map((a) => a.Type).join(', ') ?? '—'}
								</td>
								<td class="px-4 py-3 text-muted-foreground">
									{listener.Certificates?.length ?? 0} cert(s)
								</td>
								<td class="px-4 py-3 text-right">
									<ChevronDown class="h-4 w-4 text-muted-foreground inline" />
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- Listener Detail Panel -->
		{#if selectedListener}
			<div class="rounded-lg border p-4 space-y-4">
				<div class="flex items-center justify-between">
					<h3 class="font-semibold">{selectedListener.Protocol}:{selectedListener.Port} — Details</h3>
					<button onclick={() => (selectedListener = null)} class="text-xs text-muted-foreground hover:text-foreground">
						Close
					</button>
				</div>

				<!-- ARN info -->
				<p class="text-xs text-muted-foreground break-all">ARN: {selectedListener.ListenerArn}</p>

				<!-- Rules section -->
				<div class="space-y-2">
					<div class="flex items-center justify-between">
						<h4 class="text-sm font-medium">Rules</h4>
						<button
							onclick={openCreateRule}
							class="flex items-center gap-1 text-xs text-primary hover:underline"
						>
							<Plus class="h-3 w-3" /> Add Rule
						</button>
					</div>
					{#if loadingRules}
						<RefreshCw class="h-4 w-4 animate-spin text-muted-foreground" />
					{:else if listenerRules.length === 0}
						<p class="text-sm text-muted-foreground">No rules. Click "Add Rule" to create one.</p>
					{:else}
						<div class="space-y-2">
							{#each listenerRules as rule}
								<div class="rounded border p-3 text-sm space-y-2">
									<div class="flex items-center justify-between">
										<div class="flex items-center gap-2">
											<span class="font-mono text-xs bg-muted px-1.5 py-0.5 rounded">
												{rule.IsDefault ? 'default' : `priority ${rule.Priority}`}
											</span>
											{#if rule.IsDefault}
												<span class="text-xs text-muted-foreground">Default rule</span>
											{/if}
										</div>
										{#if !rule.IsDefault}
											<div class="flex gap-1">
												<button
													onclick={() => openEditRule(rule)}
													class="text-xs text-blue-600 hover:underline dark:text-blue-400"
												>
													Edit
												</button>
												<button
													onclick={() => deleteRule(rule)}
													class="text-xs text-red-500 hover:underline"
												>
													Delete
												</button>
											</div>
										{/if}
									</div>

									<!-- Conditions visualization -->
									{#if (rule.Conditions?.length ?? 0) > 0}
										<div>
											<span class="text-xs text-muted-foreground mr-2">Conditions:</span>
											<div class="inline-flex flex-wrap gap-1 mt-1">
												{#each rule.Conditions ?? [] as cond}
													<span class="rounded-full px-2 py-0.5 text-xs font-medium {conditionBadgeColor(cond.Field)}">
														{conditionLabel(cond)}
													</span>
												{/each}
											</div>
										</div>
									{/if}

									<!-- Actions -->
									<div>
										<span class="text-xs text-muted-foreground mr-2">Actions:</span>
										{#each rule.Actions ?? [] as action}
											<span class="text-xs">
												{action.Type}
												{#if action.TargetGroupArn}
													→ <span class="font-mono text-xs">{action.TargetGroupArn.split('/').pop()}</span>
												{/if}
											</span>
										{/each}
									</div>
								</div>
							{/each}
						</div>
					{/if}
				</div>

				<!-- Certificates section -->
				<div class="space-y-2 border-t pt-3">
					<div class="flex items-center justify-between">
						<h4 class="text-sm font-medium flex items-center gap-1">
							<Shield class="h-4 w-4" /> Certificates
						</h4>
						<button
							onclick={() => (showAddCertModal = true)}
							class="flex items-center gap-1 text-xs text-primary hover:underline"
						>
							<Plus class="h-3 w-3" /> Add
						</button>
					</div>
					{#if loadingCerts}
						<RefreshCw class="h-4 w-4 animate-spin text-muted-foreground" />
					{:else if listenerCerts.length === 0}
						<p class="text-sm text-muted-foreground">No additional certificates.</p>
					{:else}
						<div class="space-y-1">
							{#each listenerCerts as cert}
								<div class="flex items-center justify-between text-sm rounded border px-3 py-2">
									<div class="flex items-center gap-2">
										<Lock class="h-3 w-3 text-green-500" />
										<span class="font-mono text-xs truncate max-w-[400px]">{cert.CertificateArn}</span>
										{#if cert.IsDefault}
											<span class="text-xs bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300 px-1.5 py-0.5 rounded-full">default</span>
										{/if}
									</div>
									{#if !cert.IsDefault}
										<button
											onclick={() => removeCert(cert.CertificateArn ?? '')}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										>
											<Trash2 class="h-3 w-3" />
										</button>
									{/if}
								</div>
							{/each}
						</div>
					{/if}
				</div>
			</div>
		{/if}
	{/if}

	<!-- ── Trust Stores Tab ───────────────────────────────────────────────────── -->
	{#if activeTab === 'truststores'}
		{#if loadingTrustStores}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if trustStores.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Shield class="h-12 w-12 mb-3 opacity-30" />
				<p>No trust stores found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-left font-medium">Revoked Entries</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each trustStores as ts}
							<tr
								class="hover:bg-muted/30 cursor-pointer {selectedTrustStore?.TrustStoreArn === ts.TrustStoreArn ? 'bg-muted/40' : ''}"
								onclick={() => viewTrustStoreRevocations(ts)}
							>
								<td class="px-4 py-3 font-medium">{ts.Name ?? '—'}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {ts.Status === 'ACTIVE' ? 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900' : 'text-muted-foreground bg-muted'}">
										{ts.Status ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{ts.TotalRevokedEntries ?? 0}</td>
								<td class="px-4 py-3 text-right">
									<ChevronRight class="h-4 w-4 text-muted-foreground inline" />
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- Trust Store Revocations -->
			{#if selectedTrustStore}
				<div class="rounded-lg border p-4 space-y-3">
					<div class="flex items-center justify-between">
						<h3 class="font-semibold">{selectedTrustStore.Name} — Revocations</h3>
						<button onclick={() => (selectedTrustStore = null)} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>
					{#if loadingRevocations}
						<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
					{:else if trustStoreRevocations.length === 0}
						<p class="text-sm text-muted-foreground">No revocation entries.</p>
					{:else}
						<div class="divide-y rounded border overflow-hidden">
							{#each trustStoreRevocations as rev}
								<div class="px-4 py-3 text-sm flex items-center justify-between">
									<div>
										<span class="font-mono text-xs">{rev.RevocationId ?? '—'}</span>
										{#if rev.RevocationType}
											<span class="ml-2 text-xs text-muted-foreground">{rev.RevocationType}</span>
										{/if}
									</div>
									{#if rev.LastUpdatedAt}
										<span class="text-xs text-muted-foreground">
											{new Date(rev.LastUpdatedAt).toLocaleDateString()}
										</span>
									{/if}
								</div>
							{/each}
						</div>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}
</div>

<!-- ── Create LB Modal ──────────────────────────────────────────────────────── -->
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

<!-- ── Add Certificate Modal ─────────────────────────────────────────────────── -->
{#if showAddCertModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Add Certificate</h2>
			<div class="space-y-3">
				<div>
					<label for="cert-arn" class="block text-sm font-medium mb-1">Certificate ARN *</label>
					<input
						id="cert-arn"
						type="text"
						bind:value={newCertArn}
						placeholder="arn:aws:acm:us-east-1:123456789012:certificate/..."
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showAddCertModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={addCert}
					disabled={addingCert || !newCertArn.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{addingCert ? 'Adding...' : 'Add'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- ── Rule Condition Editor Modal ────────────────────────────────────────────── -->
{#if showRuleEditor}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-lg rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">
				{editingRule ? 'Edit Rule' : 'Create Rule'}
			</h2>
			<div class="space-y-4">
				<!-- Condition type -->
				<div>
					<label for="cond-field" class="block text-sm font-medium mb-1">Condition Type</label>
					<select
						id="cond-field"
						bind:value={ruleCondField}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="host-header">Host Header</option>
						<option value="path-pattern">Path Pattern</option>
						<option value="http-header">HTTP Header</option>
						<option value="http-request-method">HTTP Method</option>
						<option value="source-ip">Source IP</option>
						<option value="query-string">Query String</option>
					</select>
				</div>

				{#if ruleCondField === 'http-header'}
					<div>
						<label for="header-name" class="block text-sm font-medium mb-1">Header Name *</label>
						<input
							id="header-name"
							type="text"
							bind:value={ruleCondHeaderName}
							placeholder="X-Custom-Header"
							class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
				{/if}

				<div>
					<label for="cond-values" class="block text-sm font-medium mb-1">
						{#if ruleCondField === 'host-header'}Host names (comma-separated){/if}
						{#if ruleCondField === 'path-pattern'}Path patterns (comma-separated){/if}
						{#if ruleCondField === 'http-header'}Header values (comma-separated){/if}
						{#if ruleCondField === 'http-request-method'}Methods e.g. GET,POST{/if}
						{#if ruleCondField === 'source-ip'}CIDR blocks (comma-separated){/if}
						{#if ruleCondField === 'query-string'}Key=value pairs (comma-separated){/if}
					</label>
					<input
						id="cond-values"
						type="text"
						bind:value={ruleCondValues}
						placeholder={ruleCondField === 'host-header' ? 'example.com, *.example.com' :
							ruleCondField === 'path-pattern' ? '/api/*, /health' :
							ruleCondField === 'http-request-method' ? 'GET, POST' :
							ruleCondField === 'source-ip' ? '10.0.0.0/8, 192.168.0.0/16' :
							'value1, value2'}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>

				<!-- Action: forward to target group -->
				<div>
					<label for="action-tg" class="block text-sm font-medium mb-1">Forward to Target Group ARN</label>
					<input
						id="action-tg"
						type="text"
						bind:value={ruleActionTGArn}
						placeholder="arn:aws:elasticloadbalancing:..."
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					{#if targetGroups.length > 0}
						<div class="mt-1 flex flex-wrap gap-1">
							{#each targetGroups as tg}
								<button
									onclick={() => (ruleActionTGArn = tg.TargetGroupArn ?? '')}
									class="text-xs rounded border px-2 py-0.5 hover:bg-accent"
								>
									{tg.TargetGroupName}
								</button>
							{/each}
						</div>
					{/if}
				</div>
			</div>

			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showRuleEditor = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={saveRule}
					disabled={savingRule || !ruleCondValues.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{savingRule ? 'Saving...' : editingRule ? 'Update' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

