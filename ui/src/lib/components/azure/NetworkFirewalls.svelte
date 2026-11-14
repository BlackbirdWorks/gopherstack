<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import {
		listFirewallPolicies,
		listFirewalls,
		listIpGroups,
		listRuleCollectionGroups,
		type Firewall,
		type FirewallPolicy,
		type IpGroup,
		type RuleCollectionGroup
	} from '#lib/azurenetwork.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let { subscription, group }: { subscription: string; group: string } = $props();

	let firewalls = $state<Firewall[]>([]);
	let policies = $state<FirewallPolicy[]>([]);
	let ipGroups = $state<IpGroup[]>([]);
	let groups = $state<RuleCollectionGroup[]>([]);
	let selected = $state('');
	let loading = $state(true);
	let error = $state('');
	let detailError = $state('');

	async function load() {
		loading = true;
		try {
			[firewalls, policies, ipGroups] = await Promise.all([listFirewalls(subscription, group), listFirewallPolicies(subscription, group), listIpGroups(subscription, group)]);
			error = '';
			if (!policies.some((p) => p.name === selected)) selected = policies[0]?.name ?? '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await loadGroups();
	}

	async function loadGroups() {
		groups = [];
		detailError = '';
		if (!selected) return;
		try {
			groups = await listRuleCollectionGroups(subscription, group, selected);
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	const cell = 'px-4 py-3';
	const head = 'text-sm font-semibold text-gray-700 dark:text-gray-300';

	$effect(() => {
		void group;
		void load();
	});
</script>

<div class="space-y-6" data-testid="net-firewalls">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="net-firewalls-error">Could not load firewalls: {error}</p>{/if}
	{#if loading && firewalls.length + policies.length + ipGroups.length === 0}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<section class="space-y-2">
			<h3 class={head}>Azure Firewalls</h3>
			{#each firewalls as f (f.id)}
				<div class="rounded-lg border border-gray-200 p-3 text-xs dark:border-gray-700" data-testid="net-firewall">
					<p class="text-sm font-semibold">{f.name} <span class="font-normal text-gray-500">{f.sku} / {f.tier} · {f.location}</span></p>
					<p data-testid="net-firewall-threat">Threat intelligence: {f.threatIntel || '—'}</p>
					<p data-testid="net-firewall-policy">Firewall policy: {f.policy || 'none (classic rules)'}</p>
					{#each f.ipConfigs as c (c.name)}<p data-testid="net-firewall-ipconfig">IP configuration {c.name}: subnet {c.subnet || '—'}, public IP {c.publicIp || '—'}, private {c.privateIp || '—'}</p>{/each}
				</div>
			{:else}
				<p class="text-sm text-gray-500" data-testid="net-firewalls-empty">No firewalls in this resource group.</p>
			{/each}
		</section>

		<section class="space-y-2">
			<h3 class={head}>Firewall policies</h3>
			{#if policies.length === 0}
				<p class="text-sm text-gray-500" data-testid="net-policies-empty">No firewall policies in this resource group.</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="net-policy-table">
						<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Policy</th><th class={cell}>Tier</th><th class={cell}>Threat intel</th><th class={cell}>Base policy</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each policies as p (p.id)}
								<tr class={selected === p.name ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="net-policy">
									<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => { selected = p.name; void loadGroups(); }} aria-expanded={selected === p.name} data-testid="net-policy-select">{p.name}</button></td>
									<td class="{cell} text-xs">{p.tier || '—'}</td>
									<td class="{cell} text-xs">{p.threatIntel || '—'}</td>
									<td class="{cell} text-xs">{p.basePolicy || '—'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
				{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="net-policy-error">{detailError}</p>{/if}
				{#each groups as g (g.name)}
					<div class="space-y-1 rounded-lg border border-gray-200 p-3 text-xs dark:border-gray-700" data-testid="net-rcg">
						<p class="text-sm font-semibold">{g.name} <span class="font-normal text-gray-500">priority {g.priority}</span></p>
						{#each g.collections as c (c.name)}
							<div class="ml-3" data-testid="net-collection">
								<p class="font-medium">{c.priority} · {c.name} <span class="text-gray-500">{c.type} · {c.action || '—'}</span></p>
								{#each c.rules as r (r.name)}<p class="ml-3 font-mono" data-testid="net-policy-rule">{r.name} [{r.type}] {r.summary}</p>{/each}
							</div>
						{:else}
							<p class="ml-3 text-gray-500">No rule collections.</p>
						{/each}
					</div>
				{:else}
					{#if !detailError && selected}<p class="text-sm text-gray-500" data-testid="net-rcg-empty">This policy has no rule collection groups.</p>{/if}
				{/each}
			{/if}
		</section>

		<section class="space-y-2">
			<h3 class={head}>IP groups</h3>
			{#each ipGroups as g (g.id)}
				<p class="text-xs" data-testid="net-ipgroup"><span class="font-mono font-semibold">{g.name}</span>: {g.addresses.join(', ') || 'no addresses'}{g.firewalls.length ? ` · used by ${g.firewalls.join(', ')}` : ''}</p>
			{:else}
				<p class="text-sm text-gray-500" data-testid="net-ipgroups-empty">No IP groups in this resource group.</p>
			{/each}
		</section>
	{/if}
</div>
