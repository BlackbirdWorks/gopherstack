<script lang="ts">
	import { onMount } from 'svelte';
	import { Bell, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { shortOcid } from '#lib/ocirest.ts';
	import {
		confirmationQuery,
		confirmSubscription,
		isHttpUrl,
		listCompartments,
		listDeliveries,
		listSubscriptions,
		listTopics,
		readStoredNotifCompartment,
		resendConfirmation,
		writeStoredNotifCompartment,
		type Delivery,
		type Subscription,
		type Topic
	} from '#lib/ocinotifications.ts';

	const TABS = [
		{ id: 'topics', label: 'Topics' },
		{ id: 'subscriptions', label: 'Subscriptions' },
		{ id: 'deliveries', label: 'Deliveries' }
	];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredNotifCompartment());
	let tab = $state('topics');
	let topics = $state<Topic[]>([]);
	let subs = $state<Subscription[]>([]);
	let deliveries = $state<Delivery[]>([]);
	let loading = $state(true);
	let error = $state('');
	let expanded = $state('');

	const topicName = (id: string) => topics.find((t) => t.id === id)?.name ?? shortOcid(id);
	const subsOf = (t: Topic) => subs.filter((s) => s.topicId === t.id);
	const visibleDeliveries = $derived(deliveries.filter((d) => topics.some((t) => t.id === d.topicId)));

	async function loadScope() {
		if (!compartment) {
			topics = [];
			subs = [];
			deliveries = [];
			return;
		}
		try {
			[topics, subs, deliveries] = await Promise.all([listTopics(compartment), listSubscriptions(compartment), listDeliveries()]);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function load() {
		try {
			compartments = await listCompartments();
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await loadScope();
		loading = false;
	}

	function pickCompartment() {
		writeStoredNotifCompartment(compartment);
		expanded = '';
		loading = true;
		void loadScope().then(() => (loading = false));
	}

	async function confirm(d: Delivery) {
		const ok = await attempt(() => confirmSubscription(d.subscriptionId, confirmationQuery(d.confirmationUrl)), 'Subscription confirmed', 'Failed to confirm the subscription');
		if (ok) await loadScope();
	}

	async function resend(s: Subscription) {
		const ok = await attempt(() => resendConfirmation(s.id), `Confirmation resent to ${s.endpoint}`, 'Failed to resend the confirmation');
		if (ok) await loadScope();
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' || s === 'DELIVERED' || s === 'RECORDED' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'DISCARDED' || s === 'DELETED' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';

	onMount(load);
</script>

{#snippet subsTable(items: Subscription[], testId: string)}
	<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
		<table class="w-full text-sm" data-testid={testId}>
			<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Topic</th><th class={cell}>Protocol</th><th class={cell}>Endpoint</th><th class={cell}>State</th><th class="{cell} text-right">Actions</th></tr></thead>
			<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
				{#each items as s (s.id)}
					<tr data-testid="{testId}-row">
						<td class="{cell} font-mono text-xs">{topicName(s.topicId)}</td>
						<td class="{cell} text-xs" data-testid="ocinotif-protocol">{s.protocol}</td>
						<td class="{cell} font-mono text-xs break-all" data-testid="ocinotif-endpoint">{s.endpoint}</td>
						<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(s.state)}" data-testid="ocinotif-sub-state">{s.state}</span></td>
						<td class="{cell} text-right">
							{#if s.state === 'PENDING'}<button class={GCP_MINI} onclick={() => resend(s)} data-testid="ocinotif-resend">Resend confirmation</button>{/if}
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/snippet}

<div class="space-y-6 pb-20" data-testid="ocinotif-dashboard">
	<PageHeader icon={Bell} title="Notifications" description="Oracle Cloud topics, subscriptions and recorded deliveries" color="red" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="ocinotif-error">
			<p class="font-medium">Could not load Notifications</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if compartments.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="ocinotif-empty">No topics yet. Create one with the OCI SDK or CLI.</p>
	{:else}
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Compartment
			<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="ocinotif-compartment">
				{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
			</select>
		</label>
		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="ocinotif-tab-error">{error}</p>{/if}

		{#if tab === 'topics'}
			{#if topics.length === 0}
				<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="ocinotif-topics-empty">No topics in this compartment</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="ocinotif-topics">
						<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Topic</th><th class={cell}>Description</th><th class={cell}>State</th><th class={cell}>Subscriptions</th><th class="{cell} text-right">Actions</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each topics as t (t.id)}
								<tr data-testid="ocinotif-topic">
									<td class="{cell} font-mono text-xs" title={t.id}>{t.name}</td>
									<td class="{cell} text-xs">{t.description || '—'}</td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(t.state)}">{t.state}</span></td>
									<td class="{cell} text-xs" data-testid="ocinotif-topic-subs">{subsOf(t).length}</td>
									<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => (expanded = expanded === t.id ? '' : t.id)} aria-expanded={expanded === t.id} data-testid="ocinotif-topic-details">Subscriptions</button></td>
								</tr>
								{#if expanded === t.id}
									<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="ocinotif-topic-detail">
										<td colspan="5" class="px-4 py-3">
											{#if subsOf(t).length === 0}
												<p class="text-xs text-gray-500">No subscriptions</p>
											{:else}
												{@render subsTable(subsOf(t), 'ocinotif-topic-subscriptions')}
											{/if}
										</td>
									</tr>
								{/if}
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if tab === 'subscriptions'}
			{#if subs.length === 0}
				<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="ocinotif-subs-empty">No subscriptions in this compartment</p>
			{:else}
				{@render subsTable(subs, 'ocinotif-subscriptions')}
			{/if}
		{:else if visibleDeliveries.length === 0}
			<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="ocinotif-deliveries-empty">No deliveries recorded</p>
		{:else}
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="ocinotif-deliveries">
					<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Published</th><th class={cell}>Kind</th><th class={cell}>Topic</th><th class={cell}>Endpoint</th><th class={cell}>Status</th><th class={cell}>Message</th><th class="{cell} text-right">Links</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each visibleDeliveries as d (d.id)}
							<tr data-testid="ocinotif-delivery">
								<td class="{cell} text-xs">{d.published}</td>
								<td class="{cell} text-xs" data-testid="ocinotif-delivery-kind">{d.kind}</td>
								<td class="{cell} font-mono text-xs">{d.topicName}</td>
								<td class="{cell} font-mono text-xs break-all">{d.protocol}: {d.endpoint}</td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(d.status)}" title={d.lastError}>{d.status}</span></td>
								<td class="{cell} text-xs"><span class="font-medium">{d.title}</span> <span class="break-all text-gray-500">{d.body}</span></td>
								<td class="{cell} text-right whitespace-nowrap space-x-1">
									{#if d.confirmationUrl && isHttpUrl(d.confirmationUrl)}
										<a class="{GCP_MINI} inline-block" href={d.confirmationUrl} target="_blank" rel="noopener noreferrer" data-testid="ocinotif-confirm-link">Confirmation link</a>
										<button class={GCP_MINI} onclick={() => confirm(d)} data-testid="ocinotif-confirm">Confirm</button>
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
