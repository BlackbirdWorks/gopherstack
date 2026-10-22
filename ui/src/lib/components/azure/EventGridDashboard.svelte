<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Zap, Send, Inbox } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import { attempt, GCP_INPUT, GCP_PRIMARY, GCP_GHOST } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listTopics, publishEvents, sampleEvent, validateEventJson, type EgTopic } from '#lib/azureeventgrid.ts';

	let topics = $state<EgTopic[]>([]);
	let topicName = $state('');
	let loading = $state(true);
	let loadError = $state('');
	let eventJson = $state('');
	let publishing = $state(false);

	const topic = $derived(topics.find((t) => t.name === topicName));
	const jsonError = $derived(validateEventJson(eventJson));

	function resetSample() {
		eventJson = JSON.stringify(sampleEvent(topic?.inputSchema ?? ''), null, 2);
	}

	async function load() {
		loading = true;
		loadError = '';
		try {
			topics = await listTopics();
			if (!topics.some((t) => t.name === topicName)) topicName = topics[0]?.name ?? '';
			resetSample();
		} catch (e) {
			loadError = describeGcpError(e);
			toast.error(`Failed to load Event Grid: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	function pick(name: string) {
		topicName = name;
		resetSample();
	}

	async function publish(e: Event) {
		e.preventDefault();
		if (jsonError || !topic) return;
		publishing = true;
		let n = 0;
		const ok = await attempt(async () => {
			n = await publishEvents(topic.name, eventJson);
		}, '', 'Failed to publish');
		publishing = false;
		if (ok) {
			toast.success(`Published ${n} event${n === 1 ? '' : 's'} to ${topic.name}`);
			resetSample();
		}
	}

	onMount(() => void load());
</script>

<div class="space-y-6 pb-20" data-testid="eventgrid-dashboard">
	<PageHeader icon={Zap} title="Event Grid" description="Azure Event Grid: topics, event subscriptions and publishing" color="purple" onRefresh={load} />

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="eventgrid-error">
			<p class="font-medium">Could not reach Azure Event Grid</p>
			<p>{loadError}</p>
		</div>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else if topics.length === 0}
		<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="eventgrid-empty">
			<Inbox class="h-10 w-10 mb-3 opacity-30" />
			<p>No custom topics. Create one with the Azure CLI, Terraform or ARM.</p>
		</div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="eventgrid-topics">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Topic</th><th class="px-4 py-2">Input schema</th><th class="px-4 py-2">Subscriptions</th></tr></thead>
				<tbody>
					{#each topics as t (t.name)}
						<tr class="border-t border-gray-100 dark:border-gray-800 {t.name === topicName ? 'bg-blue-50 dark:bg-slate-800' : ''}">
							<td class="px-4 py-2"><button class="font-medium text-blue-600 underline dark:text-blue-400" onclick={() => pick(t.name)}>{t.name}</button></td>
							<td class="px-4 py-2">{t.inputSchema}</td>
							<td class="px-4 py-2">{t.subscriptions.length}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if topic}
			<section>
				<h2 class="mb-2 font-semibold">Event subscriptions of {topic.name}</h2>
				{#if topic.subscriptions.length === 0}
					<p class="text-gray-500" data-testid="eventgrid-no-subs">No event subscriptions.</p>
				{:else}
					<table class="w-full text-sm" data-testid="eventgrid-subs">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Name</th><th class="px-4 py-2">Endpoint</th><th class="px-4 py-2">State</th><th class="px-4 py-2">Delivery schema</th></tr></thead>
						<tbody>
							{#each topic.subscriptions as s (s.name)}
								<tr class="border-t border-gray-100 dark:border-gray-800"><td class="px-4 py-2">{s.name}</td><td class="px-4 py-2">{s.endpointType}</td><td class="px-4 py-2">{s.state}</td><td class="px-4 py-2">{s.deliverySchema || '-'}</td></tr>
							{/each}
						</tbody>
					</table>
				{/if}
			</section>

			<form class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" onsubmit={publish} data-testid="eventgrid-publish-form">
				<h2 class="font-semibold">Publish event to {topic.name}</h2>
				<textarea class="{GCP_INPUT} font-mono" rows="10" bind:value={eventJson} aria-label="Event JSON" data-testid="eventgrid-json"></textarea>
				{#if jsonError}<p class="text-sm text-red-600" data-testid="eventgrid-json-error">{jsonError}</p>{/if}
				<div class="flex gap-2">
					<button type="submit" class={GCP_PRIMARY} disabled={publishing || Boolean(jsonError)} data-testid="eventgrid-publish"><Send class="mr-1 inline h-4 w-4" />Publish</button>
					<button type="button" class={GCP_GHOST} onclick={resetSample}>Reset sample</button>
				</div>
			</form>
		{/if}
	{/if}
</div>
