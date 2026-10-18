<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Radio, Plus, Trash2, RefreshCw, Send, Download, Camera, X } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import KeyValueEditor from '#lib/components/gcp/KeyValueEditor.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { describeGcpError, durationToSeconds } from '#lib/gcprest.ts';
	import { diffMap, metadataToRows, rowsToMap, validateLabelKey, validateMetadataKey, validateRows, type MetaRow } from '#lib/gcpstorage.ts';
	import {
		acknowledge,
		createSnapshot,
		createSubscription,
		createTopic,
		deleteSnapshot,
		deleteSubscription,
		deleteTopic,
		listSnapshots,
		listSubscriptions,
		listTopicSubscriptions,
		listTopics,
		publish,
		pull,
		readStoredPubSubProject,
		seekToSnapshot,
		seekToTime,
		snapshotName,
		subscriptionName,
		topicName,
		updateTopic,
		validateResourceId,
		writeStoredPubSubProject,
		type PulledMessage,
		type SnapshotInfo,
		type SubscriptionInfo,
		type TopicInfo
	} from '#lib/gcppubsub.ts';

	type Tab = 'topics' | 'subscriptions' | 'snapshots';

	let project = $state(readStoredPubSubProject());
	let projectInput = $state(readStoredPubSubProject());
	let tab = $state<Tab>('topics');
	let topics = $state<TopicInfo[]>([]);
	let subs = $state<SubscriptionInfo[]>([]);
	let snaps = $state<SnapshotInfo[]>([]);
	let loading = $state(true);
	let loadError = $state('');

	let selTopic = $state<TopicInfo | null>(null);
	let topicSubs = $state<string[]>([]);
	let topicLabels = $state<MetaRow[]>([]);
	let retentionEdit = $state('');
	let pubData = $state('');
	let pubOrdering = $state('');
	let pubAttrs = $state<MetaRow[]>([]);
	let lastPublished = $state<string[]>([]);

	let selSub = $state<SubscriptionInfo | null>(null);
	let pullMax = $state(10);
	let pulled = $state<PulledMessage[]>([]);
	let pulledOnce = $state(false);
	let ackSel = $state<Record<string, boolean>>({});
	let seekSnapshot = $state('');
	let seekTime = $state('');
	let snapId = $state('');
	let busy = $state('');

	let topicModal = $state<Modal | null>(null);
	let tId = $state('');
	let tRetention = $state('');
	let tLabels = $state<MetaRow[]>([]);
	let tTouched = $state(false);

	let subModal = $state<Modal | null>(null);
	let sId = $state('');
	let sTopic = $state('');
	let sAck = $state(10);
	let sFilter = $state('');
	let sOrdering = $state(false);
	let sPush = $state('');
	let sDlTopic = $state('');
	let sMaxAttempts = $state(5);
	let sTouched = $state(false);

	const tError = $derived(validateResourceId(tId.trim()));
	const sError = $derived(validateResourceId(sId.trim()));
	const snapError = $derived(snapId.trim() ? validateResourceId(snapId.trim()) : null);

	const describe = describeGcpError;

	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const dd = 'text-gray-800 dark:text-gray-200 break-all';
	const btn = 'rounded-md bg-blue-600 px-3 py-1 text-sm text-white hover:bg-blue-700 disabled:opacity-50';
	const ghost = 'flex items-center gap-2 rounded-md border border-gray-200 dark:border-gray-700 px-3 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50';
	const cell = 'px-4 py-3';

	async function loadAll() {
		loading = true;
		loadError = '';
		try {
			[topics, subs, snaps] = await Promise.all([listTopics(project), listSubscriptions(project), listSnapshots(project)]);
			topics = topics.toSorted((a, b) => a.id.localeCompare(b.id));
			subs = subs.toSorted((a, b) => a.id.localeCompare(b.id));
			snaps = snaps.toSorted((a, b) => a.id.localeCompare(b.id));
			if (selTopic && !topics.some((t) => t.name === selTopic?.name)) selTopic = null;
			if (selSub && !subs.some((s) => s.name === selSub?.name)) selSub = null;
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load Pub/Sub: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function run(label: string, action: () => Promise<void>, success: string, failure: string): Promise<boolean> {
		busy = label;
		try {
			await action();
			if (success) toast.success(success);
			return true;
		} catch (e) {
			toast.error(`${failure}: ${describe(e)}`);
			return false;
		} finally {
			busy = '';
		}
	}

	async function selectTopic(t: TopicInfo) {
		selTopic = t;
		topicLabels = metadataToRows(t.labels);
		retentionEdit = t.retention ? durationToSeconds(t.retention) : '';
		lastPublished = [];
		try {
			topicSubs = await listTopicSubscriptions(project, t.id);
		} catch (e) {
			topicSubs = [];
			toast.error(`Failed to list subscriptions: ${describe(e)}`);
		}
	}

	function selectSub(s: SubscriptionInfo) {
		selSub = s;
		pulled = [];
		pulledOnce = false;
		ackSel = {};
		seekSnapshot = '';
		seekTime = '';
		snapId = '';
	}

	function openTopic() {
		tId = '';
		tRetention = '';
		tLabels = [];
		tTouched = false;
		topicModal?.open();
	}

	async function submitTopic(event: Event) {
		event.preventDefault();
		tTouched = true;
		if (tError) return;
		const bad = validateRows(tLabels, validateLabelKey);
		if (bad) return void toast.error(`Invalid label ${bad}`);
		const id = tId.trim();
		const ok = await run('create-topic', async () => {
			await createTopic(project, id, { labels: rowsToMap(tLabels), retentionSeconds: tRetention });
		}, `Created topic ${id}`, `Failed to create ${id}`);
		if (ok) {
			topicModal?.close('created');
			await loadAll();
			const made = topics.find((t) => t.id === id);
			if (made) await selectTopic(made);
		}
	}

	async function removeTopic(t: TopicInfo) {
		if (!(await confirmDestructive({ title: 'Delete topic', message: `Delete topic "${t.id}"? Its subscriptions are detached. This cannot be undone.` }))) return;
		const ok = await run('delete-topic', () => deleteTopic(project, t.id), `Deleted topic ${t.id}`, `Failed to delete ${t.id}`);
		if (ok) {
			if (selTopic?.name === t.name) selTopic = null;
			await loadAll();
		}
	}

	async function saveTopic() {
		const t = selTopic;
		if (!t) return;
		const labels = rowsToMap(topicLabels);
		const mask: string[] = [];
		const patch: Record<string, unknown> = { name: t.name };
		if (Object.keys(diffMap(t.labels, labels)).length > 0) {
			mask.push('labels');
			patch.labels = labels;
		}
		const current = t.retention ? durationToSeconds(t.retention) : '';
		if (retentionEdit.trim() !== current) {
			if (retentionEdit.trim() && !/^\d+$/.test(retentionEdit.trim())) return void toast.error('Retention must be a whole number of seconds');
			mask.push('messageRetentionDuration');
			if (retentionEdit.trim()) patch.messageRetentionDuration = `${retentionEdit.trim()}s`;
		}
		if (mask.length === 0) return void toast.info('No changes to save');
		const ok = await run('save-topic', async () => { await updateTopic(project, t.id, patch, mask.join(',')); }, `Saved topic ${t.id}`, 'Failed to save topic');
		if (ok) {
			await loadAll();
			const fresh = topics.find((x) => x.name === t.name);
			if (fresh) await selectTopic(fresh);
		}
	}

	async function doPublish() {
		const t = selTopic;
		if (!t) return;
		const bad = validateRows(pubAttrs, validateMetadataKey);
		if (bad) return void toast.error(`Invalid attribute ${bad}`);
		if (!pubData && pubAttrs.length === 0) return void toast.error('A message needs data or at least one attribute');
		const ok = await run('publish', async () => {
			lastPublished = await publish(project, t.id, { data: pubData, attributes: rowsToMap(pubAttrs), orderingKey: pubOrdering });
		}, '', 'Failed to publish');
		if (ok) toast.success(`Published message ${lastPublished.join(', ')}`);
	}

	function openSub() {
		sId = '';
		sTopic = topics[0]?.id ?? '';
		sAck = 10;
		sFilter = '';
		sOrdering = false;
		sPush = '';
		sDlTopic = '';
		sMaxAttempts = 5;
		sTouched = false;
		subModal?.open();
	}

	async function submitSub(event: Event) {
		event.preventDefault();
		sTouched = true;
		if (sError) return;
		if (!sTopic) return void toast.error('Choose a topic');
		const id = sId.trim();
		const ok = await run('create-sub', async () => {
			await createSubscription(project, id, {
				topic: topicName(project, sTopic),
				ackDeadlineSeconds: sAck,
				filter: sFilter.trim(),
				enableMessageOrdering: sOrdering,
				pushEndpoint: sPush.trim(),
				deadLetterTopic: sDlTopic ? topicName(project, sDlTopic) : '',
				maxDeliveryAttempts: sMaxAttempts,
				labels: {}
			});
		}, `Created subscription ${id}`, `Failed to create ${id}`);
		if (ok) {
			subModal?.close('created');
			await loadAll();
			const made = subs.find((s) => s.id === id);
			if (made) {
				tab = 'subscriptions';
				selectSub(made);
			}
		}
	}

	async function removeSub(s: SubscriptionInfo) {
		if (!(await confirmDestructive({ title: 'Delete subscription', message: `Delete subscription "${s.id}"? Unacknowledged messages are lost.` }))) return;
		const ok = await run('delete-sub', () => deleteSubscription(project, s.id), `Deleted subscription ${s.id}`, `Failed to delete ${s.id}`);
		if (ok) {
			if (selSub?.name === s.name) selSub = null;
			await loadAll();
		}
	}

	async function doPull() {
		const s = selSub;
		if (!s) return;
		await run('pull', async () => {
			pulled = await pull(project, s.id, pullMax);
			pulledOnce = true;
			ackSel = Object.fromEntries(pulled.map((m) => [m.ackId, true]));
		}, '', 'Failed to pull');
	}

	async function doAck() {
		const s = selSub;
		if (!s) return;
		const ids = pulled.filter((m) => ackSel[m.ackId]).map((m) => m.ackId);
		if (ids.length === 0) return void toast.error('Select at least one message');
		const ok = await run('ack', () => acknowledge(project, s.id, ids), `Acknowledged ${ids.length} message${ids.length === 1 ? '' : 's'}`, 'Failed to acknowledge');
		if (ok) pulled = pulled.filter((m) => !ids.includes(m.ackId));
	}

	async function doSeek() {
		const s = selSub;
		if (!s) return;
		if (!seekSnapshot && !seekTime) return void toast.error('Choose a snapshot or a time');
		const ok = await run('seek', async () => {
			if (seekSnapshot) await seekToSnapshot(project, s.id, snapshotName(project, seekSnapshot));
			else await seekToTime(project, s.id, new Date(seekTime).toISOString());
		}, seekSnapshot ? `Sought to snapshot ${seekSnapshot}` : 'Sought to time', 'Failed to seek');
		if (ok) {
			pulled = [];
			pulledOnce = false;
		}
	}

	async function doSnapshot() {
		const s = selSub;
		if (!s || snapError || !snapId.trim()) return;
		const id = snapId.trim();
		const ok = await run('snapshot', async () => { await createSnapshot(project, id, s.name); }, `Created snapshot ${id}`, 'Failed to create snapshot');
		if (ok) {
			snapId = '';
			await loadAll();
		}
	}

	async function removeSnap(sn: SnapshotInfo) {
		if (!(await confirmDestructive({ title: 'Delete snapshot', message: `Delete snapshot "${sn.id}"?` }))) return;
		const ok = await run('delete-snap', () => deleteSnapshot(project, sn.id), `Deleted snapshot ${sn.id}`, `Failed to delete ${sn.id}`);
		if (ok) await loadAll();
	}

	function applyProject(event: Event) {
		event.preventDefault();
		const next = projectInput.trim();
		if (!next) return;
		project = next;
		writeStoredPubSubProject(next);
		selTopic = null;
		selSub = null;
		void loadAll();
	}

	const tabs: { id: Tab; label: string }[] = [
		{ id: 'topics', label: 'Topics' },
		{ id: 'subscriptions', label: 'Subscriptions' },
		{ id: 'snapshots', label: 'Snapshots' }
	];

	onMount(loadAll);
</script>

<div class="space-y-6 pb-20" data-testid="pubsub-dashboard">
	<PageHeader icon={Radio} title="Pub/Sub" description="Google Cloud Pub/Sub: topics, subscriptions and snapshots" color="blue" onRefresh={loadAll}>
		{#snippet actions()}
			<button class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" onclick={openTopic} disabled={loading || Boolean(loadError)} data-testid="pubsub-new-topic">
				<Plus class="h-4 w-4" /> New topic
			</button>
			<button class="flex items-center gap-2 rounded-lg border border-blue-600 px-3 py-2 text-sm text-blue-700 dark:text-blue-300 hover:bg-blue-50 dark:hover:bg-blue-950/40 disabled:opacity-50" onclick={openSub} disabled={loading || Boolean(loadError) || topics.length === 0} data-testid="pubsub-new-sub">
				<Plus class="h-4 w-4" /> New subscription
			</button>
		{/snippet}
	</PageHeader>

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyProject}>
		<label for="pubsub-project">Project</label>
		<input id="pubsub-project" class="w-56 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={projectInput} data-testid="pubsub-project" />
		<button type="submit" class="rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={!projectInput.trim() || projectInput.trim() === project}>Switch</button>
		<span>Resources are listed per project.</span>
	</form>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="pubsub-error">
			<p class="font-medium">Could not reach Pub/Sub</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadAll}>Retry</button>
		</div>
	{:else}
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700" role="tablist">
			{#each tabs as t (t.id)}
				<button role="tab" aria-selected={tab === t.id} class="px-4 py-2 text-sm font-medium border-b-2 -mb-px {tab === t.id ? 'border-blue-600 text-blue-700 dark:text-blue-300' : 'border-transparent text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}" onclick={() => (tab = t.id)} data-testid="pubsub-tab-{t.id}">
					{t.label} ({t.id === 'topics' ? topics.length : t.id === 'subscriptions' ? subs.length : snaps.length})
				</button>
			{/each}
		</div>

		{#if loading}
			<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
		{:else if tab === 'topics'}
			<div class="grid gap-6 lg:grid-cols-[22rem_1fr]">
				<section aria-label="Topics">
					{#if topics.length === 0}
						<p class="py-8 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="pubsub-no-topics">No topics in {project}</p>
					{:else}
						<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
							{#each topics as t (t.name)}
								<li class="flex items-center {selTopic?.name === t.name ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="pubsub-topic-row">
									<button class="flex-1 min-w-0 px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectTopic(t)}>
										<span class="block truncate">{t.id}</span>
										<span class="block text-[11px] font-normal text-gray-500 dark:text-gray-400">{Object.keys(t.labels).length} labels{t.retention ? ` · retention ${t.retention}` : ''}</span>
									</button>
									<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Delete topic {t.id}" onclick={() => removeTopic(t)}><Trash2 class="h-4 w-4" /></button>
								</li>
							{/each}
						</ul>
					{/if}
				</section>
				<section aria-label="Topic details" class="min-w-0">
					{#if !selTopic}
						<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="pubsub-topic-hint">Select a topic to publish messages and edit settings</p>
					{:else}
						{@const t = selTopic}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-5" data-testid="pubsub-topic-detail">
							<div class="flex items-start justify-between gap-4">
								<div class="min-w-0"><h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{t.id}</h3><p class="font-mono text-xs text-gray-500 dark:text-gray-400 break-all">{t.name}</p></div>
								<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={() => (selTopic = null)} aria-label="Close details"><X class="h-4 w-4" /></button>
							</div>
							<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
								<dt class={dt}>Subscriptions</dt>
								<dd class={dd} data-testid="pubsub-topic-subs">{topicSubs.length === 0 ? 'none' : topicSubs.map((n) => n.slice(n.lastIndexOf('/') + 1)).join(', ')}</dd>
							</dl>
							<div class="grid gap-6 md:grid-cols-2">
								<div class="space-y-3">
									<label class="block text-xs text-gray-500 dark:text-gray-400">Message retention (seconds)
										<input class={input} bind:value={retentionEdit} placeholder="unset" aria-label="Retention seconds" data-testid="pubsub-retention" />
									</label>
									<KeyValueEditor bind:rows={topicLabels} label="Labels" testId="pubsub-topic-labels" validateKey={validateLabelKey} hideSave />
									<button type="button" class={btn} disabled={busy === 'save-topic'} onclick={saveTopic} data-testid="pubsub-topic-save">Save topic</button>
								</div>
								<div class="space-y-3" data-testid="pubsub-publish">
									<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Publish message</h4>
									<label class="block text-xs text-gray-500 dark:text-gray-400">Data<textarea class="{input} font-mono" rows="3" bind:value={pubData} aria-label="Message data" data-testid="pubsub-pub-data"></textarea></label>
									<label class="block text-xs text-gray-500 dark:text-gray-400">Ordering key<input class={input} bind:value={pubOrdering} aria-label="Ordering key" data-testid="pubsub-pub-ordering" /></label>
									<KeyValueEditor bind:rows={pubAttrs} label="Attributes" testId="pubsub-pub-attrs" validateKey={validateMetadataKey} hideSave />
									<button type="button" class="{btn} flex items-center gap-2" disabled={busy === 'publish'} onclick={doPublish} data-testid="pubsub-pub-submit"><Send class="h-4 w-4" /> Publish</button>
									{#if lastPublished.length > 0}<p class="text-xs text-green-700 dark:text-green-400" data-testid="pubsub-pub-result">Published message id {lastPublished.join(', ')}</p>{/if}
								</div>
							</div>
						</div>
					{/if}
				</section>
			</div>
		{:else if tab === 'subscriptions'}
			<div class="grid gap-6 lg:grid-cols-[22rem_1fr]">
				<section aria-label="Subscriptions">
					{#if subs.length === 0}
						<p class="py-8 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="pubsub-no-subs">No subscriptions in {project}</p>
					{:else}
						<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
							{#each subs as s (s.name)}
								<li class="flex items-center {selSub?.name === s.name ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="pubsub-sub-row">
									<button class="flex-1 min-w-0 px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectSub(s)}>
										<span class="block truncate">{s.id}</span>
										<span class="block text-[11px] font-normal text-gray-500 dark:text-gray-400">{s.topicId} · {s.pushEndpoint ? 'push' : 'pull'} · ack {s.ackDeadlineSeconds}s</span>
									</button>
									<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Delete subscription {s.id}" onclick={() => removeSub(s)}><Trash2 class="h-4 w-4" /></button>
								</li>
							{/each}
						</ul>
					{/if}
				</section>
				<section aria-label="Subscription details" class="min-w-0">
					{#if !selSub}
						<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="pubsub-sub-hint">Select a subscription to pull, acknowledge and seek</p>
					{:else}
						{@const s = selSub}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-5" data-testid="pubsub-sub-detail">
							<div class="flex items-start justify-between gap-4">
								<div class="min-w-0"><h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{s.id}</h3><p class="font-mono text-xs text-gray-500 dark:text-gray-400 break-all">{s.name}</p></div>
								<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={() => (selSub = null)} aria-label="Close details"><X class="h-4 w-4" /></button>
							</div>
							<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm" data-testid="pubsub-sub-props">
								<dt class={dt}>Topic</dt><dd class={dd}>{s.topic}</dd>
								<dt class={dt}>Ack deadline</dt><dd class={dd}>{s.ackDeadlineSeconds}s</dd>
								<dt class={dt}>Delivery</dt><dd class={dd}>{s.pushEndpoint ? `push to ${s.pushEndpoint}` : 'pull'}</dd>
								<dt class={dt}>Filter</dt><dd class="{dd} font-mono text-xs">{s.filter || '—'}</dd>
								<dt class={dt}>Message ordering</dt><dd class={dd}>{s.enableMessageOrdering ? 'enabled' : 'disabled'}</dd>
								<dt class={dt}>Dead letter</dt>
								<dd class={dd} data-testid="pubsub-sub-deadletter">{s.deadLetter ? `${s.deadLetter.deadLetterTopic} after ${s.deadLetter.maxDeliveryAttempts} attempts` : '—'}</dd>
								<dt class={dt}>State</dt><dd class={dd}>{s.state || '—'}</dd>
							</dl>

							<div class="space-y-3" data-testid="pubsub-pull">
								<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Pull messages</h4>
								<div class="flex flex-wrap items-end gap-3">
									<label class="text-xs text-gray-500 dark:text-gray-400">Max messages<input type="number" min="1" max="1000" class="{input} w-28" bind:value={pullMax} aria-label="Max messages" /></label>
									<button type="button" class="{btn} flex items-center gap-2" disabled={busy === 'pull'} onclick={doPull} data-testid="pubsub-pull-submit"><Download class="h-4 w-4" /> Pull</button>
									<button type="button" class={btn} disabled={busy === 'ack' || pulled.length === 0} onclick={doAck} data-testid="pubsub-ack-submit">Acknowledge selected</button>
								</div>
								{#if pulledOnce && pulled.length === 0}
									<p class="text-xs text-gray-500 dark:text-gray-400" data-testid="pubsub-pull-empty">No messages available</p>
								{/if}
								{#if pulled.length > 0}
									<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
										<table class="w-full text-sm">
											<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}></th><th class={cell}>Message</th><th class={cell}>Attributes</th><th class={cell}>Published</th></tr></thead>
											<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
												{#each pulled as m (m.ackId)}
													<tr data-testid="pubsub-pulled-row">
														<td class={cell}><input type="checkbox" bind:checked={ackSel[m.ackId]} aria-label="Acknowledge message {m.messageId}" /></td>
														<td class={cell}>
															<p class="font-mono text-xs break-all" data-testid="pubsub-pulled-data">{m.data || m.base64}</p>
															<p class="text-[11px] text-gray-500 dark:text-gray-400">id {m.messageId}{m.orderingKey ? ` · key ${m.orderingKey}` : ''}{m.deliveryAttempt ? ` · attempt ${m.deliveryAttempt}` : ''}</p>
														</td>
														<td class="{cell} font-mono text-xs">{Object.entries(m.attributes).map(([k, v]) => `${k}=${v}`).join(', ') || '—'}</td>
														<td class="{cell} whitespace-nowrap text-xs">{m.publishTime}</td>
													</tr>
												{/each}
											</tbody>
										</table>
									</div>
								{/if}
							</div>

							<div class="grid gap-6 md:grid-cols-2">
								<div class="space-y-3" data-testid="pubsub-seek">
									<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Seek</h4>
									<label class="block text-xs text-gray-500 dark:text-gray-400">To snapshot
										<select class={input} bind:value={seekSnapshot} aria-label="Seek to snapshot" data-testid="pubsub-seek-snapshot" onchange={() => (seekTime = '')}>
											<option value="">(none)</option>
											{#each snaps.filter((x) => x.topic === s.topic) as sn (sn.name)}<option value={sn.id}>{sn.id}</option>{/each}
										</select>
									</label>
									<label class="block text-xs text-gray-500 dark:text-gray-400">Or to time
										<input type="datetime-local" class={input} bind:value={seekTime} aria-label="Seek to time" data-testid="pubsub-seek-time" oninput={() => (seekSnapshot = '')} />
									</label>
									<button type="button" class={btn} disabled={busy === 'seek'} onclick={doSeek} data-testid="pubsub-seek-submit">Seek</button>
								</div>
								<div class="space-y-3" data-testid="pubsub-snapshot-create">
									<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Create snapshot</h4>
									<label class="block text-xs text-gray-500 dark:text-gray-400">Snapshot name<input class={input} bind:value={snapId} aria-label="Snapshot name" data-testid="pubsub-snapshot-name" /></label>
									{#if snapError}<p class="text-xs text-red-600 dark:text-red-400">{snapError}</p>{/if}
									<button type="button" class="{btn} flex items-center gap-2" disabled={busy === 'snapshot' || !snapId.trim() || Boolean(snapError)} onclick={doSnapshot} data-testid="pubsub-snapshot-submit"><Camera class="h-4 w-4" /> Create snapshot</button>
								</div>
							</div>
						</div>
					{/if}
				</section>
			</div>
		{:else}
			{#if snaps.length === 0}
				<p class="py-8 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="pubsub-no-snaps">No snapshots in {project}. Create one from a subscription.</p>
			{:else}
				<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
					<table class="w-full text-sm" data-testid="pubsub-snapshots">
						<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>Topic</th><th class={cell}>Expires</th><th class="{cell} text-right">Actions</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each snaps as sn (sn.name)}
								<tr data-testid="pubsub-snapshot-row">
									<td class="{cell} font-medium">{sn.id}</td>
									<td class="{cell} font-mono text-xs">{sn.topic}</td>
									<td class="{cell} whitespace-nowrap text-xs">{sn.expireTime || '—'}</td>
									<td class="{cell} text-right"><button class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Delete snapshot {sn.id}" onclick={() => removeSnap(sn)}><Trash2 class="h-4 w-4" /></button></td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>

<Modal bind:this={topicModal} title="New topic" titleId="pubsub-topic-title">
	{#snippet children()}
		<form id="pubsub-topic-form" class="space-y-3" onsubmit={submitTopic} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Topic ID</span>
				<input class={input} bind:value={tId} placeholder="my-topic" autocomplete="off" aria-label="Topic ID" />
				{#if tTouched && tError}<span class="block text-xs text-red-600 dark:text-red-400">{tError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Message retention (seconds, optional)</span>
				<input class={input} bind:value={tRetention} placeholder="600 to 2678400" aria-label="Message retention seconds" />
			</label>
			<KeyValueEditor bind:rows={tLabels} label="Labels" testId="pubsub-create-labels" validateKey={validateLabelKey} hideSave />
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => topicModal?.close()}>Cancel</button>
		<button type="submit" form="pubsub-topic-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={busy === 'create-topic'} data-testid="pubsub-topic-submit">{busy === 'create-topic' ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={subModal} title="New subscription" titleId="pubsub-sub-title">
	{#snippet children()}
		<form id="pubsub-sub-form" class="space-y-3" onsubmit={submitSub} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Subscription ID</span>
				<input class={input} bind:value={sId} placeholder="my-subscription" autocomplete="off" aria-label="Subscription ID" />
				{#if sTouched && sError}<span class="block text-xs text-red-600 dark:text-red-400">{sError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Topic</span>
				<select class={input} bind:value={sTopic} aria-label="Topic">{#each topics as t (t.name)}<option value={t.id}>{t.id}</option>{/each}</select>
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Ack deadline (seconds, 10 to 600)</span>
				<input type="number" min="10" max="600" class={input} bind:value={sAck} aria-label="Ack deadline seconds" />
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Filter (optional)</span>
				<input class="{input} font-mono" bind:value={sFilter} placeholder={'attributes.kind = "order"'} aria-label="Filter" />
			</label>
			<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={sOrdering} aria-label="Enable message ordering" /> Enable message ordering</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Push endpoint (optional, leave empty for pull)</span>
				<input class="{input} font-mono" bind:value={sPush} placeholder="http://localhost:9000/hook" aria-label="Push endpoint" />
			</label>
			<fieldset class="space-y-2 rounded-md border border-gray-200 dark:border-gray-700 p-3">
				<legend class="px-1 text-sm font-medium text-gray-700 dark:text-gray-300">Dead-letter policy</legend>
				<label class="block space-y-1 text-sm">
					<span class="text-gray-600 dark:text-gray-400">Dead-letter topic</span>
					<select class={input} bind:value={sDlTopic} aria-label="Dead-letter topic"><option value="">(none)</option>{#each topics as t (t.name)}<option value={t.id}>{t.id}</option>{/each}</select>
				</label>
				<label class="block space-y-1 text-sm">
					<span class="text-gray-600 dark:text-gray-400">Max delivery attempts (5 to 100)</span>
					<input type="number" min="5" max="100" class={input} bind:value={sMaxAttempts} aria-label="Max delivery attempts" disabled={!sDlTopic} />
				</label>
			</fieldset>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => subModal?.close()}>Cancel</button>
		<button type="submit" form="pubsub-sub-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={busy === 'create-sub'} data-testid="pubsub-sub-submit">{busy === 'create-sub' ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>
