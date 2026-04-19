<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getSNSClient } from '$lib/aws-client';
	import {
		ListTopicsCommand,
		GetTopicAttributesCommand,
		ListSubscriptionsByTopicCommand,
		CreateTopicCommand,
		DeleteTopicCommand,
		SubscribeCommand,
		PublishCommand,
		UnsubscribeCommand,
		type Topic,
		type Subscription
	} from '@aws-sdk/client-sns';
	import { toast } from 'svelte-sonner';
	import { Bell, Search, RefreshCw, Plus, Trash2, Send, Users, ChevronRight, X, Copy } from 'lucide-svelte';

	const sns = getSNSClient();

	let loading = $state(false);
	let topics = $state<Array<Topic & { Attributes?: Record<string, string> }>>([]);
	let searchQuery = $state('');
	let selectedTopic = $state<(Topic & { Attributes?: Record<string, string> }) | null>(null);
	let subscriptions = $state<Subscription[]>([]);
	let loadingSubscriptions = $state(false);

	// Create topic modal
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newTopicName = $state('');
	let newTopicFifo = $state(false);

	// Subscribe modal
	let showSubscribeModal = $state(false);
	let subscribing = $state(false);
	let subProtocol = $state('email');
	let subEndpoint = $state('');

	// Publish modal
	let showPublishModal = $state(false);
	let publishing = $state(false);
	let pubSubject = $state('');
	let pubMessage = $state('');
	let pubAttributes = $state('{}');
	let pubDeduplicationId = $state('');

	const filteredTopics = $derived(
		topics.filter((t) => {
			const arn = t.TopicArn ?? '';
			const name = arn.split(':').pop() ?? '';
			return name.toLowerCase().includes(searchQuery.toLowerCase());
		})
	);

	function topicName(arn: string | undefined): string {
		return arn?.split(':').pop() ?? arn ?? '';
	}

	function isFifo(arn: string | undefined): boolean {
		return arn?.endsWith('.fifo') ?? false;
	}

	async function loadTopics() {
		loading = true;
		try {
			const res = await sns.send(new ListTopicsCommand({}));
			const raw = res.Topics ?? [];
			// Load attributes for each topic (up to 20 to avoid flooding)
			const enriched = await Promise.all(
				raw.slice(0, 20).map(async (t) => {
					try {
						const attrs = await sns.send(new GetTopicAttributesCommand({ TopicArn: t.TopicArn }));
						return { ...t, Attributes: attrs.Attributes ?? {} };
					} catch {
						return { ...t, Attributes: {} };
					}
				})
			);
			topics = enriched;
		} catch (err: unknown) {
			toast.error(`Failed to load topics: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createTopic() {
		if (!newTopicName.trim()) return;
		creating = true;
		try {
			const name = newTopicFifo ? `${newTopicName.trim()}.fifo` : newTopicName.trim();
			await sns.send(new CreateTopicCommand({
				Name: name,
				Attributes: newTopicFifo ? { FifoTopic: 'true' } : undefined
			}));
			toast.success(`Topic "${name}" created`);
			showCreateModal = false;
			newTopicName = '';
			newTopicFifo = false;
			await loadTopics();
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteTopic(arn: string) {
		const name = topicName(arn);
		if (!await confirmDestructive({ title: 'Delete Topic', message: `Delete topic "${name}"? All subscriptions will be removed and no further messages will be delivered.` })) return;
		try {
			await sns.send(new DeleteTopicCommand({ TopicArn: arn }));
			toast.success(`Topic "${name}" deleted`);
			if (selectedTopic?.TopicArn === arn) selectedTopic = null;
			await loadTopics();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function selectTopic(topic: typeof topics[0]) {
		selectedTopic = topic;
		loadingSubscriptions = true;
		try {
			const res = await sns.send(new ListSubscriptionsByTopicCommand({ TopicArn: topic.TopicArn }));
			subscriptions = res.Subscriptions ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load subscriptions: ${(err as Error).message}`);
		} finally {
			loadingSubscriptions = false;
		}
	}

	async function subscribe() {
		if (!selectedTopic || !subEndpoint.trim()) return;
		subscribing = true;
		try {
			await sns.send(new SubscribeCommand({
				TopicArn: selectedTopic.TopicArn,
				Protocol: subProtocol,
				Endpoint: subEndpoint.trim()
			}));
			toast.success(`Subscription created`);
			showSubscribeModal = false;
			subEndpoint = '';
			await selectTopic(selectedTopic);
		} catch (err: unknown) {
			toast.error(`Subscribe failed: ${(err as Error).message}`);
		} finally {
			subscribing = false;
		}
	}

	async function unsubscribe(arn: string) {
		if (!await confirmDestructive({ title: 'Remove Subscription', message: 'Remove this SNS subscription? The endpoint will stop receiving messages.', confirmLabel: 'Remove' })) return;
		try {
			await sns.send(new UnsubscribeCommand({ SubscriptionArn: arn }));
			toast.success('Subscription removed');
			if (selectedTopic) await selectTopic(selectedTopic);
		} catch (err: unknown) {
			toast.error(`Unsubscribe failed: ${(err as Error).message}`);
		}
	}

	async function publish() {
		if (!selectedTopic || !pubMessage.trim()) return;
		publishing = true;
		try {
			let attrs: Record<string, { DataType: string; StringValue: string }> | undefined;
			try {
				const parsed = JSON.parse(pubAttributes);
				if (Object.keys(parsed).length > 0) {
					attrs = Object.fromEntries(
						Object.entries(parsed).map(([k, v]) => [k, { DataType: 'String', StringValue: String(v) }])
					);
				}
			} catch {
				// ignore parse errors
			}
			await sns.send(new PublishCommand({
				TopicArn: selectedTopic.TopicArn,
				Subject: pubSubject || undefined,
				Message: pubMessage,
				MessageAttributes: attrs,
				MessageDeduplicationId: pubDeduplicationId || undefined
			}));
			toast.success('Message published');
			showPublishModal = false;
			pubSubject = '';
			pubMessage = '';
			pubAttributes = '{}';
			pubDeduplicationId = '';
		} catch (err: unknown) {
			toast.error(`Publish failed: ${(err as Error).message}`);
		} finally {
			publishing = false;
		}
	}

	async function copyArn(arn: string | undefined) {
		if (!arn) return;
		await navigator.clipboard.writeText(arn);
		toast.success('ARN copied');
	}

	onMount(() => { loadTopics(); });
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Bell class="w-6 h-6 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">SNS Topics</h1>
				<p class="text-slate-600 dark:text-slate-300">Simple Notification Service</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={() => loadTopics()}
				class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white"
				title="Refresh"
			>
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
			<button
				onclick={() => { showCreateModal = true; }}
				class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"
			>
				<Plus class="w-4 h-4" />
				Create Topic
			</button>
		</div>
	</div>

	<!-- Search -->
	<div class="relative">
		<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
		<input
			type="text"
			bind:value={searchQuery}
			placeholder="Search topics..."
			class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
		/>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Topic List -->
		<div class="lg:col-span-1 space-y-2">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading topics...</p>
				</div>
			{:else if filteredTopics.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
					<Bell class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">No topics found</p>
				</div>
			{:else}
				{#each filteredTopics as topic}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectTopic(topic)}
						onkeypress={(e) => { if (e.key === 'Enter') selectTopic(topic); }}
						class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-4 hover:border-indigo-400 transition-colors cursor-pointer {selectedTopic?.TopicArn === topic.TopicArn ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
					>
						<div class="flex items-center justify-between">
							<div class="min-w-0 flex-1">
								<p class="font-medium text-slate-900 dark:text-white truncate">{topicName(topic.TopicArn)}</p>
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
									{topic.Attributes?.SubscriptionsConfirmed ?? 0} subscriptions
								</p>
							</div>
							<div class="flex items-center gap-1 ml-2 flex-shrink-0">
								{#if isFifo(topic.TopicArn)}
									<span class="px-2 py-0.5 text-xs rounded-full bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">FIFO</span>
								{/if}
								<button
									onclick={(e) => { e.stopPropagation(); deleteTopic(topic.TopicArn ?? ''); }}
									class="p-1 text-slate-400 hover:text-red-500"
								>
									<Trash2 class="w-4 h-4" />
								</button>
							</div>
						</div>
					</div>
				{/each}
			{/if}
		</div>

		<!-- Topic Detail -->
		<div class="lg:col-span-2">
			{#if selectedTopic}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-6">
					<div class="flex items-start justify-between">
						<div>
							<h2 class="text-xl font-bold text-slate-900 dark:text-white">{topicName(selectedTopic.TopicArn)}</h2>
							<button
								onclick={() => copyArn(selectedTopic?.TopicArn)}
								class="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400 hover:text-indigo-500 mt-1 font-mono"
							>
								<Copy class="w-3 h-3" />
								{selectedTopic.TopicArn}
							</button>
						</div>
						<div class="flex gap-2">
							<button
								onclick={() => { showSubscribeModal = true; }}
								class="px-3 py-1.5 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-1.5 text-sm"
							>
								<Users class="w-4 h-4" />
								Subscribe
							</button>
							<button
								onclick={() => { showPublishModal = true; }}
								class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm"
							>
								<Send class="w-4 h-4" />
								Publish
							</button>
						</div>
					</div>

					<!-- Attributes -->
					<div class="grid grid-cols-2 gap-4">
						{#each [
							['Messages Confirmed', selectedTopic.Attributes?.SubscriptionsConfirmed ?? '0'],
							['Messages Pending', selectedTopic.Attributes?.SubscriptionsPending ?? '0'],
							['Messages Deleted', selectedTopic.Attributes?.SubscriptionsDeleted ?? '0'],
							['Type', isFifo(selectedTopic.TopicArn) ? 'FIFO' : 'Standard']
						] as [label, value]}
							<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
								<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
								<p class="font-semibold text-slate-900 dark:text-white mt-0.5">{value}</p>
							</div>
						{/each}
					</div>

					<!-- Subscriptions -->
					<div>
						<h3 class="font-semibold text-slate-900 dark:text-white mb-3 flex items-center gap-2">
							<Users class="w-4 h-4 text-indigo-500" />
							Subscriptions ({subscriptions.length})
						</h3>
						{#if loadingSubscriptions}
							<div class="text-center py-4">
								<div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div>
							</div>
						{:else if subscriptions.length === 0}
							<p class="text-slate-500 dark:text-slate-400 text-sm">No subscriptions yet</p>
						{:else}
							<div class="space-y-2">
								{#each subscriptions as sub}
									<div class="flex items-center justify-between bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
										<div class="min-w-0 flex-1">
											<span class="px-2 py-0.5 text-xs rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 font-mono mr-2">{sub.Protocol}</span>
											<span class="text-sm text-slate-700 dark:text-slate-300 truncate">{sub.Endpoint}</span>
										</div>
										<button
											onclick={() => unsubscribe(sub.SubscriptionArn ?? '')}
											class="ml-2 p-1 text-slate-400 hover:text-red-500"
										>
											<X class="w-4 h-4" />
										</button>
									</div>
								{/each}
							</div>
						{/if}
					</div>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Bell class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a topic to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Topic Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Topic</h2>
			<form onsubmit={(e) => { e.preventDefault(); createTopic(); }} class="space-y-4">
				<div>
					<label for="sns-topic-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Topic Name</label>
					<input
						id="sns-topic-name"
						type="text"
						bind:value={newTopicName}
						placeholder="e.g. my-notifications"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
						required
					/>
					{#if newTopicFifo}
						<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">Final name: {newTopicName.trim()}.fifo</p>
					{/if}
				</div>
				<label class="flex items-center gap-2 cursor-pointer">
					<input type="checkbox" bind:checked={newTopicFifo} class="rounded" />
					<span class="text-sm text-slate-700 dark:text-slate-300">FIFO topic (ordered, deduplicated)</span>
				</label>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
					<button type="submit" disabled={creating} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creating ? 'Creating...' : 'Create Topic'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Subscribe Modal -->
{#if showSubscribeModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Subscribe to Topic</h2>
			<form onsubmit={(e) => { e.preventDefault(); subscribe(); }} class="space-y-4">
				<div>
					<label for="sns-protocol" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Protocol</label>
					<select
						id="sns-protocol"
						bind:value={subProtocol}
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
					>
						{#each ['email', 'sqs', 'http', 'https', 'lambda', 'sms', 'application'] as proto}
							<option value={proto}>{proto}</option>
						{/each}
					</select>
				</div>
				<div>
					<label for="sns-endpoint" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Endpoint</label>
					<input
						id="sns-endpoint"
						type="text"
						bind:value={subEndpoint}
						placeholder="e.g. user@example.com"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
						required
					/>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showSubscribeModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
					<button type="submit" disabled={subscribing} class="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 disabled:opacity-50">
						{subscribing ? 'Subscribing...' : 'Subscribe'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Publish Modal -->
{#if showPublishModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Publish Message</h2>
			<form onsubmit={(e) => { e.preventDefault(); publish(); }} class="space-y-4">
				<div>
					<label for="sns-subject" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Subject (optional)</label>
					<input
						id="sns-subject"
						type="text"
						bind:value={pubSubject}
						placeholder="e.g. Alert notification"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
					/>
				</div>
				<div>
					<label for="sns-message" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Message</label>
					<textarea
						id="sns-message"
						bind:value={pubMessage}
						rows={4}
						placeholder="Message body..."
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"
						required
					></textarea>
				</div>
				<div>
					<label for="sns-msg-attrs" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Message Attributes (JSON)</label>
					<textarea
						id="sns-msg-attrs"
						bind:value={pubAttributes}
						rows={2}
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"
					></textarea>
				</div>
				{#if isFifo(selectedTopic?.TopicArn)}
					<div>
						<label for="sns-dedup-id" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Deduplication ID (FIFO)</label>
						<input
							id="sns-dedup-id"
							type="text"
							bind:value={pubDeduplicationId}
							placeholder="e.g. unique-msg-id-123"
							class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
						/>
					</div>
				{/if}
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showPublishModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white">Cancel</button>
					<button type="submit" disabled={publishing} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2">
						<Send class="w-4 h-4" />
						{publishing ? 'Publishing...' : 'Publish'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
