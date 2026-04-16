<script lang="ts">
	import { onMount } from 'svelte';
	import { getIoTAnalyticsClient } from '$lib/aws-client';
	import { ListChannelsCommand, CreateChannelCommand, DeleteChannelCommand } from '@aws-sdk/client-iotanalytics';
	import { toast } from 'svelte-sonner';

	const iotAnalytics = getIoTAnalyticsClient();
	let channels = $state<Array<{ channelName?: string }>>([]);
	let showCreateModal = $state(false);
	let channelName = $state('');

	async function loadChannels() {
		const out = await iotAnalytics.send(new ListChannelsCommand({}));
		channels = out.channelSummaries ?? [];
	}

	async function createChannel() {
		try {
			await iotAnalytics.send(new CreateChannelCommand({ channelName }));
			showCreateModal = false;
			channelName = '';
			await loadChannels();
		} catch (err: unknown) {
			toast.error(`Failed to create channel: ${(err as Error).message}`);
		}
	}

	async function deleteChannel(name: string) {
		try {
			await iotAnalytics.send(new DeleteChannelCommand({ channelName: name }));
			await loadChannels();
		} catch (err: unknown) {
			toast.error(`Failed to delete channel: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		void loadChannels();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">IoT Analytics</h1>
	</div>
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="mb-4 flex justify-end"><button type="button" onclick={() => (showCreateModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create Channel</button></div>
		<table class="w-full text-left text-sm"><tbody>{#if channels.length === 0}<tr><td class="py-3 text-slate-500 dark:text-slate-400">No channels found</td></tr>{:else}{#each channels as channel}<tr class="border-b border-slate-100 dark:border-slate-800"><td class="py-3 font-medium">{channel.channelName}</td><td class="py-3"><button type="button" onclick={() => deleteChannel(channel.channelName ?? '')} class="rounded-lg border px-3 py-1 text-sm">Delete</button></td></tr>{/each}{/if}</tbody></table>
	</div>
</div>

{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<form onsubmit={(event) => { event.preventDefault(); createChannel(); }} class="space-y-4">
				<input id="channel-name" bind:value={channelName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Channel name" />
				<div class="flex justify-end gap-3"><button type="button" onclick={() => (showCreateModal = false)} class="px-4 py-2 text-sm">Cancel</button><button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button></div>
			</form>
		</div>
	</div>
{/if}
