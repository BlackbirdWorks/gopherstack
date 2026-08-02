<script lang="ts">
	// Reusable tag editor shared by every one of Lightsail's 16 taggable
	// resource kinds (of 20 ResourceType values -- 4 carry no Tags field at
	// all, see shared.ts's TAGS_NOT_SUPPORTED). TagResource/UntagResource key
	// on ResourceName, not an ARN (services/lightsail/PARITY.md point 4), so
	// each panel only needs to hand this component the resource's current tag
	// map plus two callbacks that call TagResource/UntagResource with that
	// resource's own name.
	import { Trash2 } from 'lucide-svelte';
	import { describeError } from './shared';

	type Props = {
		tags: Record<string, string>;
		onAdd: (key: string, value: string) => Promise<void>;
		onRemove: (key: string) => Promise<void>;
	};

	let { tags, onAdd, onRemove }: Props = $props();

	let newKey = $state('');
	let newValue = $state('');
	let busy = $state(false);
	let error = $state<string | null>(null);

	async function submitAdd(): Promise<void> {
		if (!newKey.trim()) return;
		busy = true;
		error = null;
		try {
			await onAdd(newKey.trim(), newValue);
			newKey = '';
			newValue = '';
		} catch (e) {
			error = describeError(e);
		} finally {
			busy = false;
		}
	}

	async function handleRemove(key: string): Promise<void> {
		busy = true;
		error = null;
		try {
			await onRemove(key);
		} catch (e) {
			error = describeError(e);
		} finally {
			busy = false;
		}
	}
</script>

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Tags</p>
	{#if Object.keys(tags).length === 0}
		<p class="text-sm text-slate-500 dark:text-slate-400">No tags.</p>
	{:else}
		<ul class="space-y-1">
			{#each Object.entries(tags) as [key, value] (key)}
				<li class="flex items-center justify-between gap-2 text-sm">
					<span class="text-slate-700 dark:text-slate-300">{key} = {value}</span>
					<button
						type="button"
						onclick={() => handleRemove(key)}
						disabled={busy}
						aria-label="Remove tag {key}"
						class="text-gray-400 hover:text-red-500 disabled:opacity-50"
					>
						<Trash2 class="w-3.5 h-3.5" />
					</button>
				</li>
			{/each}
		</ul>
	{/if}
	<div class="flex items-center gap-2">
		<input
			bind:value={newKey}
			placeholder="Key"
			aria-label="New tag key"
			class="w-1/3 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
		/>
		<input
			bind:value={newValue}
			placeholder="Value"
			aria-label="New tag value"
			class="w-1/3 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
		/>
		<button
			type="button"
			onclick={submitAdd}
			disabled={busy || !newKey.trim()}
			class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
		>
			Add tag
		</button>
	</div>
	{#if error}
		<p class="text-sm text-red-600 dark:text-red-400">{error}</p>
	{/if}
</div>
