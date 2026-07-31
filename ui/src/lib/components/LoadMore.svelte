<script lang="ts">
	// Matches the "Load More" button markup used e.g. in
	// cloudwatchlogs/+page.svelte:936-943.
	type Props = {
		hasMore: boolean;
		loading: boolean;
		onLoadMore: () => void;
	};

	let { hasMore, loading, onLoadMore }: Props = $props();
</script>

{#if hasMore}
	<div class="flex justify-center">
		<button
			onclick={onLoadMore}
			disabled={loading}
			class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-800 text-gray-600 dark:text-gray-300 disabled:opacity-50"
		>
			{loading ? 'Loading...' : 'Load More'}
		</button>
	</div>
{/if}
