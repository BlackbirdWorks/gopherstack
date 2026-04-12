<script lang="ts">
	import { toast } from 'svelte-sonner';

	let formState = $state({
		autoRefresh: true,
		refreshInterval: 5,
		maxConsoleEntries: 100,
	});

	function saveSettings(e: Event) {
		e.preventDefault();
		// For the migration demo, we just simulate saving and show a toast
		localStorage.setItem('gopherstack_settings', JSON.stringify(formState));
		toast.success('Settings saved successfully');
	}
</script>

<div class="space-y-6 max-w-2xl">
	<div class="flex justify-between items-center bg-white/40 dark:bg-zinc-900/40 p-4 rounded-xl shadow-lg border border-white/60 dark:border-zinc-800/60 backdrop-blur-md">
		<h1 class="text-2xl font-bold tracking-tight text-zinc-900 dark:text-zinc-100">Dashboard Settings</h1>
	</div>

	<div class="bg-white/60 dark:bg-zinc-900/60 backdrop-blur-lg rounded-xl border border-white/40 dark:border-zinc-800 shadow-md p-6">
		<form onsubmit={saveSettings} class="space-y-6">
			<div>
				<h3 class="text-lg font-medium text-zinc-900 dark:text-zinc-100 mb-4">Display Preferences</h3>
				
				<div class="space-y-4">
					<div class="flex items-center justify-between">
						<div>
							<label for="autoRefresh" class="text-sm font-medium text-zinc-900 dark:text-zinc-100">Auto-refresh Service Tables</label>
							<p class="text-xs text-zinc-500 dark:text-zinc-400">Automatically poll services like DynamoDB and S3 for changes</p>
						</div>
						<input type="checkbox" id="autoRefresh" bind:checked={formState.autoRefresh} class="h-4 w-4 rounded border-zinc-300 text-zinc-900 focus:ring-zinc-900 dark:border-zinc-600 dark:bg-zinc-700 dark:ring-offset-zinc-800" />
					</div>
					
					<div>
						<label for="refreshInterval" class="block text-sm font-medium text-zinc-900 dark:text-zinc-100">Refresh Interval (seconds)</label>
						<input type="number" id="refreshInterval" bind:value={formState.refreshInterval} min="1" max="60" class="mt-1 block w-full rounded-md border-zinc-300 shadow-sm focus:border-zinc-500 focus:ring-zinc-500 dark:border-zinc-600 dark:bg-zinc-700 dark:text-white sm:text-sm" />
					</div>
				</div>
			</div>

			<div class="pt-6 border-t border-zinc-200 dark:border-zinc-700">
				<h3 class="text-lg font-medium text-zinc-900 dark:text-zinc-100 mb-4">Console Preferences</h3>
				
				<div>
					<label for="maxConsoleEntries" class="block text-sm font-medium text-zinc-900 dark:text-zinc-100">Max Console Entries Limit</label>
					<p class="text-xs text-zinc-500 dark:text-zinc-400 mb-2">Maximum number of API requests to keep in the Live Console buffer</p>
					<input type="number" id="maxConsoleEntries" bind:value={formState.maxConsoleEntries} min="10" max="1000" step="10" class="mt-1 block w-full rounded-md border-zinc-300 shadow-sm focus:border-zinc-500 focus:ring-zinc-500 dark:border-zinc-600 dark:bg-zinc-700 dark:text-white sm:text-sm" />
				</div>
			</div>

			<div class="pt-4 flex justify-end">
				<button type="submit" class="rounded-md bg-zinc-900 px-4 py-2 text-sm font-medium text-white shadow-sm hover:bg-zinc-700 dark:bg-zinc-100 dark:text-zinc-900 dark:hover:bg-zinc-300">
					Save Settings
				</button>
			</div>
		</form>
	</div>
</div>
