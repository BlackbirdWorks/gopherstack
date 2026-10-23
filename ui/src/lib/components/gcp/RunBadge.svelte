<script lang="ts">
	let { tone, label, testId = '' }: { tone: 'ok' | 'failed' | 'pending'; label: string; testId?: string } = $props();

	const TONES = {
		ok: 'bg-green-100 text-green-800 dark:bg-green-950/50 dark:text-green-300',
		failed: 'bg-red-100 text-red-800 dark:bg-red-950/50 dark:text-red-300',
		pending: 'bg-amber-100 text-amber-800 dark:bg-amber-950/50 dark:text-amber-300'
	};
</script>

<span class="inline-block rounded-full px-2 py-0.5 text-xs font-medium {TONES[tone]}" data-testid={testId || undefined}>{label}</span>
