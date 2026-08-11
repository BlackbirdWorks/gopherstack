<script lang="ts">
	// Shown on every resource row, including global services (IAM, Route53,
	// CloudFront, S3's bucket namespace). This is a filter affordance --
	// which fan-out call produced this row -- not a claim about where AWS
	// actually stores the resource. Global resources pass `region={null}`
	// and render as "global" rather than disappearing or showing a region
	// they don't belong to.
	type Props = { region?: string | null; class?: string };

	let { region = null, class: className = '' }: Props = $props();
</script>

{#if region}
	<span
		class="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-mono font-medium bg-slate-100 dark:bg-slate-800 text-slate-600 dark:text-slate-300 border border-slate-200 dark:border-slate-700 {className}"
		data-testid="region-chip"
	>
		{region}
	</span>
{:else}
	<span
		class="inline-flex items-center px-1.5 py-0.5 rounded text-[10px] font-mono font-medium bg-indigo-50 dark:bg-indigo-950/40 text-indigo-600 dark:text-indigo-400 border border-indigo-200 dark:border-indigo-800 {className}"
		data-testid="region-chip"
		title="Global service -- not tied to a single region"
	>
		global
	</span>
{/if}
