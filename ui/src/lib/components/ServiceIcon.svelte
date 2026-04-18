<script lang="ts">
type Props = {
	icon: string;
	label: string;
	class?: string;
};

let { icon, label, class: className = 'w-5 h-5' }: Props = $props();

let failed = $state(false);

// Deterministic color per first letter (A=0 … Z=25)
const COLORS = [
	'#3b82f6', '#8b5cf6', '#10b981', '#f97316', '#ef4444',
	'#14b8a6', '#6366f1', '#ec4899', '#eab308', '#06b6d4',
	'#f59e0b', '#84cc16', '#22c55e', '#0ea5e9', '#7c3aed',
	'#e11d48', '#d946ef', '#64748b', '#78716c', '#71717a',
	'#a8a29e', '#3b82f6', '#8b5cf6', '#10b981', '#f97316', '#ef4444'
];

function colorFor(s: string): string {
	const idx = (s.toUpperCase().codePointAt(0) ?? 0) - 65;
	return COLORS[Math.max(0, Math.min(idx, COLORS.length - 1))];
}

let letter = $derived(label.charAt(0).toUpperCase());
let bg = $derived(colorFor(letter));
</script>

{#if !failed}
	<img
		src={`/dashboard/static/icons/${icon}.svg`}
		alt={label}
		class="{className} flex-shrink-0 rounded-md shadow-sm"
		onerror={() => { failed = true; }}
	/>
{:else}
	<span
		class="{className} flex-shrink-0 rounded-md shadow-sm inline-flex items-center justify-center text-white font-bold select-none"
		style="background:{bg}; font-size:55%;"
		aria-label={label}
	>{letter}</span>
{/if}
