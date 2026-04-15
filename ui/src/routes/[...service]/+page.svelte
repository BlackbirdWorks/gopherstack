<script lang="ts">
	import { page } from '$app/state';
	import ServiceCompat from '$lib/components/service-compat.svelte';

	const servicePath = $derived(page.params.service);
	const segments = $derived(servicePath.split('/'));
	const fallbackLabel = $derived(segments.at(-1) ?? servicePath);
</script>

<ServiceCompat {servicePath} {fallbackLabel} />
