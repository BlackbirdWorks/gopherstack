<script lang="ts">
	import { page } from '$app/state';
	import LegacyServiceFrame from '$lib/components/LegacyServiceFrame.svelte';
	import { sidebarCategories } from '$lib/nav';

	const serviceId = $derived(page.params.service);

	const allRoutes = $derived(sidebarCategories.flatMap((c) => c.routes));

	const route = $derived(allRoutes.find((r) => r.id === serviceId));

	const label = $derived(route?.label ?? serviceId);
	const src = $derived(`/dashboard/${serviceId}`);
</script>

<LegacyServiceFrame title={label} description={`Manage ${label} resources`} {src} />
