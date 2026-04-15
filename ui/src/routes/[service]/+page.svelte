<script lang="ts">
	import { page } from '$app/state';
	import ServiceCompat from '$lib/components/service-compat.svelte';
	import { sidebarCategories } from '$lib/nav';

	const serviceId = $derived(page.params.service);
	const allRoutes = $derived(sidebarCategories.flatMap((c) => c.routes));
	const route = $derived(allRoutes.find((r) => r.id === serviceId));
	const label = $derived(route?.label ?? serviceId);
</script>

<ServiceCompat servicePath={serviceId} fallbackLabel={label} />
