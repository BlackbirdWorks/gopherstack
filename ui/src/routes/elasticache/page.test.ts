import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import ElastiCachePage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getElastiCacheClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: {
		success: vi.fn(),
		error: vi.fn()
	}
}));

describe('ElastiCache Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title and create button', () => {
		mockSend.mockResolvedValueOnce({ CacheClusters: [] });

		render(ElastiCachePage);

		expect(screen.getByText('ElastiCache Clusters')).toBeInTheDocument();
		expect(screen.getByText('Create Cluster')).toBeInTheDocument();
	});

	it('displays loaded clusters', async () => {
		mockSend.mockResolvedValueOnce({
			CacheClusters: [
				{ CacheClusterId: 'session-cache', CacheClusterStatus: 'available', Engine: 'redis' },
				{ CacheClusterId: 'api-cache', CacheClusterStatus: 'available', Engine: 'memcached' }
			]
		});

		render(ElastiCachePage);

		await waitFor(() => {
			expect(screen.getByText('session-cache')).toBeInTheDocument();
		}, { timeout: 3000 });
		expect(screen.getByText('api-cache')).toBeInTheDocument();
	});

	it('filters clusters via search input', async () => {
		mockSend.mockResolvedValueOnce({
			CacheClusters: [
				{ CacheClusterId: 'session-cache', CacheClusterStatus: 'available', Engine: 'redis' },
				{ CacheClusterId: 'product-cache', CacheClusterStatus: 'available', Engine: 'redis' },
				{ CacheClusterId: 'api-cache', CacheClusterStatus: 'available', Engine: 'memcached' }
			]
		});

		render(ElastiCachePage);

		await waitFor(() => {
			expect(screen.getByText('session-cache')).toBeInTheDocument();
		}, { timeout: 3000 });

		const searchInput = screen.getByPlaceholderText('Search clusters...');
		await fireEvent.input(searchInput, { target: { value: 'api' } });

		await waitFor(() => {
			expect(screen.queryByText('session-cache')).not.toBeInTheDocument();
		});
		expect(screen.getByText('api-cache')).toBeInTheDocument();
		expect(screen.queryByText('product-cache')).not.toBeInTheDocument();
	});

	it('opens create modal when button is clicked', async () => {
		mockSend.mockResolvedValueOnce({ CacheClusters: [] });

		render(ElastiCachePage);

		await fireEvent.click(screen.getByText('Create Cluster'));

		expect(screen.getByText('Create Cache Cluster')).toBeInTheDocument();
		expect(screen.getByPlaceholderText('e.g. session-cache')).toBeInTheDocument();
	});

	it('closes create modal on cancel click', async () => {
		mockSend.mockResolvedValueOnce({ CacheClusters: [] });

		render(ElastiCachePage);

		await fireEvent.click(screen.getByText('Create Cluster'));
		expect(screen.getByText('Create Cache Cluster')).toBeInTheDocument();

		await fireEvent.click(screen.getByText('Cancel'));

		await waitFor(() => {
			expect(screen.queryByText('Create Cache Cluster')).not.toBeInTheDocument();
		});
	});

	it('creates a cluster via the modal form', async () => {
		mockSend.mockResolvedValueOnce({ CacheClusters: [] });
		mockSend.mockResolvedValueOnce({ CacheCluster: { CacheClusterId: 'new-cache' } });
		mockSend.mockResolvedValueOnce({
			CacheClusters: [{ CacheClusterId: 'new-cache', CacheClusterStatus: 'creating', Engine: 'redis' }]
		});

		render(ElastiCachePage);

		await fireEvent.click(screen.getByText('Create Cluster'));

		const nameInput = screen.getByPlaceholderText('e.g. session-cache');
		await fireEvent.input(nameInput, { target: { value: 'new-cache' } });

		const form = nameInput.closest('form')!;
		await fireEvent.submit(form);

		await waitFor(() => {
			expect(mockSend).toHaveBeenCalledTimes(3);
		}, { timeout: 3000 });

		const { toast } = await import('svelte-sonner');
		expect(toast.success).toHaveBeenCalled();
	});

	it('selects a cluster to view details', async () => {
		mockSend.mockResolvedValueOnce({
			CacheClusters: [
				{
					CacheClusterId: 'prod-cache',
					CacheClusterStatus: 'available',
					Engine: 'redis',
					NumCacheNodes: 1
				}
			]
		});

		render(ElastiCachePage);

		await waitFor(() => {
			expect(screen.getByText('prod-cache')).toBeInTheDocument();
		}, { timeout: 3000 });

		await fireEvent.click(screen.getByText('prod-cache'));

		await waitFor(() => {
			expect(screen.getAllByText('prod-cache').length).toBeGreaterThan(0);
		}, { timeout: 3000 });
	});

	it('shows empty state when no clusters exist', async () => {
		mockSend.mockResolvedValueOnce({ CacheClusters: [] });

		render(ElastiCachePage);

		await waitFor(() => {
			expect(screen.getByText('No clusters found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows error toast on load failure', async () => {
		mockSend.mockRejectedValueOnce(new Error('access denied'));

		render(ElastiCachePage);

		const { toast } = await import('svelte-sonner');
		await waitFor(() => {
			expect(vi.mocked(toast.error)).toHaveBeenCalled();
		}, { timeout: 3000 });
	});
});
