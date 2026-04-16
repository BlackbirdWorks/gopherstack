import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import S3TablesPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getS3TablesClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('S3 Tables Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ tableBuckets: [] });
		render(S3TablesPage);
		expect(screen.getByText('Amazon S3 Tables')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ tableBuckets: [] });
		render(S3TablesPage);
		expect(screen.getByText('Table Buckets')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ tableBuckets: [] });
		render(S3TablesPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no buckets', async () => {
		mockSend.mockResolvedValue({ tableBuckets: [] });
		render(S3TablesPage);
		await waitFor(() => {
			expect(screen.getByText(/no table buckets/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded buckets', async () => {
		mockSend.mockResolvedValue({
			tableBuckets: [{ name: 'my-table-bucket', arn: 'arn:aws:s3tables:us-east-1:123:bucket/my-table-bucket' }]
		});
		render(S3TablesPage);
		await waitFor(() => {
			expect(screen.getByText('my-table-bucket')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ tableBuckets: [] });
		render(S3TablesPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Tables tab', () => {
		mockSend.mockResolvedValue({ tableBuckets: [] });
		render(S3TablesPage);
		expect(screen.getByText('Tables')).toBeInTheDocument();
	});

	it('shows Tables selected stat', () => {
		mockSend.mockResolvedValue({ tableBuckets: [] });
		render(S3TablesPage);
		expect(screen.getByText('Tables (selected)')).toBeInTheDocument();
	});
});
