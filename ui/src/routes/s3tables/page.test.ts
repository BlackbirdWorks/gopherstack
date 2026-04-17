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
		expect(screen.getAllByText('Amazon S3 Tables')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ tableBuckets: [] });
		render(S3TablesPage);
		expect(screen.getAllByText('Table Buckets')[0]).toBeInTheDocument();
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
			expect(screen.getAllByText(/no table buckets/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded buckets', async () => {
		mockSend.mockResolvedValue({
			tableBuckets: [{ name: 'my-table-bucket', arn: 'arn:aws:s3tables:us-east-1:123:bucket/my-table-bucket' }]
		});
		render(S3TablesPage);
		await waitFor(() => {
			expect(screen.getAllByText('my-table-bucket')[0]).toBeInTheDocument();
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
		expect(screen.getAllByText('Tables')[0]).toBeInTheDocument();
	});

	it('shows Tables selected stat', () => {
		mockSend.mockResolvedValue({ tableBuckets: [] });
		render(S3TablesPage);
		expect(screen.getAllByText('Tables (selected)')[0]).toBeInTheDocument();
	});
});
