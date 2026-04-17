import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import ElasticTranscoderPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getElasticTranscoderClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Elastic Transcoder Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Pipelines: [], Jobs: [] });
		render(ElasticTranscoderPage);
		expect(screen.getAllByText('Amazon Elastic Transcoder')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ Pipelines: [] });
		render(ElasticTranscoderPage);
		expect(screen.getAllByText('Pipelines')[0]).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ Pipelines: [] });
		render(ElasticTranscoderPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValue({ Pipelines: [], Jobs: [] });
		render(ElasticTranscoderPage);
		await waitFor(() => {
			expect(screen.getAllByText(/no pipelines/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded pipelines', async () => {
		mockSend.mockResolvedValue({
			Pipelines: [{ Name: 'my-pipeline', Id: 'pipe-123', Status: 'Active', InputBucket: 'input-bucket' }],
			Jobs: []
		});
		render(ElasticTranscoderPage);
		await waitFor(() => {
			expect(screen.getAllByText('my-pipeline')[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ Pipelines: [] });
		render(ElasticTranscoderPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Jobs tab', () => {
		mockSend.mockResolvedValue({ Pipelines: [] });
		render(ElasticTranscoderPage);
		expect(screen.getAllByText('Jobs')[0]).toBeInTheDocument();
	});

	it('shows Active stat', () => {
		mockSend.mockResolvedValue({ Pipelines: [] });
		render(ElasticTranscoderPage);
		expect(screen.getAllByText('Active')[0]).toBeInTheDocument();
	});
});
