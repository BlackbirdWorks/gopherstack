import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import CloudControlPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getCloudControlClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Cloud Control Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ ResourceDescriptions: [], ResourceRequestStatusSummaries: [] });
		render(CloudControlPage);
		expect(screen.getAllByText('AWS Cloud Control API')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ ResourceDescriptions: [] });
		render(CloudControlPage);
		expect(screen.getAllByText('Resources')[0]).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ ResourceDescriptions: [] });
		render(CloudControlPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValue({ ResourceDescriptions: [], ResourceRequestStatusSummaries: [] });
		render(CloudControlPage);
		await waitFor(() => {
			expect(screen.getAllByText(/no resources found/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ ResourceDescriptions: [] });
		render(CloudControlPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Resource Requests tab', () => {
		mockSend.mockResolvedValue({ ResourceDescriptions: [] });
		render(CloudControlPage);
		expect(screen.getAllByText('Resource Requests')[0]).toBeInTheDocument();
	});

	it('shows resource type selector', () => {
		mockSend.mockResolvedValue({ ResourceDescriptions: [] });
		render(CloudControlPage);
		expect(screen.getByDisplayValue('AWS::S3::Bucket')).toBeInTheDocument();
	});

	it('shows Resource Requests stat', () => {
		mockSend.mockResolvedValue({ ResourceDescriptions: [] });
		render(CloudControlPage);
		expect(screen.getAllByText('Resource Requests')[0]).toBeInTheDocument();
	});
});
