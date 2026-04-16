import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import EMRServerlessPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getEMRServerlessClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('EMR Serverless Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ applications: [] });
		render(EMRServerlessPage);
		expect(screen.getByText('Amazon EMR Serverless')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ applications: [] });
		render(EMRServerlessPage);
		expect(screen.getByText('Applications')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ applications: [] });
		render(EMRServerlessPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no applications', async () => {
		mockSend.mockResolvedValue({ applications: [] });
		render(EMRServerlessPage);
		await waitFor(() => {
			expect(screen.getByText(/no applications/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded applications', async () => {
		mockSend.mockResolvedValue({
			applications: [{ id: 'app-123', name: 'my-spark-app', type: 'SPARK', state: 'STARTED', releaseLabel: 'emr-6.9.0' }]
		});
		render(EMRServerlessPage);
		await waitFor(() => {
			expect(screen.getByText('my-spark-app')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ applications: [] });
		render(EMRServerlessPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Job Runs tab', () => {
		mockSend.mockResolvedValue({ applications: [] });
		render(EMRServerlessPage);
		expect(screen.getByText('Job Runs')).toBeInTheDocument();
	});

	it('shows Started stat card', () => {
		mockSend.mockResolvedValue({ applications: [] });
		render(EMRServerlessPage);
		expect(screen.getByText('Started')).toBeInTheDocument();
	});
});
