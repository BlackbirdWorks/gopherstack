import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import SageMakerPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getSageMakerClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('SageMaker Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ NotebookInstances: [], TrainingJobSummaries: [], Models: [], Endpoints: [] });
		render(SageMakerPage);
		expect(screen.getByText('Amazon SageMaker')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ NotebookInstances: [] });
		render(SageMakerPage);
		expect(screen.getByText('Notebooks')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ NotebookInstances: [] });
		render(SageMakerPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no notebooks', async () => {
		mockSend.mockResolvedValue({ NotebookInstances: [], TrainingJobSummaries: [], Models: [], Endpoints: [] });
		render(SageMakerPage);
		await waitFor(() => {
			expect(screen.getByText(/no notebook instances/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded notebooks', async () => {
		mockSend.mockResolvedValue({
			NotebookInstances: [{ NotebookInstanceName: 'my-notebook', NotebookInstanceStatus: 'InService', InstanceType: 'ml.t3.medium' }],
			TrainingJobSummaries: [], Models: [], Endpoints: []
		});
		render(SageMakerPage);
		await waitFor(() => {
			expect(screen.getByText('my-notebook')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ NotebookInstances: [] });
		render(SageMakerPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Notebooks tab', () => {
		mockSend.mockResolvedValue({ NotebookInstances: [] });
		render(SageMakerPage);
		expect(screen.getAllByText('Notebooks').length).toBeGreaterThanOrEqual(1);
	});

	it('shows Models tab', () => {
		mockSend.mockResolvedValue({ NotebookInstances: [] });
		render(SageMakerPage);
		expect(screen.getByText('Models')).toBeInTheDocument();
	});
});
