import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import ECRPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getECRClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('ECR Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(ECRPage);
		expect(screen.getByText('ECR Repositories')).toBeInTheDocument();
	});

	it('shows Create Repository button', () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(ECRPage);
		expect(screen.getByText('Create Repository')).toBeInTheDocument();
	});

	it('shows empty state when no repos', async () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(ECRPage);
		await waitFor(() => {
			expect(screen.getByText('No repositories found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded repositories', async () => {
		mockSend.mockResolvedValueOnce({
			repositories: [
				{
					repositoryName: 'my-service/api',
					repositoryUri: '123.dkr.ecr.us-east-1.amazonaws.com/my-service/api',
					imageTagMutability: 'MUTABLE',
					createdAt: new Date()
				},
				{
					repositoryName: 'frontend',
					repositoryUri: '123.dkr.ecr.us-east-1.amazonaws.com/frontend',
					imageTagMutability: 'IMMUTABLE',
					createdAt: new Date()
				}
			]
		});
		render(ECRPage);
		await waitFor(() => {
			expect(screen.getByText('my-service/api')).toBeInTheDocument();
		}, { timeout: 3000 });
		expect(screen.getByText('frontend')).toBeInTheDocument();
	});

	it('filters repositories via search', async () => {
		mockSend.mockResolvedValueOnce({
			repositories: [
				{ repositoryName: 'backend-service', repositoryUri: 'uri1', imageTagMutability: 'MUTABLE', createdAt: new Date() },
				{ repositoryName: 'frontend-app', repositoryUri: 'uri2', imageTagMutability: 'MUTABLE', createdAt: new Date() }
			]
		});
		render(ECRPage);
		await waitFor(() => {
			expect(screen.getByText('backend-service')).toBeInTheDocument();
		}, { timeout: 3000 });
		const searchInput = screen.getByPlaceholderText('Search repositories...');
		await fireEvent.input(searchInput, { target: { value: 'frontend' } });
		await waitFor(() => {
			expect(screen.queryByText('backend-service')).not.toBeInTheDocument();
		});
		expect(screen.getByText('frontend-app')).toBeInTheDocument();
	});

	it('opens create repository modal', async () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(ECRPage);
		await fireEvent.click(screen.getByText('Create Repository'));
		await waitFor(() => {
			expect(screen.getByText('Create Repository', { selector: 'h2' })).toBeInTheDocument();
		});
		expect(screen.getByPlaceholderText('e.g. my-service/api')).toBeInTheDocument();
	});

	it('closes create modal on cancel', async () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(ECRPage);
		await fireEvent.click(screen.getByText('Create Repository'));
		await waitFor(() => {
			expect(screen.getByPlaceholderText('e.g. my-service/api')).toBeInTheDocument();
		});
		await fireEvent.click(screen.getByText('Cancel'));
		await waitFor(() => {
			expect(screen.queryByPlaceholderText('e.g. my-service/api')).not.toBeInTheDocument();
		});
	});

	it('shows error toast on load failure', async () => {
		mockSend.mockRejectedValueOnce(new Error('access denied'));
		render(ECRPage);
		const { toast } = await import('svelte-sonner');
		await waitFor(() => {
			expect(vi.mocked(toast.error)).toHaveBeenCalled();
		}, { timeout: 3000 });
	});
});
