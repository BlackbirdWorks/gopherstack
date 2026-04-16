import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import CodeCommitPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getCodeCommitClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('CodeCommit Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(CodeCommitPage);
		expect(screen.getByText('AWS CodeCommit')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(CodeCommitPage);
		expect(screen.getByText('Repositories')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(CodeCommitPage);
		expect(screen.getByPlaceholderText(/search repositories/i)).toBeInTheDocument();
	});

	it('shows empty state when no repositories', async () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(CodeCommitPage);
		await waitFor(() => {
			expect(screen.getByText(/no repositories/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded repositories', async () => {
		mockSend.mockResolvedValue({
			repositories: [{ repositoryName: 'my-repo', repositoryId: 'abc-123' }]
		});
		render(CodeCommitPage);
		await waitFor(() => {
			expect(screen.getByText('my-repo')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(CodeCommitPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Branches stat card', () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(CodeCommitPage);
		expect(screen.getByText('Branches (selected)')).toBeInTheDocument();
	});

	it('shows detail placeholder when no repo selected', () => {
		mockSend.mockResolvedValue({ repositories: [] });
		render(CodeCommitPage);
		expect(screen.getByText(/select a repository/i)).toBeInTheDocument();
	});
});
