import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import CodeArtifactPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getCodeArtifactClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('CodeArtifact Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ domains: [], repositories: [] });
		render(CodeArtifactPage);
		expect(screen.getAllByText('AWS CodeArtifact')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ domains: [], repositories: [] });
		render(CodeArtifactPage);
		expect(screen.getAllByText('Domains')[0]).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ domains: [], repositories: [] });
		render(CodeArtifactPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no domains', async () => {
		mockSend.mockResolvedValue({ domains: [], repositories: [] });
		render(CodeArtifactPage);
		await waitFor(() => {
			expect(screen.getAllByText(/no domains/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded domains', async () => {
		mockSend.mockResolvedValue({
			domains: [{ name: 'my-domain', owner: '123456789', status: 'Active' }],
			repositories: []
		});
		render(CodeArtifactPage);
		await waitFor(() => {
			expect(screen.getAllByText('my-domain')[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ domains: [], repositories: [] });
		render(CodeArtifactPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Repositories tab', () => {
		mockSend.mockResolvedValue({ domains: [], repositories: [] });
		render(CodeArtifactPage);
		expect(screen.getAllByText('Repositories')[0]).toBeInTheDocument();
	});

	it('shows Packages tab', () => {
		mockSend.mockResolvedValue({ domains: [], repositories: [] });
		render(CodeArtifactPage);
		expect(screen.getAllByText('Packages')[0]).toBeInTheDocument();
	});
});
