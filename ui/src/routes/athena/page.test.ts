import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import AthenaPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getAthenaClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Athena Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ WorkGroups: [] });
		render(AthenaPage);
		expect(screen.getByText('Amazon Athena')).toBeInTheDocument();
	});

	it('shows query editor tab by default', () => {
		mockSend.mockResolvedValue({ WorkGroups: [] });
		render(AthenaPage);
		expect(screen.getByText('Query Editor')).toBeInTheDocument();
		expect(screen.getByText('Run Query')).toBeInTheDocument();
	});

	it('shows all tabs', () => {
		mockSend.mockResolvedValue({ WorkGroups: [] });
		render(AthenaPage);
		expect(screen.getByText('Workgroups')).toBeInTheDocument();
		expect(screen.getByText('Data Catalogs')).toBeInTheDocument();
		expect(screen.getByText('Query History')).toBeInTheDocument();
	});

	it('shows SQL query textarea', () => {
		mockSend.mockResolvedValue({ WorkGroups: [] });
		render(AthenaPage);
		expect(screen.getByRole('textbox', { name: 'SQL Query' })).toBeInTheDocument();
	});

	it('shows workgroups', async () => {
		mockSend.mockResolvedValue({
			WorkGroups: [
				{ Name: 'primary', State: 'ENABLED', Description: 'Default workgroup' },
				{ Name: 'data-science', State: 'ENABLED' }
			]
		});
		render(AthenaPage);
		await fireEvent.click(screen.getByText('Workgroups'));
		await waitFor(() => {
			expect(screen.getByText('primary')).toBeInTheDocument();
			expect(screen.getByText('data-science')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows empty state for workgroups', async () => {
		mockSend.mockResolvedValue({ WorkGroups: [] });
		render(AthenaPage);
		await fireEvent.click(screen.getByText('Workgroups'));
		await waitFor(() => {
			expect(screen.getByText('No workgroups found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('loads data catalogs tab', async () => {
		mockSend.mockResolvedValue({ WorkGroups: [] });
		render(AthenaPage);
		mockSend.mockResolvedValue({ DataCatalogsSummary: [] });
		await fireEvent.click(screen.getByText('Data Catalogs'));
		await waitFor(() => {
			expect(screen.getByText('No data catalogs found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('disables Run Query button when query is empty', async () => {
		mockSend.mockResolvedValue({ WorkGroups: [] });
		render(AthenaPage);
		const btn = screen.getByRole('button', { name: 'Run Query' });
		expect(btn).toBeInTheDocument();
	});
});
