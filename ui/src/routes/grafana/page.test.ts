import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import GrafanaPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getGrafanaClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Grafana Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ workspaces: [] });
		render(GrafanaPage);
		expect(screen.getByText('Amazon Managed Grafana')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ workspaces: [] });
		render(GrafanaPage);
		expect(screen.getByText('Workspaces')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ workspaces: [] });
		render(GrafanaPage);
		expect(screen.getByPlaceholderText(/search workspaces/i)).toBeInTheDocument();
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValue({ workspaces: [] });
		render(GrafanaPage);
		await waitFor(() => {
			expect(screen.getByText(/no workspaces/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded workspaces', async () => {
		mockSend.mockResolvedValue({
			workspaces: [{ name: 'my-grafana', id: 'g-123', status: 'ACTIVE', grafanaVersion: '9.4', endpoint: 'g-123.grafana.us-east-1.amazonaws.com', authentication: { providers: ['SAML'] } }]
		});
		render(GrafanaPage);
		await waitFor(() => {
			expect(screen.getByText('my-grafana')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ workspaces: [] });
		render(GrafanaPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Active stat', () => {
		mockSend.mockResolvedValue({ workspaces: [] });
		render(GrafanaPage);
		expect(screen.getByText('Active')).toBeInTheDocument();
	});

	it('shows SAML Enabled stat', () => {
		mockSend.mockResolvedValue({ workspaces: [] });
		render(GrafanaPage);
		expect(screen.getByText('SAML Enabled')).toBeInTheDocument();
	});
});
