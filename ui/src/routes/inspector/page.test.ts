import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import InspectorPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getInspectorClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn() }
}));

describe('Inspector Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValueOnce({ findings: [] });
		render(InspectorPage);
		expect(screen.getByText('Amazon Inspector')).toBeInTheDocument();
	});

	it('shows tabs', () => {
		mockSend.mockResolvedValueOnce({ findings: [] });
		render(InspectorPage);
		expect(screen.getByText('Findings')).toBeInTheDocument();
		expect(screen.getByText('Coverage')).toBeInTheDocument();
	});

	it('displays findings', async () => {
		mockSend.mockResolvedValueOnce({
			findings: [
				{
					findingArn: 'arn:inspector:finding/1',
					title: 'CVE-2024-1234 in package libssl',
					severity: 'HIGH',
					type: 'PACKAGE_VULNERABILITY',
					fixAvailable: 'ACTIVE',
					resources: [{ id: 'i-abc123' }]
				}
			]
		});

		render(InspectorPage);

		await waitFor(() => {
			expect(screen.getByText('CVE-2024-1234 in package libssl')).toBeInTheDocument();
		});
	});

	it('shows empty state', async () => {
		mockSend.mockResolvedValueOnce({ findings: [] });
		render(InspectorPage);
		await waitFor(() => {
			expect(screen.getByText('No findings found')).toBeInTheDocument();
		});
	});

	it('switches to coverage tab', async () => {
		mockSend
			.mockResolvedValueOnce({ findings: [] })
			.mockResolvedValueOnce({ coveredResources: [] });
		render(InspectorPage);
		await fireEvent.click(screen.getByText('Coverage'));
		await waitFor(() => {
			expect(screen.getByText('No covered resources')).toBeInTheDocument();
		});
	});
});
