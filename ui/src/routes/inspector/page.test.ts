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

const criticalFinding = {
	findingArn: 'arn:inspector:finding/1',
	title: 'CVE-2024-9999 critical vuln',
	severity: 'CRITICAL',
	type: 'PACKAGE_VULNERABILITY',
	fixAvailable: 'YES',
	resources: [{ id: 'i-abc123' }]
};
const highFinding = {
	findingArn: 'arn:inspector:finding/2',
	title: 'CVE-2024-1234 in package libssl',
	severity: 'HIGH',
	type: 'PACKAGE_VULNERABILITY',
	fixAvailable: 'NO',
	resources: [{ id: 'i-abc123' }]
};
const medFinding = {
	findingArn: 'arn:inspector:finding/3',
	title: 'Medium severity finding',
	severity: 'MEDIUM',
	type: 'PACKAGE_VULNERABILITY',
	fixAvailable: 'NO',
	resources: [{ id: 'i-xyz' }]
};

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
		mockSend.mockResolvedValueOnce({ findings: [highFinding] });
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

	it('shows 4 severity stat cards', async () => {
		mockSend.mockResolvedValueOnce({ findings: [criticalFinding, highFinding, medFinding] });
		render(InspectorPage);
		await waitFor(() => screen.getByText('CVE-2024-9999 critical vuln'), { timeout: 3000 });
		// Stats use uppercase tracking-wide class
		const cards = document.querySelectorAll('.uppercase.tracking-wide');
		const labels = [...cards].map(c => c.textContent?.trim());
		expect(labels).toContain('Critical');
		expect(labels).toContain('High');
		expect(labels).toContain('Medium');
		expect(labels).toContain('Low');
	});

	it('filters findings by severity', async () => {
		mockSend.mockResolvedValueOnce({ findings: [criticalFinding, highFinding, medFinding] });
		render(InspectorPage);
		await waitFor(() => screen.getByText('CVE-2024-9999 critical vuln'), { timeout: 3000 });
		const severitySelect = screen.getByRole('combobox');
		await fireEvent.change(severitySelect, { target: { value: 'HIGH' } });
		await waitFor(() => {
			expect(screen.queryByText('CVE-2024-9999 critical vuln')).not.toBeInTheDocument();
			expect(screen.getByText('CVE-2024-1234 in package libssl')).toBeInTheDocument();
		});
	});

	it('shows severity breakdown bar when findings exist', async () => {
		mockSend.mockResolvedValueOnce({ findings: [criticalFinding, highFinding] });
		render(InspectorPage);
		await waitFor(() => screen.getByText('CVE-2024-9999 critical vuln'), { timeout: 3000 });
		expect(screen.getByText(/2 total findings/i)).toBeInTheDocument();
	});
});
