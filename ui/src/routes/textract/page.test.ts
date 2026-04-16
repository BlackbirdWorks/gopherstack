import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import TextractPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getTextractClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Textract Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Adapters: [], DocumentClassificationJobPropertiesList: [] });
		render(TextractPage);
		expect(screen.getAllByText('Amazon Textract')[0]).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ Adapters: [] });
		render(TextractPage);
		expect(screen.getAllByText('Adapters')[0]).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ Adapters: [] });
		render(TextractPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no adapters', async () => {
		mockSend.mockResolvedValue({ Adapters: [], DocumentClassificationJobPropertiesList: [] });
		render(TextractPage);
		await waitFor(() => {
			expect(screen.getAllByText(/no adapters/i)[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded adapters', async () => {
		mockSend.mockResolvedValue({
			Adapters: [{ AdapterId: 'adapt-123', AdapterName: 'my-adapter' }],
			DocumentClassificationJobPropertiesList: []
		});
		render(TextractPage);
		await waitFor(() => {
			expect(screen.getAllByText('adapt-123')[0]).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ Adapters: [] });
		render(TextractPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Classification Jobs tab', () => {
		mockSend.mockResolvedValue({ Adapters: [] });
		render(TextractPage);
		expect(screen.getAllByText('Classification Jobs')[0]).toBeInTheDocument();
	});

	it('shows Succeeded Jobs stat', () => {
		mockSend.mockResolvedValue({ Adapters: [] });
		render(TextractPage);
		expect(screen.getAllByText('Succeeded Jobs')[0]).toBeInTheDocument();
	});
});
