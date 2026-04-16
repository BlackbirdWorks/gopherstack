import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import TimestreamPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getTimestreamWriteClient: () => ({ send: mockSend }),
	getTimestreamQueryClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Timestream Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Databases: [], ScheduledQueries: [] });
		render(TimestreamPage);
		expect(screen.getByText('Amazon Timestream')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ Databases: [] });
		render(TimestreamPage);
		expect(screen.getByText('Databases')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ Databases: [] });
		render(TimestreamPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no databases', async () => {
		mockSend.mockResolvedValue({ Databases: [], ScheduledQueries: [] });
		render(TimestreamPage);
		await waitFor(() => {
			expect(screen.getByText(/no databases/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded databases', async () => {
		mockSend.mockResolvedValue({
			Databases: [{ DatabaseName: 'my-tsdb', TableCount: 5 }],
			ScheduledQueries: []
		});
		render(TimestreamPage);
		await waitFor(() => {
			expect(screen.getByText('my-tsdb')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ Databases: [] });
		render(TimestreamPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Tables tab', () => {
		mockSend.mockResolvedValue({ Databases: [] });
		render(TimestreamPage);
		expect(screen.getByText('Tables')).toBeInTheDocument();
	});

	it('shows Scheduled Queries tab', () => {
		mockSend.mockResolvedValue({ Databases: [] });
		render(TimestreamPage);
		expect(screen.getByText('Scheduled Queries')).toBeInTheDocument();
	});
});
