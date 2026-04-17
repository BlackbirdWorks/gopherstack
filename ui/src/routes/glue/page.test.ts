import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import GluePage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getGlueClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Glue Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ DatabaseList: [] });
		render(GluePage);
		expect(screen.getByText('AWS Glue')).toBeInTheDocument();
	});

	it('shows catalog/jobs/crawlers/connections tabs', () => {
		mockSend.mockResolvedValue({ DatabaseList: [] });
		render(GluePage);
		expect(screen.getByText('Data Catalog')).toBeInTheDocument();
		expect(screen.getByText('ETL Jobs')).toBeInTheDocument();
		expect(screen.getByText('Crawlers')).toBeInTheDocument();
		expect(screen.getByText('Connections')).toBeInTheDocument();
	});

	it('shows empty state when no databases', async () => {
		mockSend.mockResolvedValue({ DatabaseList: [] });
		render(GluePage);
		await waitFor(() => {
			expect(screen.getByText('No databases found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded databases', async () => {
		mockSend.mockResolvedValue({
			DatabaseList: [
				{ Name: 'analytics_db', Description: 'Analytics database', CreateTime: new Date() },
				{ Name: 'raw_data', Description: 'Raw data lake', CreateTime: new Date() }
			]
		});
		render(GluePage);
		await waitFor(() => {
			expect(screen.getByText('analytics_db')).toBeInTheDocument();
			expect(screen.getByText('raw_data')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('switches to ETL jobs tab', async () => {
		mockSend.mockResolvedValue({ DatabaseList: [] });
		render(GluePage);
		mockSend.mockResolvedValue({ Jobs: [] });
		await fireEvent.click(screen.getByText('ETL Jobs'));
		await waitFor(() => {
			expect(screen.getByText('No ETL jobs found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays jobs in ETL tab', async () => {
		mockSend.mockResolvedValue({ DatabaseList: [] });
		render(GluePage);
		mockSend.mockResolvedValue({
			Jobs: [
				{ Name: 'transform-sales', Command: { Name: 'glueetl' }, Role: 'arn:aws:iam::123:role/glue', CreatedOn: new Date() }
			]
		});
		await fireEvent.click(screen.getByText('ETL Jobs'));
		await waitFor(() => {
			expect(screen.getByText('transform-sales')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('switches to crawlers tab', async () => {
		mockSend.mockResolvedValue({ DatabaseList: [] });
		render(GluePage);
		mockSend.mockResolvedValue({ Crawlers: [] });
		await fireEvent.click(screen.getByText('Crawlers'));
		await waitFor(() => {
			expect(screen.getByText('No crawlers found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ DatabaseList: [] });
		render(GluePage);
		expect(screen.getByPlaceholderText('Search...')).toBeInTheDocument();
	});
});
