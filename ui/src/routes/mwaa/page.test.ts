import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import MWAAPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getMWAAClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('MWAA Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ Environments: [] });
		render(MWAAPage);
		expect(screen.getByText('Amazon MWAA')).toBeInTheDocument();
	});

	it('shows subtitle', () => {
		mockSend.mockResolvedValue({ Environments: [] });
		render(MWAAPage);
		expect(screen.getByText('Managed Workflows for Apache Airflow')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ Environments: [] });
		render(MWAAPage);
		expect(screen.getByText('Total')).toBeInTheDocument();
		expect(screen.getByText('Available')).toBeInTheDocument();
		expect(screen.getByText('Creating')).toBeInTheDocument();
		expect(screen.getByText('Public Access')).toBeInTheDocument();
	});

	it('shows environments tab', () => {
		mockSend.mockResolvedValue({ Environments: [] });
		render(MWAAPage);
		expect(screen.getByText('Environments')).toBeInTheDocument();
	});

	it('shows empty state when no environments', async () => {
		mockSend.mockResolvedValue({ Environments: [] });
		render(MWAAPage);
		await waitFor(() => {
			expect(screen.getByText('No environments found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ Environments: [] });
		render(MWAAPage);
		expect(screen.getByPlaceholderText('Search environments...')).toBeInTheDocument();
	});

	it('displays loaded environment names', async () => {
		mockSend.mockResolvedValueOnce({ Environments: ['my-airflow-env'] });
		mockSend.mockResolvedValue({
			Environment: {
				Name: 'my-airflow-env',
				Status: 'AVAILABLE',
				AirflowVersion: '2.5.1',
				EnvironmentClass: 'mw1.small',
				WebserverAccessMode: 'PUBLIC_ONLY',
				Arn: 'arn:aws:airflow:us-east-1:123:environment/my-airflow-env'
			}
		});
		render(MWAAPage);
		await waitFor(() => {
			expect(screen.getByText('my-airflow-env')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('selects environment and shows details', async () => {
		const envData = {
			Name: 'dev-airflow',
			Status: 'AVAILABLE',
			AirflowVersion: '2.6.3',
			EnvironmentClass: 'mw1.medium',
			WebserverAccessMode: 'PUBLIC_ONLY',
			WebserverUrl: 'https://dev-airflow.airflow.amazonaws.com',
			Schedulers: 2,
			MinWorkers: 1,
			MaxWorkers: 10,
			DagS3Path: 'dags/',
			Arn: 'arn:aws:airflow:us-east-1:123:environment/dev-airflow',
			CreatedAt: new Date('2024-01-01')
		};
		mockSend.mockResolvedValueOnce({ Environments: ['dev-airflow'] });
		mockSend.mockResolvedValueOnce({ Environment: envData });
		mockSend.mockResolvedValueOnce({ Environment: envData });
		render(MWAAPage);
		await waitFor(() => expect(screen.getByText('dev-airflow')).toBeInTheDocument(), { timeout: 3000 });
		await fireEvent.click(screen.getByText('dev-airflow'));
		await waitFor(() => {
			expect(screen.getByText('Environment ARN')).toBeInTheDocument();
			expect(screen.getByText('Airflow Version')).toBeInTheDocument();
			expect(screen.getByText('Environment Class')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows webserver URL when available', async () => {
		const envData = {
			Name: 'url-env',
			Status: 'AVAILABLE',
			AirflowVersion: '2.5.1',
			EnvironmentClass: 'mw1.small',
			WebserverUrl: 'https://url-env.airflow.amazonaws.com',
			Arn: 'arn:aws:airflow:us-east-1:123:environment/url-env'
		};
		mockSend.mockResolvedValueOnce({ Environments: ['url-env'] });
		mockSend.mockResolvedValueOnce({ Environment: envData });
		mockSend.mockResolvedValueOnce({ Environment: envData });
		render(MWAAPage);
		await waitFor(() => expect(screen.getByText('url-env')).toBeInTheDocument(), { timeout: 3000 });
		await fireEvent.click(screen.getByText('url-env'));
		await waitFor(() => {
			expect(screen.getByText('https://url-env.airflow.amazonaws.com')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows empty detail panel by default', () => {
		mockSend.mockResolvedValue({ Environments: [] });
		render(MWAAPage);
		expect(screen.getByText('Select an environment to view details')).toBeInTheDocument();
	});
});
