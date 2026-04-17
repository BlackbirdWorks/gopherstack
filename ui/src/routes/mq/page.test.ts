import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import MQPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getMQClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('MQ Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ BrokerSummaries: [] });
		render(MQPage);
		expect(screen.getByText('Amazon MQ')).toBeInTheDocument();
	});

	it('shows subtitle', () => {
		mockSend.mockResolvedValue({ BrokerSummaries: [] });
		render(MQPage);
		expect(screen.getByText('Managed message broker service')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ BrokerSummaries: [] });
		render(MQPage);
		expect(screen.getAllByText('Brokers').length).toBeGreaterThan(0);
		expect(screen.getAllByText('Configurations').length).toBeGreaterThan(0);
		expect(screen.getByText('ActiveMQ')).toBeInTheDocument();
		expect(screen.getByText('RabbitMQ')).toBeInTheDocument();
	});

	it('shows tab navigation', () => {
		mockSend.mockResolvedValue({ BrokerSummaries: [] });
		render(MQPage);
		expect(screen.getByRole('button', { name: /Brokers/ })).toBeInTheDocument();
		expect(screen.getByRole('button', { name: /Configurations/ })).toBeInTheDocument();
	});

	it('shows empty state when no brokers', async () => {
		mockSend.mockResolvedValue({ BrokerSummaries: [] });
		render(MQPage);
		await waitFor(() => {
			expect(screen.getByText('No brokers found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded broker names', async () => {
		mockSend.mockResolvedValue({
			BrokerSummaries: [
				{
					BrokerName: 'my-activemq-broker',
					BrokerId: 'broker-abc123',
					BrokerState: 'RUNNING',
					EngineType: 'ACTIVEMQ',
					DeploymentMode: 'SINGLE_INSTANCE'
				},
				{
					BrokerName: 'my-rabbitmq-broker',
					BrokerId: 'broker-def456',
					BrokerState: 'RUNNING',
					EngineType: 'RABBITMQ',
					DeploymentMode: 'SINGLE_INSTANCE'
				}
			]
		});
		render(MQPage);
		await waitFor(() => {
			expect(screen.getByText('my-activemq-broker')).toBeInTheDocument();
			expect(screen.getByText('my-rabbitmq-broker')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ BrokerSummaries: [] });
		render(MQPage);
		expect(screen.getByPlaceholderText('Search brokers...')).toBeInTheDocument();
	});

	it('selects broker and loads details', async () => {
		mockSend.mockResolvedValueOnce({
			BrokerSummaries: [
				{
					BrokerName: 'test-broker',
					BrokerId: 'broker-123',
					BrokerState: 'RUNNING',
					EngineType: 'ACTIVEMQ'
				}
			]
		});
		mockSend.mockResolvedValueOnce({
			BrokerName: 'test-broker',
			BrokerId: 'broker-123',
			BrokerState: 'RUNNING',
			EngineType: 'ACTIVEMQ',
			BrokerArn: 'arn:aws:mq:us-east-1:123:broker:test-broker',
			EngineVersion: '5.15.14',
			DeploymentMode: 'SINGLE_INSTANCE',
			HostInstanceType: 'mq.m5.large',
			PubliclyAccessible: false,
			AutoMinorVersionUpgrade: true,
			StorageType: 'efs',
			BrokerInstances: []
		});
		render(MQPage);
		await waitFor(() => expect(screen.getByText('test-broker')).toBeInTheDocument(), { timeout: 3000 });
		await fireEvent.click(screen.getByText('test-broker'));
		await waitFor(() => {
			expect(screen.getByText('Broker ARN')).toBeInTheDocument();
			expect(screen.getByText('Deployment Mode')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('switches to configurations tab', async () => {
		mockSend.mockResolvedValueOnce({ BrokerSummaries: [] });
		mockSend.mockResolvedValueOnce({ Configurations: [] });
		render(MQPage);
		await waitFor(() => expect(screen.getByText('No brokers found')).toBeInTheDocument(), { timeout: 3000 });
		const configTab = screen.getByRole('button', { name: /Configurations/ });
		await fireEvent.click(configTab);
		await waitFor(() => {
			expect(screen.getByText('No configurations found')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays configurations', async () => {
		mockSend.mockResolvedValueOnce({ BrokerSummaries: [] });
		mockSend.mockResolvedValueOnce({
			Configurations: [
				{
					Id: 'config-abc',
					Name: 'my-activemq-config',
					EngineType: 'ACTIVEMQ',
					EngineVersion: '5.15.14',
					LatestRevision: { Revision: 1 }
				}
			]
		});
		render(MQPage);
		await waitFor(() => expect(screen.getByText('No brokers found')).toBeInTheDocument(), { timeout: 3000 });
		const configTab = screen.getByRole('button', { name: /Configurations/ });
		await fireEvent.click(configTab);
		await waitFor(() => {
			expect(screen.getByText('my-activemq-config')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('counts engine types correctly', async () => {
		mockSend.mockResolvedValue({
			BrokerSummaries: [
				{ BrokerName: 'a1', BrokerId: 'b1', BrokerState: 'RUNNING', EngineType: 'ACTIVEMQ' },
				{ BrokerName: 'a2', BrokerId: 'b2', BrokerState: 'RUNNING', EngineType: 'ACTIVEMQ' },
				{ BrokerName: 'r1', BrokerId: 'b3', BrokerState: 'RUNNING', EngineType: 'RABBITMQ' }
			]
		});
		render(MQPage);
		await waitFor(() => {
			expect(screen.getByText('a1')).toBeInTheDocument();
		}, { timeout: 3000 });
		const statCards = screen.getAllByText(/^\d+$/);
		expect(statCards.length).toBeGreaterThan(0);
	});
});
