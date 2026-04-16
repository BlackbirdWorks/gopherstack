import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import BedrockRuntimePage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getBedrockRuntimeClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Bedrock Runtime Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getByText('Amazon Bedrock Runtime')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getByText('Supported Models')).toBeInTheDocument();
	});

	it('shows Invoke Model section', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getByText('Invoke Foundation Model')).toBeInTheDocument();
	});

	it('shows model selector', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getByText('Model')).toBeInTheDocument();
	});

	it('shows prompt textarea', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getByText('Prompt')).toBeInTheDocument();
	});

	it('shows invoke button', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getByText('Invoke Model')).toBeInTheDocument();
	});

	it('shows async invocations tab', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getByText('Async Invocations')).toBeInTheDocument();
	});

	it('shows empty async invocations', async () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		const asyncTab = screen.getByText('Async Invocations');
		asyncTab.click();
		await waitFor(() => {
			expect(screen.getByText(/no async invocations/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ asyncInvokeSummaries: [] });
		render(BedrockRuntimePage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});
});
