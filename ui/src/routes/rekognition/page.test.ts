import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, waitFor } from '@testing-library/svelte';
import RekognitionPage from './+page.svelte';

const mockSend = vi.fn();

vi.mock('$lib/aws-client', () => ({
	getRekognitionClient: () => ({ send: mockSend })
}));

vi.mock('svelte-sonner', () => ({
	toast: { success: vi.fn(), error: vi.fn(), warning: vi.fn(), info: vi.fn() }
}));

describe('Rekognition Page', () => {
	beforeEach(() => {
		vi.clearAllMocks();
		mockSend.mockReset();
	});

	it('renders page title', () => {
		mockSend.mockResolvedValue({ CollectionIds: [], StreamProcessors: [] });
		render(RekognitionPage);
		expect(screen.getByText('Amazon Rekognition')).toBeInTheDocument();
	});

	it('shows stat cards', () => {
		mockSend.mockResolvedValue({ CollectionIds: [] });
		render(RekognitionPage);
		expect(screen.getByText('Face Collections')).toBeInTheDocument();
	});

	it('shows search input', () => {
		mockSend.mockResolvedValue({ CollectionIds: [] });
		render(RekognitionPage);
		expect(screen.getByPlaceholderText(/search/i)).toBeInTheDocument();
	});

	it('shows empty state when no collections', async () => {
		mockSend.mockResolvedValue({ CollectionIds: [], StreamProcessors: [] });
		render(RekognitionPage);
		await waitFor(() => {
			expect(screen.getByText(/no face collections/i)).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('displays loaded collections', async () => {
		mockSend.mockResolvedValue({
			CollectionIds: ['my-collection'],
			StreamProcessors: []
		});
		render(RekognitionPage);
		await waitFor(() => {
			expect(screen.getByText('my-collection')).toBeInTheDocument();
		}, { timeout: 3000 });
	});

	it('shows refresh button', () => {
		mockSend.mockResolvedValue({ CollectionIds: [] });
		render(RekognitionPage);
		expect(screen.getByTitle('Refresh')).toBeInTheDocument();
	});

	it('shows Stream Processors tab', () => {
		mockSend.mockResolvedValue({ CollectionIds: [] });
		render(RekognitionPage);
		expect(screen.getByText('Stream Processors')).toBeInTheDocument();
	});

	it('shows Indexed Faces stat', () => {
		mockSend.mockResolvedValue({ CollectionIds: [] });
		render(RekognitionPage);
		expect(screen.getByText('Indexed Faces')).toBeInTheDocument();
	});
});
