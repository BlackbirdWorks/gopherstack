import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/svelte';
import KMSPage from './+page.svelte';

const mockSend = vi.fn();
vi.mock('$lib/aws-client', () => ({ getKMSClient: () => ({ send: mockSend }) }));
vi.mock('svelte-sonner', () => ({ toast: { success: vi.fn(), error: vi.fn(), info: vi.fn() } }));

const mockKey = { KeyId: 'aaaa-bbbb-cccc', KeyArn: 'arn:aws:kms:us-east-1:123:key/aaaa-bbbb-cccc' };
const mockKeyMeta = { KeyId: 'aaaa-bbbb-cccc', Description: 'My key', KeyState: 'Enabled', KeyUsage: 'ENCRYPT_DECRYPT', CreationDate: new Date() };

describe('KMS Page', () => {
beforeEach(() => { vi.clearAllMocks(); mockSend.mockReset(); });

it('renders page title', () => {
mockSend.mockResolvedValue({ Keys: [] });
render(KMSPage);
expect(screen.getByText('KMS Keys')).toBeInTheDocument();
});

it('shows Create Key button', () => {
mockSend.mockResolvedValue({ Keys: [] });
render(KMSPage);
expect(screen.getByText('Create Key')).toBeInTheDocument();
});

it('shows empty state', async () => {
mockSend.mockResolvedValue({ Keys: [] });
render(KMSPage);
await waitFor(() => {
expect(screen.getByText('No KMS keys found')).toBeInTheDocument();
}, { timeout: 3000 });
});

it('loads and displays keys', async () => {
mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
render(KMSPage);
await waitFor(() => {
expect(screen.getByText('My key')).toBeInTheDocument();
}, { timeout: 3000 });
});

it('shows key state badge', async () => {
mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
render(KMSPage);
await waitFor(() => {
expect(screen.getByText('Enabled')).toBeInTheDocument();
}, { timeout: 3000 });
});

it('filters keys by search', async () => {
const key2 = { KeyId: 'xxxx', KeyArn: 'arn:aws:kms:us-east-1:123:key/xxxx' };
const meta2 = { KeyId: 'xxxx', Description: 'Other key', KeyState: 'Enabled', KeyUsage: 'SIGN_VERIFY' };
mockSend.mockResolvedValueOnce({ Keys: [mockKey, key2] });
mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
mockSend.mockResolvedValueOnce({ KeyMetadata: meta2 });
render(KMSPage);
await waitFor(() => expect(screen.getByText('My key')).toBeInTheDocument(), { timeout: 3000 });
const searchInput = screen.getByPlaceholderText(/search/i);
await fireEvent.input(searchInput, { target: { value: 'Other' } });
await waitFor(() => expect(screen.queryByText('My key')).not.toBeInTheDocument());
expect(screen.getByText('Other key')).toBeInTheDocument();
});

it('shows error toast on load failure', async () => {
mockSend.mockRejectedValueOnce(new Error('access denied'));
render(KMSPage);
const { toast } = await import('svelte-sonner');
await waitFor(() => expect(vi.mocked(toast.error)).toHaveBeenCalled(), { timeout: 3000 });
});

it('shows key usage type', async () => {
mockSend.mockResolvedValueOnce({ Keys: [mockKey] });
mockSend.mockResolvedValueOnce({ KeyMetadata: mockKeyMeta });
render(KMSPage);
await waitFor(() => {
expect(screen.getByText('ENCRYPT_DECRYPT')).toBeInTheDocument();
}, { timeout: 3000 });
});
});
