import { describe, it, expect, vi } from 'vitest';
import { render, screen } from '@testing-library/svelte';
import GlacierPage from './+page.svelte';

vi.mock('svelte-sonner', () => ({
  toast: { success: vi.fn(), error: vi.fn() }
}));

describe('Glacier Page', () => {
  it('renders page title', () => {
    render(GlacierPage);
    expect(screen.getByText('Amazon S3 Glacier')).toBeInTheDocument();
  });

  it('shows info banner', () => {
    render(GlacierPage);
    expect(screen.getByText('Managed via Amazon S3')).toBeInTheDocument();
  });

  it('shows storage classes', () => {
    render(GlacierPage);
    expect(screen.getByText('Glacier Storage Classes')).toBeInTheDocument();
  });

  it('shows instant retrieval class', () => {
    render(GlacierPage);
    expect(screen.getByText('S3 Glacier Instant Retrieval')).toBeInTheDocument();
  });

  it('shows flexible retrieval class', () => {
    render(GlacierPage);
    expect(screen.getByText('S3 Glacier Flexible Retrieval')).toBeInTheDocument();
  });

  it('shows deep archive class', () => {
    render(GlacierPage);
    expect(screen.getByText('S3 Glacier Deep Archive')).toBeInTheDocument();
  });
});
