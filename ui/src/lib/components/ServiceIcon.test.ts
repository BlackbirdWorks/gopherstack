import { describe, it, expect } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/svelte';
import ServiceIcon from '$lib/components/ServiceIcon.svelte';

describe('ServiceIcon', () => {
	it('renders an img with the correct src when icon is provided', () => {
		render(ServiceIcon, { props: { icon: 's3', label: 'S3' } });
		const img = screen.getByRole('img', { name: 'S3' });
		expect(img).toBeInTheDocument();
		expect(img).toHaveAttribute('src', '/dashboard/static/icons/s3.svg');
	});

	it('shows fallback letter span when image fails to load', async () => {
		render(ServiceIcon, { props: { icon: 'missing-icon', label: 'Bedrock' } });
		const img = screen.getByRole('img', { name: 'Bedrock' });
		// Simulate image load failure
		await fireEvent.error(img);
		// Image should now be gone and letter fallback shown
		expect(screen.queryByRole('img', { name: 'Bedrock' })).not.toBeInTheDocument();
		expect(screen.getByText('B')).toBeInTheDocument();
	});

	it('fallback shows the first letter of the label', async () => {
		render(ServiceIcon, { props: { icon: 'no-such-icon', label: 'Lambda' } });
		const img = screen.getByRole('img', { name: 'Lambda' });
		await fireEvent.error(img);
		expect(screen.getByText('L')).toBeInTheDocument();
	});

	it('applies custom class to the img', () => {
		render(ServiceIcon, { props: { icon: 's3', label: 'S3', class: 'w-8 h-8' } });
		const img = screen.getByRole('img', { name: 'S3' });
		expect(img.className).toContain('w-8');
		expect(img.className).toContain('h-8');
	});

	it('applies custom class to the fallback span', async () => {
		render(ServiceIcon, { props: { icon: 'missing', label: 'SQS', class: 'w-8 h-8' } });
		const img = screen.getByRole('img', { name: 'SQS' });
		await fireEvent.error(img);
		const span = screen.getByText('S');
		expect(span.className).toContain('w-8');
		expect(span.className).toContain('h-8');
	});

	it('uses default class when none provided', () => {
		render(ServiceIcon, { props: { icon: 'iam', label: 'IAM' } });
		const img = screen.getByRole('img', { name: 'IAM' });
		expect(img.className).toContain('w-5');
		expect(img.className).toContain('h-5');
	});

	it('renders uppercase first letter in fallback', async () => {
		render(ServiceIcon, { props: { icon: 'none', label: 'ec2' } });
		const img = screen.getByRole('img', { name: 'ec2' });
		await fireEvent.error(img);
		expect(screen.getByText('E')).toBeInTheDocument();
	});
});
