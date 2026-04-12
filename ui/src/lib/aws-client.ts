import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { S3Client } from '@aws-sdk/client-s3';
import { ElastiCacheClient } from '@aws-sdk/client-elasticache';

// When running in dev server, default to localhost:8000. In production (embedded), default to current origin.
const isBrowser = typeof window !== 'undefined';
const defaultEndpoint = isBrowser 
  ? (window.location.port === '5173' || window.location.port !== '' && window.location.port !== '8000') 
     ? 'http://localhost:8000' 
     : window.location.origin 
  : 'http://localhost:8000';

const credentialProvider = {
	accessKeyId: 'test',
	secretAccessKey: 'test'
};

export const getDynamoDBClient = (endpoint: string = defaultEndpoint) => {
	return new DynamoDBClient({
		endpoint,
		region: 'us-east-1',
		credentials: credentialProvider
	});
};

export const getS3Client = (endpoint: string = defaultEndpoint) => {
	return new S3Client({
		endpoint,
		region: 'us-east-1',
		credentials: credentialProvider,
		forcePathStyle: true
	});
};

export const getElastiCacheClient = (endpoint: string = defaultEndpoint) => {
	return new ElastiCacheClient({
		endpoint,
		region: 'us-east-1',
		credentials: credentialProvider
	});
};
