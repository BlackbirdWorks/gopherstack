const { ApiGatewayManagementApiClient, PostToConnectionCommand } = require('@aws-sdk/client-apigatewaymanagementapi');

exports.handler = async (event) => {
  console.log("Received event:", JSON.stringify(event, null, 2));

  const routeKey = event.requestContext.routeKey;
  const connectionId = event.requestContext.connectionId;
  const domainName = event.requestContext.domainName;
  const stage = event.requestContext.stage;
  
  if (routeKey === '$connect') {
    console.log(`Connection established: ${connectionId}`);
    return { statusCode: 200, body: 'Connected.' };
  }
  
  if (routeKey === '$disconnect') {
    console.log(`Connection closed: ${connectionId}`);
    return { statusCode: 200, body: 'Disconnected.' };
  }
  
  // For $default or specific action (echoing back to the connection)
  const endpoint = process.env.ENDPOINT_URL || `http://host.docker.internal:8000`; // For local Gopherstack testing

  // Using Management API to post back to the connection
  const client = new ApiGatewayManagementApiClient({
    region: 'us-east-1',
    endpoint: endpoint,
    credentials: {
      accessKeyId: 'test',
      secretAccessKey: 'test'
    }
  });

  try {
    let msg = `Echo: ${event.body}`;
    const command = new PostToConnectionCommand({
      ConnectionId: connectionId,
      Data: Buffer.from(msg)
    });

    await client.send(command);
    return { statusCode: 200, body: 'Message sent.' };
  } catch (e) {
    console.error('Failed to post message to connection:', e);
    return { statusCode: 500, body: e.stack };
  }
};
