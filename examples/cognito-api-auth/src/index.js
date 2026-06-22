exports.handler = async (event) => {
    console.log("Received event:", JSON.stringify(event, null, 2));

    // The Cognito claims are available in event.requestContext.authorizer.claims
    const claims = event.requestContext?.authorizer?.claims || {};
    const username = claims['cognito:username'] || 'Unknown User';
    const email = claims['email'] || 'No Email';

    return {
        statusCode: 200,
        headers: {
            "Content-Type": "application/json"
        },
        body: JSON.stringify({
            message: `Hello ${username}!`,
            email: email,
            status: "Success",
            secretData: "This is protected data that requires a valid Cognito token."
        })
    };
};
