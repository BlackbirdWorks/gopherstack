exports.validate = async (event) => {
  console.log("Validating order:", event);
  
  const amount = event.amount;
  if (!amount || amount <= 0) {
    throw new Error("Invalid order amount");
  }

  // Pass along the event data
  return {
    ...event,
    status: "VALIDATED"
  };
};

exports.processPayment = async (event) => {
  console.log("Processing payment for:", event);

  // Simulate payment processing logic
  // Succeeds ~80% of the time
  const isSuccess = Math.random() < 0.8;
  
  return {
    ...event,
    paymentStatus: isSuccess ? "SUCCESS" : "FAILED"
  };
};

exports.fulfill = async (event) => {
  console.log("Fulfilling order:", event);
  
  return {
    ...event,
    status: "FULFILLED",
    message: "Your order is on the way!"
  };
};

exports.cancel = async (event) => {
  console.log("Canceling order:", event);

  return {
    ...event,
    status: "CANCELED",
    message: "Your order was canceled due to payment failure."
  };
};
