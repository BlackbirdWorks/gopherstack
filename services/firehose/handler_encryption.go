package firehose

import "context"

type startDeliveryStreamEncryptionInput struct {
	EncryptionConfig   *EncryptionConfigInput `json:"DeliveryStreamEncryptionConfigurationInput,omitempty"`
	DeliveryStreamName string                 `json:"DeliveryStreamName"`
}

type startDeliveryStreamEncryptionOutput struct{}

func (h *Handler) handleStartDeliveryStreamEncryption(
	ctx context.Context,
	in *startDeliveryStreamEncryptionInput,
) (*startDeliveryStreamEncryptionOutput, error) {
	cfgInput := in.EncryptionConfig
	if err := h.Backend.StartDeliveryStreamEncryption(ctx, in.DeliveryStreamName, cfgInput); err != nil {
		return nil, err
	}

	return &startDeliveryStreamEncryptionOutput{}, nil
}

type stopDeliveryStreamEncryptionOutput struct{}

func (h *Handler) handleStopDeliveryStreamEncryption(
	ctx context.Context,
	in *deliveryStreamNameInput,
) (*stopDeliveryStreamEncryptionOutput, error) {
	if err := h.Backend.StopDeliveryStreamEncryption(ctx, in.DeliveryStreamName); err != nil {
		return nil, err
	}

	return &stopDeliveryStreamEncryptionOutput{}, nil
}
