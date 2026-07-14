package glue

import (
	"context"
)

// createClassifierInput holds input for CreateClassifier.
type createClassifierInput struct {
	GrokClassifier *GrokClassifier `json:"GrokClassifier,omitempty"`
	XMLClassifier  *XMLClassifier  `json:"XMLClassifier,omitempty"`
	JSONClassifier *JSONClassifier `json:"JSONClassifier,omitempty"`
	CsvClassifier  *CsvClassifier  `json:"CsvClassifier,omitempty"`
}

func (h *Handler) handleCreateClassifier(
	_ context.Context,
	in *createClassifierInput,
) (*emptyOutput, error) {
	c := Classifier{
		GrokClassifier: in.GrokClassifier,
		XMLClassifier:  in.XMLClassifier,
		JSONClassifier: in.JSONClassifier,
		CsvClassifier:  in.CsvClassifier,
	}
	if err := h.Backend.CreateClassifier(c); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// deleteClassifierInput holds input for DeleteClassifier.
type deleteClassifierInput struct {
	Name string `json:"Name"`
}

func (h *Handler) handleDeleteClassifier(
	_ context.Context,
	in *deleteClassifierInput,
) (*emptyOutput, error) {
	if err := h.Backend.DeleteClassifier(in.Name); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

// getClassifierInput holds input for GetClassifier.
type getClassifierInput struct {
	Name string `json:"Name"`
}

// getClassifierOutput holds the result for GetClassifier.
type getClassifierOutput struct {
	Classifier *Classifier `json:"Classifier"`
}

func (h *Handler) handleGetClassifier(
	_ context.Context,
	in *getClassifierInput,
) (*getClassifierOutput, error) {
	c, err := h.Backend.GetClassifier(in.Name)
	if err != nil {
		return nil, err
	}

	return &getClassifierOutput{Classifier: c}, nil
}

// getClassifiersInput holds input for GetClassifiers.
type getClassifiersInput struct{}

// getClassifiersOutput holds the result for GetClassifiers.
type getClassifiersOutput struct {
	Classifiers []*Classifier `json:"Classifiers"`
}

func (h *Handler) handleGetClassifiers(
	_ context.Context,
	_ *getClassifiersInput,
) (*getClassifiersOutput, error) {
	return &getClassifiersOutput{Classifiers: h.Backend.GetClassifiers()}, nil
}

// updateClassifierInput holds input for UpdateClassifier.
type updateClassifierInput struct {
	GrokClassifier *GrokClassifier `json:"GrokClassifier,omitempty"`
	XMLClassifier  *XMLClassifier  `json:"XMLClassifier,omitempty"`
	JSONClassifier *JSONClassifier `json:"JSONClassifier,omitempty"`
	CsvClassifier  *CsvClassifier  `json:"CsvClassifier,omitempty"`
}

func (h *Handler) handleUpdateClassifier(
	_ context.Context,
	in *updateClassifierInput,
) (*emptyOutput, error) {
	c := Classifier{
		GrokClassifier: in.GrokClassifier,
		XMLClassifier:  in.XMLClassifier,
		JSONClassifier: in.JSONClassifier,
		CsvClassifier:  in.CsvClassifier,
	}
	if err := h.Backend.UpdateClassifier(c); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}
