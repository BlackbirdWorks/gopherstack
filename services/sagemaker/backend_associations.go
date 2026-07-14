package sagemaker

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

var (
	// ErrAssociationNotFound is returned when an association does not exist.
	ErrAssociationNotFound = awserr.New("ResourceNotFound", awserr.ErrNotFound)
	// ErrAssociationAlreadyExists is returned when an association already exists.
	ErrAssociationAlreadyExists = awserr.New("ResourceInUse", awserr.ErrConflict)
)

// associationKey returns the map key for an association.
func associationKey(sourceArn, destinationArn string) string {
	return sourceArn + "|" + destinationArn
}

// trialComponentKey returns the map key for a trial-component association.
func trialComponentKey(trialName, componentName string) string {
	return trialName + "|" + componentName
}

// AddAssociation creates an association between a source and destination entity in the ML lineage graph.
func (b *InMemoryBackend) AddAssociation(
	ctx context.Context,
	sourceArn, destinationArn, associationType string,
	tags map[string]string,
) (*Association, error) {
	b.mu.Lock("AddAssociation")
	defer b.mu.Unlock()

	if sourceArn == "" {
		return nil, fmt.Errorf("%w: SourceArn is required", ErrValidation)
	}

	if destinationArn == "" {
		return nil, fmt.Errorf("%w: DestinationArn is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	assocStore := b.associationsStore(region)

	key := associationKey(sourceArn, destinationArn)
	if _, ok := assocStore.Get(key); ok {
		return nil, fmt.Errorf(
			"%w: association between %s and %s already exists",
			ErrAssociationAlreadyExists,
			sourceArn,
			destinationArn,
		)
	}

	assocARN := arn.Build(
		"sagemaker",
		region,
		b.accountID,
		fmt.Sprintf("association/%s/%s", sourceArn, destinationArn),
	)

	a := &Association{
		SourceArn:       sourceArn,
		DestinationArn:  destinationArn,
		AssociationType: associationType,
		AssociationArn:  assocARN,
		CreationTime:    time.Now(),
		Tags:            mergeTags(nil, tags),
	}
	assocStore.Put(a)

	return cloneAssociation(a), nil
}

// AssociateTrialComponent associates a trial component with a trial.
func (b *InMemoryBackend) AssociateTrialComponent(
	ctx context.Context,
	trialName, trialComponentName string,
) (*TrialComponentAssociation, error) {
	b.mu.Lock("AssociateTrialComponent")
	defer b.mu.Unlock()

	if trialName == "" {
		return nil, fmt.Errorf("%w: TrialName is required", ErrValidation)
	}

	if trialComponentName == "" {
		return nil, fmt.Errorf("%w: TrialComponentName is required", ErrValidation)
	}

	region := getRegion(ctx, b.region)
	tcaStore := b.trialComponentAssociationsStore(region)

	key := trialComponentKey(trialName, trialComponentName)
	if _, ok := tcaStore.Get(key); ok {
		return nil, fmt.Errorf("%w: trial component %s is already associated with trial %s",
			ErrAssociationAlreadyExists, trialComponentName, trialName)
	}

	trialArn := arn.Build("sagemaker", region, b.accountID, "experiment-trial/"+trialName)
	componentArn := arn.Build(
		"sagemaker",
		region,
		b.accountID,
		"experiment-trial-component/"+trialComponentName,
	)

	assoc := &TrialComponentAssociation{
		TrialName:          trialName,
		TrialComponentName: trialComponentName,
		TrialArn:           trialArn,
		TrialComponentArn:  componentArn,
		CreationTime:       time.Now(),
	}
	tcaStore.Put(assoc)

	return cloneTrialComponentAssociation(assoc), nil
}
