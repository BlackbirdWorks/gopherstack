package main

import (
	"context"
	"fmt"

	run "cloud.google.com/go/run/apiv2"
	"cloud.google.com/go/run/apiv2/runpb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func grpcOpts(addr string) []option.ClientOption {
	return []option.ClientOption{
		option.WithEndpoint(addr),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}
}

func serviceName() string {
	return "projects/" + project + "/locations/" + location + "/services/" + svcID
}

func newRunClient(ctx context.Context) (*run.ServicesClient, error) {
	return run.NewServicesClient(ctx, grpcOpts(env("RUN_ADDR", "localhost:8126"))...)
}

func removeService(ctx context.Context, c *run.ServicesClient) error {
	op, err := c.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: serviceName()})
	if status.Code(err) == codes.NotFound {
		return nil
	}

	if err != nil {
		return err
	}

	_, err = op.Wait(ctx)

	return err
}

// deployService replaces any previous deployment and returns the new service.
func deployService(ctx context.Context) (*runpb.Service, error) {
	c, err := newRunClient(ctx)
	if err != nil {
		return nil, err
	}
	defer c.Close()

	if err = removeService(ctx, c); err != nil {
		return nil, fmt.Errorf("remove old service: %w", err)
	}

	op, err := c.CreateService(ctx, &runpb.CreateServiceRequest{
		Parent:    "projects/" + project + "/locations/" + location,
		ServiceId: svcID,
		Service: &runpb.Service{Template: &runpb.RevisionTemplate{
			Containers: []*runpb.Container{{
				Image: image,
				Ports: []*runpb.ContainerPort{{ContainerPort: 8080}},
			}},
		}},
	})
	if err != nil {
		return nil, err
	}

	return op.Wait(ctx)
}

func deleteService(ctx context.Context) error {
	c, err := newRunClient(ctx)
	if err != nil {
		return err
	}
	defer c.Close()

	return removeService(ctx, c)
}
