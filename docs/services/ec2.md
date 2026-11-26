# EC2

In-memory EC2 control plane covering instances, networking, storage, images, fleets, IPAM and more. By default instances are metadata records. With the Docker provider each instance is a real container.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

VPC=$(aws ec2 create-vpc --cidr-block 10.0.0.0/16 --query Vpc.VpcId --output text)
SUBNET=$(aws ec2 create-subnet --vpc-id "$VPC" --cidr-block 10.0.1.0/24 \
    --query Subnet.SubnetId --output text)
SG=$(aws ec2 create-security-group --group-name my-sg --description "My SG" \
    --vpc-id "$VPC" --query GroupId --output text)

aws ec2 run-instances --image-id ami-12345678 --instance-type t3.micro \
    --min-count 1 --max-count 1 --subnet-id "$SUBNET" --security-group-ids "$SG"
aws ec2 describe-instances
aws ec2 terminate-instances --instance-ids i-00000001
```

## Supported operations

The service handles several hundred EC2 operations. Areas:

| Area | Covers |
|------|--------|
| Instances | run, describe, start, stop, reboot, terminate, attributes, instance types, status, scheduled instances, instance event windows |
| Images and key pairs | AMIs (register, copy, create, deregister, attributes), key pairs, VM import and export |
| Storage | EBS volumes, snapshots, volume attachment and modification |
| VPC | VPCs, subnets, route tables, internet and NAT gateways, network ACLs, DHCP options, prefix lists, VPC CIDR associations, flow logs, encryption control |
| Security | security groups and rules, network interfaces |
| Addresses | Elastic IPs, IP pools, BYOIP |
| Connectivity | VPC endpoints and endpoint services, VPC peering, VPN gateways and connections, Client VPN, Transit Gateway (attachments, peering, route tables, multicast), carrier and local gateways |
| Compute capacity | launch templates, EC2 Fleet, Spot Fleet, Spot requests, placement groups, capacity reservations, capacity blocks and Capacity Manager, reserved instances, dedicated and Mac hosts |
| Networking tools | Traffic Mirroring, Network Insights, Route Server, Verified Access, IPAM |
| Account | tags, account attributes, regions and availability zones |

## Configuration

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--ec2-provider` | `EC2_PROVIDER` | `inmemory` | `inmemory` for metadata only, `docker` to launch containers as instances |
| `--ec2-docker-image` | `EC2_DOCKER_IMAGE` | `amazonlinux:2` | Image the Docker provider launches instances from |
| `--ec2-docker-network` | `EC2_DOCKER_NETWORK` | daemon default bridge | Docker network for instance containers |
| `--ec2-docker-flags` | `GOPHERSTACK_EC2_DOCKER_FLAGS` | none | Extra `docker run` options: `--privileged`, `-e`, `-v`, `--cap-add`, `--cap-drop`, `-m`, `--cpus`, `--network`, `-p`. Others are rejected at startup |
| `--ec2-docker-ssh-host-ip` | `EC2_DOCKER_SSH_HOST_IP` | `127.0.0.1` | Host IP that mapped SSH ports bind to. `0.0.0.0` exposes them externally |
| `--ec2-docker-ssh-port-min`, `--ec2-docker-ssh-port-max` | `EC2_DOCKER_SSH_PORT_MIN`, `EC2_DOCKER_SSH_PORT_MAX` | `0` | Host port range for SSH. `0` lets Docker pick |
| `--ec2-docker-ebs-volumes` | `GOPHERSTACK_EC2_DOCKER_EBS_VOLUMES` | `false` | Map EBS volumes to Docker volumes mounted at `/mnt/ebs/<device>` |
| none | `EC2_JANITOR_INTERVAL` | `1m` | Janitor tick |
| none | `EC2_TERMINATED_TTL` | `1h` | How long terminated instances stay listed |
| none | `EC2_CANCELLED_SPOT_TTL` | `6h` | How long cancelled spot requests stay listed |

## Differences from AWS

> [!WARNING]
> With the default `inmemory` provider no compute runs. Security group rules never filter traffic between instances, even with the Docker provider.

- The in-memory provider accepts any well-formed `InstanceType` and any `ami-` prefixed `ImageId`. The Docker provider rejects an unknown `ImageId` with `InvalidAMIID.NotFound`.
- Docker provider: Windows and Bottlerocket AMIs run the default image. AMI `Architecture` does not choose a platform-specific image. Deregistering an AMI does not remove its committed image. `CreateImage` captures the container filesystem only, not attached EBS volumes.
- Docker EBS volumes: data attached to a running instance is copied in at attach and back at detach or terminate, because Docker cannot add a mount to a live container. The copy registry is lost on restart.
- Snapshot and AMI sharing is account-aware only for resources created by an explicit-account caller. Instances and volumes are not account-scoped.
- `DescribeApplicationStatus` never reports a real status or transition: instances have no application to health-check.
