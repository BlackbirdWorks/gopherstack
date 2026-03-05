# EC2 — Elastic Compute Cloud

Stub EC2 implementation covering instance lifecycle, security groups, VPCs, and subnets.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `RunInstances` | Launch EC2 instances (stub records) |
| `DescribeInstances` | List instances with optional filters |
| `TerminateInstances` | Terminate instances |
| `DescribeSecurityGroups` | List security groups |
| `CreateSecurityGroup` | Create a security group |
| `DeleteSecurityGroup` | Delete a security group |
| `DescribeVpcs` | List VPCs |
| `DescribeVpcAttribute` | Get a VPC attribute |
| `DescribeSubnets` | List subnets |
| `CreateVpc` | Create a VPC |
| `CreateSubnet` | Create a subnet |

## AWS CLI Examples

```bash
# Create a VPC
aws --endpoint-url http://localhost:8000 ec2 create-vpc \
    --cidr-block 10.0.0.0/16

# Create a subnet
aws --endpoint-url http://localhost:8000 ec2 create-subnet \
    --vpc-id vpc-00000001 \
    --cidr-block 10.0.1.0/24

# Create a security group
aws --endpoint-url http://localhost:8000 ec2 create-security-group \
    --group-name my-sg \
    --description "My security group" \
    --vpc-id vpc-00000001

# Launch an instance
aws --endpoint-url http://localhost:8000 ec2 run-instances \
    --image-id ami-12345678 \
    --instance-type t3.micro \
    --min-count 1 \
    --max-count 1 \
    --security-group-ids sg-00000001

# Describe instances
aws --endpoint-url http://localhost:8000 ec2 describe-instances

# Terminate an instance
aws --endpoint-url http://localhost:8000 ec2 terminate-instances \
    --instance-ids i-00000001
```

## Known Limitations

- No real compute is performed. Instances are stub records only.
- Auto Scaling, Elastic Load Balancing, and EBS volumes are not implemented.
- Network ACLs, route tables, and internet gateways are not implemented.
- Instance types and AMIs are accepted without validation.
- Security group ingress/egress rules are not enforced.
