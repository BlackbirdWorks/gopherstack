// Package lambda provides a mock AWS Lambda service for Gopherstack.
// Supports Image-based (PackageType: Image) and Zip-based (PackageType: Zip) functions.
// Zip functions use runtime-to-base-image mapping to select an AWS ECR base image
// and bind-mount the extracted code at /var/task.
package lambda
