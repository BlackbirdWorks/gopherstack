package ecs

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type addressedRunner struct {
	failingRunner
	ip string
}

func (r addressedRunner) TaskAddress(string) string { return r.ip }

// race runs mutate while a goroutine keeps reading the handed-out value.
func race(read, mutate func()) {
	stop := make(chan struct{})

	var wg sync.WaitGroup

	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				read()
			}
		}
	})

	mutate()
	close(stop)
	wg.Wait()
}

func TestHandedOutCopies_NotAliasedToBackgroundMutation(t *testing.T) {
	t.Parallel()

	t.Run("task_runtime_address", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend("123456789012", "us-east-1", addressedRunner{ip: "10.0.0.9"})
		task := &Task{
			TaskArn: "arn:aws:ecs:us-east-1:123456789012:task/c/t1",
			Attachments: []TaskAttachment{
				{Details: []KeyValuePair{{Name: detailPrivateIPv4, Value: "10.0.0.1"}}},
			},
			Containers: []Container{{NetworkInterfaces: []NetworkInterface{{PrivateIpv4Address: "10.0.0.1"}}}},
		}

		b.mu.Lock("test")
		b.tasks.Put(task)
		held := b.taskWithLiveTagsLocked(task)
		b.mu.Unlock()

		race(func() {
			_ = held.Attachments[0].Details[0].Value
			_ = held.Containers[0].NetworkInterfaces[0].PrivateIpv4Address
		}, func() {
			b.mu.Lock("test")
			b.applyRuntimeAddress(task)
			b.mu.Unlock()
		})

		assert.Equal(t, "10.0.0.1", held.Attachments[0].Details[0].Value)
		assert.Equal(t, "10.0.0.1", held.Containers[0].NetworkInterfaces[0].PrivateIpv4Address)
	})

	t.Run("service_deployment_alarms", func(t *testing.T) {
		t.Parallel()

		b := NewInMemoryBackend("123456789012", "us-east-1", failingRunner{})
		sd := &ServiceDeployment{
			ServiceDeploymentArn: "arn:aws:ecs:us-east-1:123456789012:service-deployment/c/s/d1",
			Alarms:               &ServiceDeploymentAlarms{Status: alarmsStatusMonitoring, AlarmNames: []string{"a"}},
			Rollback:             &ServiceDeploymentRollback{Reason: "r"},
		}

		b.mu.Lock("test")
		b.serviceDeployments.Put(sd)
		held := b.enrichServiceDeploymentLocked(sd)
		b.mu.Unlock()

		race(func() {
			_ = held.Alarms.Status
		}, func() {
			b.mu.Lock("test")
			b.completeDeploymentLifecycleLocked(&Service{}, sd, time.Now())
			b.mu.Unlock()
		})

		assert.Equal(t, alarmsStatusMonitoring, held.Alarms.Status)
	})
}
