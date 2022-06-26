package main

import (
	"context"
	"fmt"
	"log"
	"testing"
	"time"
)

func TestWorkerPool(t *testing.T) {
	wp := NewWorkerPool()
	wp.Start(context.Background())

	for i := 0; i < 20; i++ {
		wp.AddJob(NewJob(fmt.Sprintf("j%d", i), func() error {
			time.Sleep(time.Duration(2) * time.Second)
			return nil
		}))
	}

	subChan := wp.Subscribe()
	go func() {
		for jr := range subChan {
			log.Printf("SUBSCRIBER MSG: Job(%s), Err(%v)", jr.JobID, jr.Err)
		}
	}()

	time.Sleep(5 * time.Second)
	wp.AddWorkers(2)
	time.Sleep(2 * time.Second)
	wp.AddWorkers(3)
	time.Sleep(5 * time.Second)
	wp.RemoveWorkers(20)

	time.Sleep(30 * time.Second)
}

func TestWorkerPoolGracefulShutDown(t *testing.T) {
	wp := NewWorkerPool()
	wp.Start(context.Background())

	for i := 0; i < 15; i++ {
		_i := i
		wp.AddJob(NewJob(fmt.Sprintf("j%d", _i), func() error {
			time.Sleep(time.Duration(_i) * time.Second)
			return nil
		}))
	}

	subChan := wp.Subscribe()
	go func() {
		for jr := range subChan {
			log.Printf("SUBSCRIBER MSG: Job(%s), Err(%v)", jr.JobID, jr.Err)
		}
	}()

	time.Sleep(5 * time.Second)
	wp.AddWorkers(2)
	time.Sleep(2 * time.Second)
	wp.AddWorkers(3)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err := wp.Stop(ctx)
	log.Printf("%v", err)
}

func TestWorkerPoolForcefulShutDown(t *testing.T) {
	wp := NewWorkerPool()
	wp.Start(context.Background())

	for i := 0; i < 15; i++ {
		_i := i
		wp.AddJob(NewJob(fmt.Sprintf("j%d", _i), func() error {
			time.Sleep(time.Duration(_i) * time.Second)
			return nil
		}))
	}

	subChan := wp.Subscribe()
	go func() {
		for jr := range subChan {
			log.Printf("SUBSCRIBER MSG: Job(%s), Err(%v)", jr.JobID, jr.Err)
		}
	}()

	time.Sleep(5 * time.Second)
	wp.AddWorkers(2)
	time.Sleep(2 * time.Second)
	wp.AddWorkers(4)
	time.Sleep(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()

	err := wp.Stop(ctx)
	log.Printf("%v", err)
}
