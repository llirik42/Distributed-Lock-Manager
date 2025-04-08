package main

import (
	"Distributed-Lock-Manager/src/config"
	"Distributed-Lock-Manager/src/key-value"
	"Distributed-Lock-Manager/src/locks"
	"fmt"
	"log"
	"os"
	"time"
)

func doWork(lock *locks.Lock, workDurationMs uint, workerId string) {
	workDuration := time.Millisecond * time.Duration(workDurationMs)

	for {
		// Lock
		if err := lock.Lock(workerId); err != nil {
			log.Printf("Worker \"%s\" didn't lock \"%s\": %s\n", workerId, lock.GetName(), err)
			continue
		}

		log.Printf("Worker \"%s\" has locked \"%s\"\n\n", workerId, lock.GetName())
		log.Printf("Worker \"%s\" is doing some work \n\n", workerId)
		time.Sleep(workDuration)
		log.Printf("Worker \"%s\" has finished work \n\n", workerId)

		// Try unlocking until unlocked successfully
		for {
			if err := lock.Unlock(workerId); err != nil {
				log.Printf("Worker \"%s\" didn't unlock \"%s\": %s\n", workerId, lock.GetName(), err)
				continue
			}
			break
		}

		log.Printf("Worker \"%s\" has unlocked \"%s\"\n\n", workerId, lock.GetName())
	}
}

func main() {
	args := os.Args

	if len(args) != 2 {
		panic("Invalid number of arguments")
	}

	filePath := args[1]

	cfg, err := config.NewConfiguration(filePath)
	if err != nil {
		panic(err)
	}

	client := key_value.NewClient(cfg, 10)

	lockName := "my-lock"
	lock, err := locks.NewLock(&lockName, client, 50)
	if err != nil {
		panic(err)
	}

	log.Printf("Lock \"%s\" is created!\n\n", lock.GetName())

	go doWork(lock, 5000, "worker-1")
	go doWork(lock, 4000, "worker-2")
	go doWork(lock, 3000, "worker-3")

	if _, err := fmt.Scanln(); err != nil {
		return
	}
}
