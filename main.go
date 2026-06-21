package main

import (
	"log"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"sync"
	"sync/atomic"
	"time"
)

type Job struct {
	ID int
}

var (
	// DATA RACE
	globalCounter int64
	globalStats   = make([]int64, 128)

	// MEMORY LEAK
	leakedData [][]byte
	leakMu     sync.Mutex

	// Просто для логов
	createdJobs atomic.Int64
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	jobs := make(chan Job, 100)

	for i := 0; i < 8; i++ {
		go worker(i, jobs)
	}

	go producer(jobs)

	ticker := time.NewTicker(10 * time.Second)

	for range ticker.C {
		log.Printf(
			"jobs=%d leakedBuffers=%d",
			createdJobs.Load(),
			len(leakedData),
		)
	}
}

func producer(jobs chan<- Job) {
	id := 0

	for {
		id++

		createdJobs.Add(1)

		jobs <- Job{
			ID: id,
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func worker(_ int, jobs <-chan Job) {
	for job := range jobs {
		process(job)
	}
}

func process(job Job) {
	// ===================================
	// DATA RACE
	// ===================================

	globalCounter++

	index := job.ID % len(globalStats)
	globalStats[index]++

	// ===================================
	// MEMORY LEAK
	// ===================================

	if rand.Intn(100) < 30 {
		blob := make([]byte, 64*1024)

		for i := range blob {
			blob[i] = byte(i)
		}

		leakMu.Lock()
		leakedData = append(leakedData, blob)
		leakMu.Unlock()
	}

	// ===================================
	// GOROUTINE + CPU LEAK
	// ===================================

	if rand.Intn(100) < 20 {
		go leakedWorker(job.ID)
	}

	time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
}

func leakedWorker(id int) {
	var x uint64

	for {
		// Немного крутим CPU.

		for i := 0; i < 80000; i++ {
			x += uint64(i)
			x ^= uint64(id)
			x *= 3
		}

		// Ключевой момент:
		// постоянно уступаем планировщику.
		time.Sleep(100 * time.Millisecond)
	}
}
