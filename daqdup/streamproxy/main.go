package main

import (
	"context"
	"flag"
	"fmt"
	"sync/atomic"
	"time"

	zmq "github.com/go-zeromq/zmq4"
)

func logf(format string, a ...any) {
	fmt.Printf(format+"\n", a...)
}

var (
	zmqURI = flag.String(
		"zmq-recv-addr", "", "zmq stream uri to connect to")
	sendURI1 = flag.String(
		"zmq-send-addr1", "", "zmq stream addr1 to listen to")
	sendURI2 = flag.String(
		"zmq-send-addr2", "", "zmq stream addr2 to listen to")
	maxRetries = flag.Int(
		"max-retries", 2, "maximum number of retries for channel operations")
	initialBackoff = flag.Duration(
		"initial-backoff", 100*time.Microsecond, "initial backoff duration")
	maxBackoff = flag.Duration(
		"max-backoff", 1000*time.Microsecond, "maximum backoff duration")
	qsize1 = flag.Int(
		"qsize1", 500000, "size of the first queue")
	qsize2 = flag.Int(
		"qsize2", 500000, "size of the second queue")
)

// retryWithBackoff performs a channel operation with exponential backoff retry logic
func retryWithBackoff(operation func() bool) bool {
	backoff := *initialBackoff

	for i := 0; i < *maxRetries; i++ {
		if operation() {
			return true
		}

		if i < *maxRetries-1 {
			// logf("Channel operation failed (attempt %d/%d), retrying in %v", i+1, *maxRetries, backoff)
			time.Sleep(backoff)
			backoff = time.Duration(float64(backoff) * 1.1)
			if backoff > *maxBackoff {
				backoff = *maxBackoff
			}
		}
	}

	return false
}

func main() {
	flag.Parse()
	if *zmqURI == "" {
		panic("-zmq-recv-addr must be set")
	}
	if *sendURI1 == "" {
		panic("-zmq-send-addr1 must be set")
	}
	if *sendURI2 == "" {
		panic("-zmq-send-addr2 must be set")
	}
	socket := zmq.NewPull(context.Background())
	if err := socket.Dial(*zmqURI); err != nil {
		panic(fmt.Errorf("error dialing %s: %w", *zmqURI, err))
	}
	sendSocket1 := zmq.NewPush(context.Background())
	if err := sendSocket1.Listen(*sendURI1); err != nil {
		panic(fmt.Errorf("error listening to %s: %w", *sendURI1, err))
	}
	sendSocket2 := zmq.NewPush(context.Background())
	if err := sendSocket2.Listen(*sendURI2); err != nil {
		panic(fmt.Errorf("error listening to %s: %w", *sendURI2, err))
	}
	var totalMessagesReceived, sent1, missed1, sent2, missed2 uint64
	channel1 := make(chan []byte, *qsize1)
	channel2 := make(chan []byte, *qsize2)
	go func() {
		for {
			msgBytes := <-channel1
			if !retryWithBackoff(func() bool {
				err := sendSocket1.Send(zmq.NewMsg(msgBytes))
				if err != nil {
					return false
				}
				atomic.AddUint64(&sent1, 1)
				return true
			}) {
				atomic.AddUint64(&missed1, 1)
			}
		}
	}()
	go func() {
		for {
			msgBytes := <-channel2
			if !retryWithBackoff(func() bool {
				err := sendSocket2.Send(zmq.NewMsg(msgBytes))
				if err != nil {
					return false
				}
				atomic.AddUint64(&sent2, 1)
				return true
			}) {
				atomic.AddUint64(&missed2, 1)
			}
		}
	}()
	go func() {
		for {
			startTime := time.Now()
			atomic.StoreUint64(&totalMessagesReceived, 0)
			time.Sleep(1 * time.Second)
			totalSeconds := time.Since(startTime).Seconds()
			msgCount := atomic.LoadUint64(&totalMessagesReceived)
			logf("received msg count: %d, average msg/sec: %d, sent1: %d, missed1: %d, sent2: %d, missed2: %d",
				msgCount,
				msgCount/uint64(totalSeconds),
				atomic.LoadUint64(&sent1),
				atomic.LoadUint64(&missed1),
				atomic.LoadUint64(&sent2),
				atomic.LoadUint64(&missed2),
			)
		}
	}()
	for {
		msg, err := socket.Recv()
		if err != nil {
			logf("error receiving msg: %v", err)
			time.Sleep(*initialBackoff) // Add small delay on receive error
			continue
		}
		atomic.AddUint64(&totalMessagesReceived, 1)
		msgBytes := msg.Bytes()

		// Try to send to channel1
		select {
		case channel1 <- msgBytes:
			// Message sent successfully
		default:
			atomic.AddUint64(&missed1, 1)
		}

		// Try to send to channel2
		select {
		case channel2 <- msgBytes:
			// Message sent successfully
		default:
			atomic.AddUint64(&missed2, 1)
		}
	}
}
