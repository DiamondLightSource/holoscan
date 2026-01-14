package main

import (
        "context"
        "flag"
        "fmt"
        "time"

        zmq "github.com/go-zeromq/zmq4"
)

var (
        zmqURI = flag.String(
                "zmq-addr", "", "zmq stream uri to connect to")
)

func main() {
        flag.Parse()

        socket := zmq.NewPull(context.Background())
        if err := socket.Dial(*zmqURI); err != nil {
                panic(fmt.Errorf("error dialing %s: %w", *zmqURI, err))
        }
        var firstBatchMsgReceived time.Time
        var batchBytesReceived float64
        var batchCount uint64 = 100000
        var totalMessagesReceived uint64
        var totalBytesReceived float64
        for {
                msg, err := socket.Recv()
                if err != nil {
                        fmt.Printf("error receiving msg: %v\n", err)
                        continue
                }
                totalMessagesReceived++
                totalBytesReceived += float64(len(msg.Bytes()))
                batchBytesReceived += float64(len(msg.Bytes()))
                
                if firstBatchMsgReceived.IsZero() {
                        firstBatchMsgReceived = time.Now()
                }
                
                if totalMessagesReceived%batchCount == 0 {
                        batchSeconds := time.Since(firstBatchMsgReceived).Seconds()
                        fmt.Printf("msg count: %d, average msg/sec: %.1f, total MB received: %.1f, average MB/sec: %.2f\n", totalMessagesReceived, float64(batchCount)/batchSeconds, (totalBytesReceived/1000000), (batchBytesReceived/1000000)/batchSeconds)
                        firstBatchMsgReceived = time.Now()
                        batchBytesReceived = 0
                }
        }
}

