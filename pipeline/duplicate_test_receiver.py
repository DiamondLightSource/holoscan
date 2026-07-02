#!/usr/bin/env python3

import argparse
import time
import zmq


def main():
    parser = argparse.ArgumentParser(
        description="Test receiver for ZeroMQ proxy writer and live endpoints."
    )

    parser.add_argument(
        "--writer-endpoint",
        default="tcp://localhost:8881",
        help="Endpoint of proxy backend PUSH socket. This script connects with PULL.",
    )

    parser.add_argument(
        "--live-endpoint",
        default="tcp://localhost:8882",
        help="Endpoint of proxy capture PUB socket. This script connects with SUB.",
    )

    parser.add_argument(
        "--io-threads",
        type=int,
        default=1,
        help="ZeroMQ context I/O threads.",
    )

    parser.add_argument(
        "--hwm",
        type=int,
        default=10000,
        help="Receive high-water mark in messages.",
    )

    parser.add_argument(
        "--report-every",
        type=int,
        default=1000,
        help="Print summary every N received messages per stream.",
    )

    parser.add_argument(
        "--data-streamed",
        default='both',
        help="Which data stream to receive: 'writer', 'live', or 'both'.",
    )

    args = parser.parse_args()

    context = zmq.Context(io_threads=args.io_threads)
    poller = zmq.Poller()

    if args.data_streamed in ['writer']:
        writer_rx = context.socket(zmq.PULL)
        writer_rx.setsockopt(zmq.RCVHWM, args.hwm)
        writer_rx.setsockopt(zmq.LINGER, 0)
        writer_rx.connect(args.writer_endpoint)
        poller.register(writer_rx, zmq.POLLIN)

        live_rx = None

    if args.data_streamed in ['live']:
        live_rx = context.socket(zmq.SUB)
        live_rx.setsockopt(zmq.RCVHWM, args.hwm)
        live_rx.setsockopt(zmq.LINGER, 0)

        # Subscribe to everything.
        live_rx.setsockopt(zmq.SUBSCRIBE, b"")
        live_rx.connect(args.live_endpoint)
        poller.register(live_rx, zmq.POLLIN)

        writer_rx = None

    if args.data_streamed in ['both']:
        writer_rx = context.socket(zmq.PULL)
        writer_rx.setsockopt(zmq.RCVHWM, args.hwm)
        writer_rx.setsockopt(zmq.LINGER, 0)
        writer_rx.connect(args.writer_endpoint) 

        live_rx = context.socket(zmq.SUB)
        live_rx.setsockopt(zmq.RCVHWM, args.hwm)
        live_rx.setsockopt(zmq.LINGER, 0)

        # Subscribe to everything.
        live_rx.setsockopt(zmq.SUBSCRIBE, b"")
        live_rx.connect(args.live_endpoint)

        poller.register(writer_rx, zmq.POLLIN)
        poller.register(live_rx, zmq.POLLIN)

    writer_count = 0
    writer_bytes = 0
    live_count = 0
    live_bytes = 0

    t0 = time.time()
    last_report = t0

    print("Test receivers running")
    print(f"  writer PULL connected to: {args.writer_endpoint}")
    print(f"  live   SUB  connected to: {args.live_endpoint}")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            events = dict(poller.poll(timeout=1000))

            now = time.time()

            if writer_rx and writer_rx in events:
                msg = writer_rx.recv()
                writer_count += 1
                writer_bytes += len(msg)

                if writer_count % args.report_every == 0:
                    dt = now - t0
                    rate = writer_count / dt if dt > 0 else 0
                    mbps = writer_bytes / dt / 1e6 if dt > 0 else 0
                    print(
                        f"[writer] count={writer_count:,} "
                        f"last_size={len(msg):,} bytes "
                        f"rate={rate:,.1f} msg/s "
                        f"throughput={mbps:,.2f} MB/s"
                    )

            if live_rx and live_rx in events:
                msg = live_rx.recv()
                live_count += 1
                live_bytes += len(msg)

                if live_count % args.report_every == 0:
                    dt = now - t0
                    rate = live_count / dt if dt > 0 else 0
                    mbps = live_bytes / dt / 1e6 if dt > 0 else 0
                    print(
                        f"[live]   count={live_count:,} "
                        f"last_size={len(msg):,} bytes "
                        f"rate={rate:,.1f} msg/s "
                        f"throughput={mbps:,.2f} MB/s"
                    )

            # Optional heartbeat if nothing is arriving.
            if now - last_report > 5:
                print(
                    f"[status] writer={writer_count:,} msgs, "
                    f"live={live_count:,} msgs"
                )
                last_report = now

    except KeyboardInterrupt:
        print("\nStopping...")

    finally:
        if writer_rx:
            writer_rx.close(linger=0)
        if live_rx:
            live_rx.close(linger=0)
        context.term()

        elapsed = time.time() - t0
        print("Final summary:")
        print(f"  elapsed:       {elapsed:.2f} s")
        print(f"  writer msgs:   {writer_count:,}")
        print(f"  live msgs:     {live_count:,}")
        print(f"  writer bytes:  {writer_bytes:,}")
        print(f"  live bytes:    {live_bytes:,}")


if __name__ == "__main__":
    main()