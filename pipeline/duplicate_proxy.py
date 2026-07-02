#!/usr/bin/env python3
"""
Benchmark ZeroMQ tee proxy:

    Detector PUSH bind tcp://*:5555
            |
            v
    This process:
        frontend PULL  -> connects to detector
        backend  PUSH  -> sends to file writer
        capture  PUB   -> publishes to live processor

Example:

    python benchmark_proxy.py \
        --detector tcp://detector-hostname:5555 \
        --writer-bind tcp://*:5556 \
        --live-bind tcp://*:5557 \
        --io-threads 4 \
        --hwm 10000 \
        --sndbuf 0 \
        --rcvbuf 0

The file writer should connect with a PULL socket to tcp://proxy-host:5556.
The live processor should connect with a SUB socket to tcp://proxy-host:5557.
"""

import argparse
import signal
import sys
import time
import zmq


def set_common_socket_options(sock, *, hwm=None, sndbuf=None, rcvbuf=None, linger=0):
    """
    Apply common options before bind/connect.

    hwm is in messages, not bytes.
    sndbuf/rcvbuf are OS socket buffer hints in bytes.
    A value of None means 'do not set'.
    A value of 0 for sndbuf/rcvbuf lets the OS / ZeroMQ default apply.
    """

    if hwm is not None:
        sock.setsockopt(zmq.SNDHWM, hwm)
        sock.setsockopt(zmq.RCVHWM, hwm)

    if sndbuf is not None:
        sock.setsockopt(zmq.SNDBUF, sndbuf)

    if rcvbuf is not None:
        sock.setsockopt(zmq.RCVBUF, rcvbuf)

    sock.setsockopt(zmq.LINGER, linger)


def main():
    parser = argparse.ArgumentParser(
        description="ZeroMQ PULL/PUSH proxy with PUB capture for benchmarking."
    )

    parser.add_argument(
        "--detector",
        default="tcp://localhost:5555",
        help="Detector PUSH endpoint to connect frontend PULL to. "
             "Example: tcp://detector-host:5555",
    )

    parser.add_argument(
        "--writer-bind",
        default="tcp://*:8881",
        help="Endpoint where backend PUSH binds for the file writer to connect. "
             "Example: tcp://*:8881",
    )

    parser.add_argument(
        "--live-bind",
        default="tcp://*:8882",
        help="Endpoint where capture PUB binds for the live processor to connect. "
             "Example: tcp://*:8882",
    )

    parser.add_argument(
        "--io-threads",
        type=int,
        default=4,
        help="ZeroMQ context I/O threads. Must be set before creating sockets.",
    )

    parser.add_argument(
        "--hwm",
        type=int,
        default=10000,
        help="SNDHWM and RCVHWM in messages. Default: 10000.",
    )

    parser.add_argument(
        "--sndbuf",
        type=int,
        default=None,
        help="Kernel send buffer size in bytes. Default: do not set.",
    )

    parser.add_argument(
        "--rcvbuf",
        type=int,
        default=None,
        help="Kernel receive buffer size in bytes. Default: do not set.",
    )

    parser.add_argument(
        "--startup-sleep",
        type=float,
        default=1.0,
        help="Seconds to wait after bind/connect before starting proxy. "
             "Useful for allowing SUB connections/subscriptions to settle.",
    )

    args = parser.parse_args()

    context = zmq.Context(io_threads=args.io_threads)

    # Socket 1: detector -> proxy
    frontend = context.socket(zmq.PULL)
    set_common_socket_options(
        frontend,
        hwm=args.hwm,
        sndbuf=args.sndbuf,
        rcvbuf=args.rcvbuf,
        linger=0,
    )

    # Socket 2: proxy -> file writer
    backend = context.socket(zmq.PUSH)
    set_common_socket_options(
        backend,
        hwm=args.hwm,
        sndbuf=args.sndbuf,
        rcvbuf=args.rcvbuf,
        linger=0,
    )

    # Socket 3: proxy capture -> live processor
    capture = context.socket(zmq.PUSH)
    set_common_socket_options(
        capture,
        hwm=args.hwm,
        sndbuf=args.sndbuf,
        rcvbuf=args.rcvbuf,
        linger=0,
    )

    stopping = False

    def handle_signal(signum, frame):
        nonlocal stopping
        if not stopping:
            stopping = True
            print("\nStopping proxy...", file=sys.stderr)
            context.term()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    try:
        print("Starting ZeroMQ benchmark proxy")
        print(f"  frontend PULL connect: {args.detector}")
        print(f"  backend  PUSH bind:    {args.writer_bind}")
        print(f"  capture  PUSH bind:     {args.live_bind}")
        print(f"  io_threads:            {args.io_threads}")
        print(f"  hwm:                   {args.hwm} messages")
        print(f"  sndbuf:                {args.sndbuf}")
        print(f"  rcvbuf:                {args.rcvbuf}")

        frontend.connect(args.detector)
        backend.bind(args.writer_bind)
        capture.bind(args.live_bind)

        if args.startup_sleep > 0:
            print(f"Waiting {args.startup_sleep:.2f} s before starting proxy...")
            time.sleep(args.startup_sleep)

        print("Proxy running. Press Ctrl+C to stop.")

        # This blocks until the context is terminated.
        zmq.proxy(frontend, backend, capture)

    except KeyboardInterrupt:
        pass

    except zmq.ContextTerminated:
        pass

    finally:
        print("Cleaning up sockets...", file=sys.stderr)

        for sock in (frontend, backend, capture):
            try:
                sock.close(linger=0)
            except Exception:
                pass

        try:
            context.term()
        except Exception:
            pass

        print("Proxy stopped.", file=sys.stderr)


if __name__ == "__main__":
    main()
