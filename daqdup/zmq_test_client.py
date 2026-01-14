import time
import zmq
import argparse
import signal
import sys

class ZmqTestClient:
    def __init__(self, zmq_addr):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PULL)
        self.socket.connect(zmq_addr)
        self.running = True
        
        # Statistics
        self.stats = {
            'received': 0,
            'start_time': None,
            'last_print_time': None
        }
        
        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        print("\nShutting down gracefully...")
        self.running = False

    def print_stats(self):
        """Print statistics periodically"""
        current_time = time.time()
        if self.stats['start_time'] is None:
            self.stats['start_time'] = current_time
            self.stats['last_print_time'] = current_time
            return

        elapsed = current_time - self.stats['start_time']
        if current_time - self.stats['last_print_time'] >= 1.0:  # Print every second
            rate = self.stats['received'] / elapsed if elapsed > 0 else 0
            print(f"\rReceived: {self.stats['received']} messages, "
                  f"Rate: {rate:.1f} msg/s", end='')
            self.stats['last_print_time'] = current_time

    def run(self):
        """Main function to receive messages"""
        print(f"ZMQ Test Client is starting...")
        print(f"Connected to {self.socket.getsockopt_string(zmq.LAST_ENDPOINT)}")
        
        while self.running:
            try:
                msg = self.socket.recv(zmq.NOBLOCK)
                self.stats['received'] += 1
            except zmq.Again:
                time.sleep(0.00001)  # Small sleep to prevent CPU spinning
            except Exception as ex:
                if self.running:
                    print(f"Error in receive loop: {ex}")
                break
            self.print_stats()
            
        # Cleanup
        self.socket.close()
        self.context.term()
        print("\nZMQ Test Client has been shut down.")

def main():
    parser = argparse.ArgumentParser(
        description="ZMQ Test Client: Connects to a PUSH socket and receives messages.\n"
        "Used for testing message throughput and reliability.",
    )

    parser.add_argument(
        "--zmq-addr",
        dest="zmq_addr",
        action="store",
        default="tcp://127.0.0.1:5566",
        help="Address of the PUSH socket to connect to, e.g. 'tcp://127.0.0.1:5566'.",
    )

    args = parser.parse_args()

    try:
        client = ZmqTestClient(args.zmq_addr)
        client.run()

    except Exception as ex:
        print(f"Error: {ex}")
        sys.exit(1)

if __name__ == "__main__":
    main()
