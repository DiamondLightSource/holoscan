import time
import zmq
import argparse
import threading
import queue
import signal
import sys
from concurrent.futures import ThreadPoolExecutor

class ZmqProxy:
    def __init__(self, zmq_recv_addr, zmq_send1_addr, zmq_send2_addr):
        self.context = zmq.Context()
        # Central queue for received messages

        self.running = True
        
        # Setup receive socket
        self.sock_recv = self.context.socket(zmq.PULL)
        self.sock_recv.connect(zmq_recv_addr)
        self.sock_recv.setsockopt(zmq.RCVHWM, 1000)

        self.msg_queue1 = queue.Queue(maxsize=10000) # for DAQ
        self.msg_queue2 = queue.Queue(maxsize=500000) # for Holoscan

        # Setup send sockets with higher HWM to accommodate bursts
        self.sock_snd1 = self.context.socket(zmq.PUSH)
        self.sock_snd1.bind(zmq_send1_addr)
        self.sock_snd1.setsockopt(zmq.SNDHWM, 1000)  # Twice the receive HWM
        self.sock_snd1.setsockopt(zmq.SNDTIMEO, 1)

        self.sock_snd2 = self.context.socket(zmq.PUSH)
        self.sock_snd2.bind(zmq_send2_addr)
        self.sock_snd2.setsockopt(zmq.SNDHWM, 1000)  # Twice the receive HWM
        self.sock_snd2.setsockopt(zmq.SNDTIMEO, 1)

        # Setup signal handler for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Statistics
        self.stats = {
            'received': 0,
            'sent1': 0,
            'sent2': 0,
            'dropped1': 0,
            'dropped2': 0,
            'recv_queue_size': 0,
            'start_time': None,
            'last_print_time': None,
            'receive_rate': 0.0,
            'send1_start_time': None,
            'send2_start_time': None,
            'send1_rate': 0.0,
            'send2_rate': 0.0,
            'failed_retries2': 0
        }

    def signal_handler(self, signum, frame):
        print("\nShutting down gracefully...")
        self.running = False

    def receive_messages(self):
        """Thread function to receive messages and put them in the central queue"""
        while self.running:
            try:
                msg = None
                # the first one if a priority queue
                if not self.msg_queue1.full(): # this could be changed to try/except with queue.Full
                    msg = self.sock_recv.recv(zmq.NOBLOCK)    
                    self.stats['received'] += 1
                    self.msg_queue1.put(msg, block=True, timeout=1)
                    self.stats['recv_queue_size'] = self.msg_queue1.qsize()
                    
                    # add to the second queue if it is not full - if it is full, drop the message for that queue
                    try:
                        self.msg_queue2.put(msg, block=True, timeout=1)
                    except queue.Full:
                        self.stats['dropped2'] += 1

                    # Update receive rate
                    current_time = time.time()
                    if self.stats['start_time'] is None:
                        self.stats['start_time'] = current_time
                    elapsed = current_time - self.stats['start_time']
                    if elapsed > 0:
                        self.stats['receive_rate'] = self.stats['received'] / elapsed
                else:
                    time.sleep(0.0001)
                

            except zmq.Again:
                time.sleep(0.0001)  # Small sleep to prevent CPU spinning
                self.stats['recv_queue_size'] = self.msg_queue1.qsize()
            except Exception as ex:
                if self.running:
                    print(f"Error in receive thread: {ex}")
                break
    
    def send_with_retry(self, socket, msg, max_retries=5, initial_delay=0.001, block=True):
        """Helper function to send message with retry logic"""
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                socket.send(msg, int(not block))
                return True
            except Exception as ex:
                if attempt < max_retries - 1:
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                else:
                    return False
        return False

    def send_messages(self, socket, msg_queue, stats_prefix): # Generic send function
        """Thread function to send messages from the queue to specified socket"""
        while self.running:
            try:
                msg = msg_queue.get(block=True, timeout=1)
                
                # Try to send to socket with retry - currently these are fudge values for testing
                max_retries = 3 if stats_prefix == '1' else 10 # 10 is for the second queue
                initial_delay = 0.001 if stats_prefix == '1' else 0.0001 # 0.0001 is for the second queue
                
                if self.send_with_retry(socket, msg, max_retries=max_retries, initial_delay=initial_delay, block=False):
                    self.stats[f'sent{stats_prefix}'] += 1
                    if self.stats[f'send{stats_prefix}_start_time'] is None:
                        self.stats[f'send{stats_prefix}_start_time'] = time.time()
                    else:
                        elapsed = time.time() - self.stats[f'send{stats_prefix}_start_time']
                        if elapsed > 0:
                            self.stats[f'send{stats_prefix}_rate'] = self.stats[f'sent{stats_prefix}'] / elapsed
                else:
                    if stats_prefix == '2':
                        self.stats['failed_retries2'] += 1
                    self.stats[f'dropped{stats_prefix}'] += 1
                    
            except queue.Empty:
                time.sleep(0.001)
            except Exception as ex:
                if self.running:
                    print(f"Error in send thread: {ex}")
                break

    def print_stats(self):
        """Print statistics periodically"""
        while self.running:
            print(f"\rStats - Received: {self.stats['received']} ({self.stats['receive_rate']:.1f} Hz), "
                  f"Sent1: {self.stats['sent1']} ({self.stats['send1_rate']:.1f} Hz), "
                  f"Sent2: {self.stats['sent2']} ({self.stats['send2_rate']:.1f} Hz), "
                  f"Drop1: {self.stats['dropped1']}, Drop2: {self.stats['dropped2']} ({self.stats['failed_retries2']}), "
                  f"RecvQ: {self.stats['recv_queue_size']}", end='')
            time.sleep(1)

    def run(self):
        """Main function to run the proxy"""
        print("0MQ Proxy is starting...")
        
        # Create thread pool
        with ThreadPoolExecutor(max_workers=4) as executor:  # One more worker for stats
            # Start receive thread
            receive_future = executor.submit(self.receive_messages)

            # Start send threads
            send_future1 = executor.submit(self.send_messages, self.sock_snd1, self.msg_queue1, '1')
            send_future2 = executor.submit(self.send_messages, self.sock_snd2, self.msg_queue2, '2')
            
            # Start stats thread
            stats_future = executor.submit(self.print_stats)
            
            print("\n0MQ Proxy is running successfully.")
            
            # Wait for threads to complete
            receive_future.result()
            send_future1.result()
            send_future2.result()
            stats_future.result()

        # Cleanup
        self.sock_recv.close()
        self.sock_snd1.close()
        self.sock_snd2.close()
        self.context.term()
        print("\n0MQ Proxy has been shut down.")

def main():
    def formatter(prog):
        return argparse.RawDescriptionHelpFormatter(prog, max_help_position=20, width=90)

    parser = argparse.ArgumentParser(
        description="0MQ Proxy: Connects to a single PUSH socket and republishes all received messages.\n"
        "to two different PUSH sockets. The proxy is used to distribute messages to multiple consumers.\n",
        formatter_class=formatter,
    )

    parser.add_argument(
        "--zmq-recv-addr",
        dest="zmq_recv_addr",
        action="store",
        default=None,
        help="Address of the remote PUSH socket, e.g. 'tcp://127.0.0.1:5557'.",
    )

    parser.add_argument(
        "--zmq-send-addr1",
        dest="zmq_send_addr1",
        action="store",
        default=None,
        help="Address of the first PUSH socket, e.g. 'tcp://127.0.0.1:5558'.",
    )

    parser.add_argument(
        "--zmq-send-addr2",
        dest="zmq_send_addr2",
        action="store",
        default=None,
        help="Address of the first PUSH socket, e.g. 'tcp://127.0.0.1:5559'.",
    )

    args = parser.parse_args()

    try:
        if args.zmq_recv_addr is None:
            raise ValueError("The address of the remote PUSH socket must be provided.")
        if args.zmq_send_addr1 is None:
            raise ValueError("The address of the first PUSH socket must be provided.")
        if args.zmq_send_addr2 is None:
            raise ValueError("The address of the second PUSH socket must be provided.")

        proxy = ZmqProxy(args.zmq_recv_addr, args.zmq_send_addr1, args.zmq_send_addr2)
        proxy.run()

    except ValueError as ex:
        print(f"Error: {ex}")
        sys.exit(1)

if __name__ == "__main__":
    main()