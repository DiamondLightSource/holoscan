import asyncio
from typing import Dict
import nats
from functools import partial
from queue import Empty, Queue
from logging import getLogger
from attrs import define, frozen, field
from typing import Dict, Any, Union, List
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError
from nats.js.errors import BadRequestError
import json
import numpy as np
import threading
import os
import time

from enum import Enum #, IntEnum

MAX_Q_SIZE = 100
DEFAULT_NATS_PORT = 6000

# utility classes for internal communication between threads/processes
class message_type(Enum):
    """Internal message types for communicating between threads/processes

    Args:
        Enum (int): Type of message
    """
    EXTERNAL = 1,
    SUBSCRIBE = 2
    UNSUBSCRIBE = 3
    SHUTDOWN = 4


@frozen
class unsubscribe:
    subject: str
    type: message_type = message_type.UNSUBSCRIBE


@frozen
class subscribe:
    subject: str
    type: message_type = message_type.SUBSCRIBE

@frozen
class disconnect:
    type: message_type = message_type.SHUTDOWN


@frozen
class ext_msg:
    subject: str
    payload: field(default=None)
    type: message_type = message_type.EXTERNAL


# handler for incoming messages
async def async_sub_handler(msg: nats.aio.msg.Msg, q: Dict[str,Queue]):
    """Handle incoming messages

    Args:
        msg (NATS message): NATS message type
        q (Queue): Receive queue to push message to
    """
    #logger.debug(f"Message arrived on NATS queue with subject {msg.subject}")
    if msg.subject in q and q[msg.subject].qsize() < MAX_Q_SIZE:
        q[msg.subject].put(msg.data, block=False)


# main class for handling NATS communication
@define
class nats_async():
    """Asynchronous handler of ingress and egress NATS messages. Incoming messages
       are required to be in dictionary format and specify a type and a payload.
    """
    # Mandatory attributes (no default values)
    host: str     # NATS host
    rxq_dict: Dict[str, Queue]    # Queue to pass messages on
    txq: Queue    # Queue to send out NATS
    
    # Attributes with default values
    stream_name: str = "holoscan_data"  # Name of the JetStream stream
    sub: Dict[str, nats.aio.subscription.Subscription] = {}
    nc: None = None     # NATS connection
    js: nats.js.JetStreamContext = None
    logger = getLogger(__name__)

    def __attrs_post_init__(self):
        """Post initialization to add stream name to topic keys"""
        # Create a new dictionary with stream-prefixed keys
        self.rxq_dict = {f"{self.stream_name}.{k}": v for k, v in self.rxq_dict.items()}

    def start_async_loop(self):
        """Starts ths asyncio loop
        """
        self.logger.info("Starting event loop")
        try:
            asyncio.run(self.run())
        except KeyboardInterrupt:
            pass

    async def run(self):
        """Main event loop to do async message processing
        """
        self.logger.info("Connecting to NATS")
        await self.connect()

        self.logger.info("Finished connecting")
        # Main processing loop
        while True:
            try:
                el = self.txq.get_nowait()
                if not hasattr(el, 'type'):
                    self.logger.error("Failed to find message type")
                    continue

                self.logger.debug(f"Got message type {el.type}")
                # Check internal message type
                if el.type == message_type.EXTERNAL:
                    await self._publish(el)
                elif el.type == message_type.SUBSCRIBE:
                    await self._subscribe(el.subject)
                elif el.type == message_type.SHUTDOWN:
                    await self._close()
                else:
                    self.logger.error(
                        f"Invalid message type sent to NATS queue: {el.type} {message_type.SUBSCRIBE}")
            except Empty:
                await asyncio.sleep(0.1)

    def _get_full_subject(self, subject: str) -> str:
        """Get the full subject name with stream prefix"""
        return f"{self.stream_name}.{subject}"

    async def connect(self):
        """Connect to a NATS JetStream host

        Args:
            host (str): Host to connect to, including port
        """
        try:
            self.nc = await nats.connect(servers=self.host)
            self.js = self.nc.jetstream()
            
            # Create holoscan_data stream if it doesn't exist
            max_retries = 5
            retry_count = 0
            while retry_count < max_retries:
                try:
                    await self.js.add_stream(
                        name=self.stream_name,
                        subjects=[f"{self.stream_name}.*"],
                        max_msgs=100,  # Limit total number of messages
                        max_msgs_per_subject=-1,  # No limit per subject
                        retention="workqueue"  # Use workqueue retention policy
                    )
                    self.logger.info(f"Created stream: {self.stream_name}")
                    break  # Success, exit retry loop
                except BadRequestError as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        self.logger.error(f"Failed to create stream after {max_retries} retries: {e}")
                        raise
                    self.logger.warning(f"BadRequestError creating stream, retrying ({retry_count}/{max_retries}): {e}")
                    await asyncio.sleep(3)  # Wait 1 second before retry
                except Exception as e:
                    self.logger.info(f"Stream might already exist: {e}")
                    break  # Other errors, don't retry
                
        except ConnectionRefusedError as e:
            self.logger.error(f"Cannot connect to NATS: {e}")
            raise

    async def _subscribe(self, subject: str):
        """Subscribe to a NATS subject

        Args:
            subject (str): Subject to subscribe to. Can contain wildcards
        """
        full_subject = self._get_full_subject(subject)
        self.logger.info(f"Subscribing to subject {full_subject}")
        handler = partial(async_sub_handler, q=self.rxq_dict)

        # Generate a unique consumer name based on subject and timestamp
        consumer_name = f"{subject.replace('.', '_')}_{int(time.time())}"

        max_retries = 5
        retry_count = 0
        while retry_count < max_retries:
            try:
                # First try to delete any existing consumer for this subject
                print(f"Deleting existing consumer for {subject}")
                try:
                    consumers = await self.js.consumers_info(self.stream_name)
                    # print(f"Consumers: {consumers[0].config.filter_subject}")
                    for consumer in consumers:
                        print(f"Consumer: {consumer.config.filter_subject}")
                        if subject in consumer.config.filter_subject:
                            self.logger.info(f"Deleting existing consumer {consumer.name}")
                            await self.js.delete_consumer(self.stream_name, consumer.name)
                except Exception as e:
                    self.logger.debug(f"No existing consumers to clean up: {e}")

                print(f"Creating new subscription for {subject} with consumer {consumer_name}")
                # Now try to create new subscription with unique consumer name
                self.sub[subject] = await self.js.subscribe(
                    full_subject,
                    cb=handler,
                    durable=consumer_name,  # Use 'durable' instead of 'durable_name'
                    # max_deliver=1,  # Only deliver each message once
                    # queue_group="visualizer"  # Use queue group to distribute messages
                )
                self.logger.info(f"Successfully subscribed to {full_subject} with consumer {consumer_name}")
                break  # Success, exit retry loop
            except BadRequestError as e:
                retry_count += 1
                if retry_count >= max_retries:
                    self.logger.error(f"Failed to subscribe after {max_retries} retries: {e}")
                    raise
                self.logger.warning(f"BadRequestError subscribing to {full_subject}, retrying ({retry_count}/{max_retries}): {e}")
                await asyncio.sleep(3)  # Wait 3 seconds before retry
            except TypeError as e:
                self.logger.error(f"Cannot subscribe to {full_subject} since stream doesn't exist: {e}")
                raise  # Don't retry on TypeError
            except Exception as e:
                self.logger.error(f"Unexpected error subscribing to {full_subject}: {e}")
                raise  # Don't retry on other exceptions

    async def _publish(self, msg: ext_msg):
        """Publish a message to NATS

        Args:
            msg (ext_msg): Message to publish
        """
        try:
            full_subject = self._get_full_subject(msg.subject)
            # Convert numpy array to list if needed
            try:
                payload = msg.payload.tolist()
            # if isinstance(msg.payload, np.ndarray):
                # payload = msg.payload.tolist()
            except:
                payload = msg.payload
                
            # Convert to JSON string
            if not isinstance(payload, (str, bytes)):
                payload = json.dumps(payload)
                
            # Ensure payload is bytes
            if isinstance(payload, str):
                payload = payload.encode()
                
            await self.js.publish(full_subject, payload)
            self.logger.debug(f"Published to {full_subject}")
        except nats.errors.ConnectionClosedError:
            self.logger.error("Failed to send message since NATS connect was closed")
        except Exception as e:
            self.logger.error(f"Failed to publish message: {e}")

    async def _close(self):
        """Close connection to NATS
        """
        self.logger.info('Closing NATS connection')
        try:
            await self.nc.close()
        except nats.errors.ConnectionClosedError:
            pass

    
    def subscribe(self, subject: str):
        """Subscribe to a NATS subject

        Args:
            subject (str): Subject to subscribe to
        """
        self.txq.put_nowait(subscribe(subject))

    def publish(self, subject: str, payload: Any):
        """Publish a message to NATS

        Args:
            subject (str): Subject to publish to    
            payload (Any): Payload to publish
        """
        self.txq.put_nowait(ext_msg(subject, payload))

    def close(self):
        """Close the NATS connection
        """
        self.txq.put_nowait(disconnect())


    def get_rxq(self, subject: str = None) -> Queue:
        """Get the rxq for a specific subject

        Args:
            subject (str, optional): Subject to get queue for. If None, returns the first queue if only one exists.

        Returns:
            Queue: The queue for the specified subject
        """
        if len(self.rxq_dict) == 1:
            return list(self.rxq_dict.values())[0]
        else:
            full_subject = self._get_full_subject(subject)
            return self.rxq_dict[full_subject]


    @property
    def rxq(self):
        """Get the only rxq if there is only one subject
        """
        return self.get_rxq()


def launch_nats_instance(host: str = None,
                         subscribe_subjects: Union[str, List[str]]=None):
    """Launch a nats instance and return the txq and rxq

    Args:
        host (str, optional): Host to connect to. If None, uses environment variable or default.
        subscribe_subjects (Union[str, List[str]]): Subjects to subscribe to

    Returns:
        txq (NatsMsgQueue): Message queue to send messages to nats    
        rxq (Dict[str, Queue]): Rx queue
    """
    if host is None:
        port = os.getenv('NATS_PORT', DEFAULT_NATS_PORT)
        host = f"localhost:{port}"
    
    txq = Queue()
    if subscribe_subjects is None:
        subscribe_subjects = []
    elif isinstance(subscribe_subjects, str):
        subscribe_subjects = [subscribe_subjects]

    rxq = {subject: Queue() for subject in subscribe_subjects}
    
    nats_inst = nats_async(host, rxq, txq)
    t = threading.Thread(target=nats_inst.start_async_loop, daemon=True)
    t.start()

    print(f"{nats_inst.rxq_dict=}")
    
    if subscribe_subjects:
        for subject in subscribe_subjects:
            nats_inst.subscribe(subject)
    return nats_inst