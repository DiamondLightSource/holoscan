# Holoscan pipeline

The ptychography reconstruction app files are located in `./ptycho`.
Ptycho pipeline is under development.

STXM analysis app files are located in `./stxm`.


To build Holoscan container:
```
docker build . -t ptycho-holoscan:stxm --network host
```

To run the container:
```
docker run -it --rm --ipc=host --privileged \
    --runtime=nvidia \
    --gpus all \
    --ulimit memlock=-1 \
    --ulimit stack=67108864 \
    --network host \
    -v ./ptycho:/workspace/ptycho \
    -v ./stxm:/workspace/stxm \
    ptycho-holoscan:stxm
```
The mounting of the folder is done to avoid rebuilding the container on every code change during the development.

The entrypoint at the moment is simply bash. The app consists of several pieces (TODO: add other pieces):

```
cd ../stxm
pipeline_speed.py - Rx speed test
pipeline_stxm.py - STXM analysis
pipeline_stxm_image_only.py - STXM analysis with image only; positions are ommitted. Also, currently this file has optimized data recieving with buffered message ingest and parallel decompression.
```

Prior to running the app, look at the config file `stxm/holoscan_config.yaml` (or other config file, as needed) and make sure the network ports and data formats are correct.

To run the app:
```
cd /workspace/stxm
python pipeline_stxm_image_only.py # or other file as needed
```

# Stream proxy
This is a golang utility that allows to duplicate the data stream from the detector to Holoscan app and to another client.

To launch this utility, run the following command:
```
cd /workspace/stxm/streamproxy
./streamproxy --zmq-recv-addr=tcp://127.0.0.1:5555 --zmq-send-addr1=tcp://127.0.0.1:5566 --zmq-send-addr2=tcp://127.0.0.1:5565
```

where the first address is the address of the detector, the second address is the address of the first client, and the third address is the address of the second client.

There is also a test client that can be used to test the stream proxy. To launch the test client, run the following command:
```
cd /workspace/stxm/streamclient
./streamclient --zmq-addr=tcp://127.0.0.1:5566 # or other address as needed
```

The corresponding folders also contain the code for the test client (one would need to install the golang compiler to build it).

# Profiling the pipeline with nsight systems

When profiling the pipeline, the Linux operating system’s perf_event_paranoid level must be 2 or less. Use the following command to check:
```
cat /proc/sys/kernel/perf_event_paranoid
```
If the output is >2, then do the following to temporarily adjust the paranoid level (note that this has to be done after each reboot):
```
sudo sh -c 'echo 2 >/proc/sys/kernel/perf_event_paranoid'
```

Profile the app:
```
nsys profile -t cuda,nvtx,osrt,python-gil -o report.nsys-rep -f true python3 pipeline_stxm_image_only.py # or other file as needed
```
Useful: add `-d 30` to profile for 30 seconds.

# Simplon API Simulator

this is ran from a separate repo using private binaries.