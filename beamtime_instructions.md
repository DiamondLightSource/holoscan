# Beamtime instructions

After all the hardware is connected and the ports are configured, do the following:

- Start port forwarding on Hub
```
socat TCP-LISTEN:8080,fork TCP:168.254.254.1:80 &
```
- Start ZMQ proxy on the Hub:
```
cd ./stxm/streamproxy
./streamproxy --zmq-recv-addr=tcp://168.254.254.1:31001 --zmq-send-addr1=tcp://127.0.0.1:5565 --zmq-send-addr2=tcp://127.0.0.1:5566
```

- Test the proxy with client on the Hub or elsewhere:
```
cd ./stxm/streamclient
./streamclient --zmq-addr=tcp://127.0.0.1:5566
```
- if needed, launch two clients connected to different ports. Turn off one of the clients before starting the Holoscan app.
- (optional, if needed): build the container:
```
cd ./stxm/streamclient
docker build ./stxm -t ptycho-holoscan:stxm --network host
```
- To launch the container go to the ./holoscan_pipeline folder and run the docker run command:
```
docker run -it --rm --ipc=host --privileged \
    --runtime=nvidia \
    --gpus all \
    --ulimit memlock=-1 \
    --ulimit stack=67108864 \
    --network host \
    -v "/$pwd/stxm:/workspace/stxm" \
    ptycho-holoscan:stxm
```

- Holoscan app config file is in `holoscan_pipeline/stxm/holoscan_config.yaml`
- Start Holoscan app (pipeline, or consumer):
```
cd ./stxm
python pipeline_stxm_image_only.py
```
- Tuning of data receiving can be done by changing the launch paramters (number of threads, number of decompression operators):
```
python pipeline_stxm_image_only.py --num-decompress-ops 4 --threads 6
```



