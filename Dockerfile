FROM nvidia/cuda:10.0-cudnn7-devel-ubuntu18.04
LABEL Satyalab, satya-group@lists.andrew.cmu.edu

ARG DEBIAN_FRONTEND=noninteractive

# Install build and runtime dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    clinfo \
    curl \
    libgtk-3-0 \
    libsm6 \
    libxext6 \
    libxrender1 \
    python3 \
    python3-pip \
    python3-pyqt5 \
 && apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

#upgrade pip, otherwise tensorflow 1.15.0 will not be found
RUN pip3 install --upgrade pip

# Install Tensorflow and Gabriel's external dependencies
RUN python3 -m pip install --no-cache-dir \
    'gabriel-client==0.0.4' \
    'gabriel-protocol==0.0.2' \
    'gabriel-server==0.0.9' \
    'opencv-python<5' \
    protobuf \
    py-cpuinfo \
    'PyQt5==5.14.0' \
    pyzmq \
    'setuptools==41.0.0' \
    'websockets==8.0.0' \
    zmq 

# You can speed up build slightly by reducing build context with
#     git archive --format=tgz HEAD | docker build -t openscout -
COPY . openscout
WORKDIR openscout/server
RUN python3 -m pip install -r requirements.txt

EXPOSE 5555 9099
ENTRYPOINT ["./entrypoint.sh"]
