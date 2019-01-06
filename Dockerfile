FROM golang:1.11

RUN apt-get update && apt-get install cmake -y
RUN git clone https://github.com/yahoojapan/NGT.git && cd NGT && mkdir build && cd build && cmake .. && make install && ldconfig
ENV GO111MODULE on
WORKDIR /go/src/github.com/takebayashi/ngt-cluster
ADD *.go go.* ./
RUN go build -o ngt-cluster
