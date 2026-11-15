FROM ubuntu:24.04 AS build

RUN apt-get update \
 && apt-get install -y --no-install-recommends g++ libzmq3-dev cppzmq-dev libprotobuf-dev protobuf-compiler nlohmann-json3-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app/build

COPY proto/chat.proto ./
COPY cpp/ ./
RUN protoc --cpp_out=. chat.proto \
 && g++ -std=c++17 -O2 -I. -o bbs-server Server.cpp chat.pb.cc -lzmq -lprotobuf -pthread \
 && g++ -std=c++17 -O2 -I. -o bbs-client Client.cpp chat.pb.cc -lzmq -lprotobuf -pthread

FROM ubuntu:24.04

RUN apt-get update \
 && apt-get install -y --no-install-recommends libzmq5 libprotobuf32t64 \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/build/bbs-server /usr/local/bin/bbs-server
COPY --from=build /app/build/bbs-client /usr/local/bin/bbs-client

CMD ["bbs-server"]
