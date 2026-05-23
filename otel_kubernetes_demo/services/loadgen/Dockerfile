FROM golang:1.22-alpine AS build
RUN apk add --no-cache protobuf protobuf-dev git
WORKDIR /src

ENV GOBIN=/usr/local/bin
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

COPY go.mod ./
RUN go mod download || true

COPY . .

RUN mkdir -p proto/vitalsv1 && \
    protoc \
      --go_out=proto/vitalsv1 --go_opt=paths=source_relative \
      --go-grpc_out=proto/vitalsv1 --go-grpc_opt=paths=source_relative \
      -I proto proto/vitals.proto

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/loadgen .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/loadgen /loadgen
ENTRYPOINT ["/loadgen"]
