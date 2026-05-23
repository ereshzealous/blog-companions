# ---- Stage 1: Generate protobuf + build ----
FROM golang:1.22-alpine AS build
RUN apk add --no-cache protobuf protobuf-dev git
WORKDIR /src

# Install protoc plugins.
ENV GOBIN=/usr/local/bin
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.34.2 \
 && go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.5.1

# Cache deps.
COPY go.mod ./
RUN go mod download || true

COPY . .

# Generate proto stubs into proto/vitalsv1.
RUN mkdir -p proto/vitalsv1 && \
    protoc \
      --go_out=proto/vitalsv1 --go_opt=paths=source_relative \
      --go-grpc_out=proto/vitalsv1 --go-grpc_opt=paths=source_relative \
      -I proto proto/vitals.proto && \
    mv proto/vitalsv1/vitals.pb.go proto/vitalsv1/vitals.pb.go && \
    mv proto/vitalsv1/vitals_grpc.pb.go proto/vitalsv1/vitals_grpc.pb.go

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/device-gateway ./cmd

# ---- Stage 2: Runtime ----
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/device-gateway /device-gateway
EXPOSE 9000
ENTRYPOINT ["/device-gateway"]
