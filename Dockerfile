# Build stage: static binary, no cgo, version stamped through ldflags.
FROM golang:1.27-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION} -X main.commit=${COMMIT} -X main.date=${DATE}" \
      -o /out/antwatcher ./cmd/antwatcher

# /data holds the embedded JetStream store and the filesystem archive. It is
# created here, owned by the distroless nonroot user (uid 65532), so a fresh
# named volume inherits writable ownership.
RUN mkdir -p /out/data && chown 65532:65532 /out/data

# Runtime stage: distroless, non-root, nothing but the binary and CA roots.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/antwatcher /antwatcher
COPY --from=build --chown=65532:65532 /out/data /data
COPY antwatcher.example.yml /etc/antwatcher/antwatcher.example.yml

VOLUME ["/data"]
# 8080 webhook listener, 9090 admin listener (bind admin to 0.0.0.0 in the
# container and publish it to the host loopback only).
EXPOSE 8080 9090
USER nonroot:nonroot

ENTRYPOINT ["/antwatcher"]
CMD ["serve", "-config", "/etc/antwatcher/antwatcher.yml"]
