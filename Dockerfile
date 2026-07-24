# Multi-stage, one image per service (pass the service via --build-arg SVC).
# CGO is off — modernc.org/sqlite is pure Go, so the final image is a static
# binary on distroless: a few MB, no libc, nothing to CVE-scan.
#
#   docker build --build-arg SVC=gateway -t orderops-gateway .
FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SVC
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/app ./cmd/${SVC}

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/app /app
# UIs are //go:embed-ed into the gateway binary — no assets to copy.
ENTRYPOINT ["/app"]
