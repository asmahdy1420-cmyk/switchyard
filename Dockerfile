# syntax=docker/dockerfile:1

FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/switchyard ./cmd/switchyard

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/switchyard /switchyard
COPY examples/switchyard.yaml /etc/switchyard/switchyard.yaml
EXPOSE 8080 9090
USER nonroot:nonroot
ENTRYPOINT ["/switchyard"]
CMD ["-config", "/etc/switchyard/switchyard.yaml"]
