FROM golang:1.27.4@sha256:205c7636d74e0ffbe3d007162df7d74f859f5a8c8322dfb9f907977686c126cf AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -o /out/ ./cmd/demo ./cmd/ledgerd && mkdir /out/data

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/demo /out/ledgerd /
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
ENV DEMO_DIR=/data DEMO_ADDR=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/demo"]
