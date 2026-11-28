FROM golang:1.27.4@sha256:5f1b98fa02fe396ebce2a0227a0e08309e11792915b9dd2e1f71053d7e7e6666 AS build
ARG REPO=https://github.com/Demonisreal/dmn-ledger
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-X github.com/Demonisreal/dmn-ledger/internal/demo.repo=${REPO}" -o /out/ ./cmd/demo ./cmd/ledgerd && mkdir /out/data

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab
COPY --from=build /out/demo /out/ledgerd /
COPY --from=build --chown=65532:65532 /out/data /data
USER 65532:65532
ENV DEMO_DIR=/data DEMO_ADDR=0.0.0.0:8080
EXPOSE 8080
ENTRYPOINT ["/demo"]
