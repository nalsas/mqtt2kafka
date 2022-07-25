# syntax=docker/dockerfile:1

##
## Build
##
FROM golang:1.16-alpine AS build

ENV GOPROXY=https://goproxy.cn,direct
WORKDIR /app

COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o /mqtt2kafka

##
## Deploy
##
FROM alpine:latest

WORKDIR /

COPY --from=build /mqtt2kafka /mqtt2kafka

USER root:root

ENTRYPOINT ["/mqtt2kafka"]
CMD []