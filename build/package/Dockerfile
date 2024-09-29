FROM golang:1.23-alpine AS build-stage

WORKDIR /app

RUN apk add make

COPY ./ ./
RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux make

# ---

FROM alpine:3.20

WORKDIR /

RUN apk add --no-cache iputils ca-certificates net-snmp-tools procps lm_sensors tzdata su-exec libcap && \
    update-ca-certificates

ENV TELEGRAF_VERSION=1.32.0-megaease

RUN mkdir -p /etc/telegraf /var/lib/telegraf && \
    addgroup -S telegraf && adduser -S -G telegraf telegraf

EXPOSE 8125/udp 8092/udp 8094

COPY build/package/entrypoint.sh /entrypoint.sh
COPY --from=build-stage /app/telegraf /telegraf

ENTRYPOINT ["/entrypoint.sh"]
CMD ["telegraf", "--config", "/etc/telegraf/telegraf.conf"]