# docker-stats

`docker-stats` is a small Prometheus exporter for running Docker containers. It
watches container start/stop events, streams Docker stats for each running
container, and serves a cached Prometheus text feed.

The exporter currently exposes:

- `GET /metrics` for Prometheus metrics
- `GET /health` for a simple healthcheck

## Metrics

The metric names follow cAdvisor-style naming where practical:

- `container_cpu_usage_seconds_total`
- `container_memory_rss`
- `container_memory_cache`
- `container_network_receive_bytes_total`
- `container_network_transmit_bytes_total`
- `container_start_time_seconds`

Each metric includes `name` and `image` labels. Docker labels are also exposed as
Prometheus labels named `container_label_<label_name>`, with `.` replaced by
`_`.

## Command Line

```text
Usage: docker-stats [OPTIONS]

Options:
  -l, --listen ADDR             Address for the Prometheus metrics endpoint [default: 127.0.0.1:9100]
  -r, --render-seconds SECONDS  How often to render the current metrics body [default: 5]
      --help                    Show this help
```

Run locally:

```sh
cargo run -- --listen 127.0.0.1:9100
```

Then check:

```sh
curl http://127.0.0.1:9100/health
curl http://127.0.0.1:9100/metrics
```

## Docker

Build the image:

```sh
docker build -t docker-stats:local .
```

Run it locally with access to the Docker socket:

```sh
docker run --rm -it \
  --name docker-stats \
  -p 9100:9100 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  docker-stats:local
```

The container entrypoint supports these environment variables:

- `BIND_ADDR`: bind address inside the container, default `0.0.0.0`
- `LISTEN_PORT`: metrics and healthcheck port, default `9100`
- `RENDER_SECONDS`: metrics render interval, default `5`
- `HEALTHCHECK_URL`: optional override for the Docker healthcheck URL

For example:

```sh
docker run --rm -it \
  --name docker-stats \
  -p 9200:9200 \
  -e LISTEN_PORT=9200 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  docker-stats:local
```
