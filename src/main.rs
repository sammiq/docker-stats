#![deny(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use docker_stats::docker_events::{
    ContainerIdentity, ContainerStats, DockerEventFilter, DockerEventMonitor,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    signal::unix::{SignalKind, signal},
    task::JoinHandle,
};

type Containers = Arc<RwLock<HashMap<String, ContainerIdentity>>>;
type ContainerStatsById = Arc<RwLock<HashMap<String, ContainerStats>>>;
type StatsTasksById = Arc<RwLock<HashMap<String, JoinHandle<()>>>>;
type MetricsBody = Arc<RwLock<String>>;

const STATS_RECONNECT_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
struct Config {
    listen_addr: SocketAddr,
    render_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::from(([127, 0, 0, 1], 9100)),
            render_interval: Duration::from_secs(5),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = parse_config()?;
    let monitor =
        DockerEventMonitor::connect_to_socket().context("failed to connect to Docker socket")?;
    let containers = Containers::default();
    let container_stats = ContainerStatsById::default();
    let stats_tasks = StatsTasksById::default();
    let metrics_body = MetricsBody::default();
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the Unix epoch")?
        .as_secs()
        .to_string();

    let filter = DockerEventFilter::containers()
        .with_since(since)
        .with_filters("event", ["start", "die"]);

    let event_containers = Arc::clone(&containers);
    let event_container_stats = Arc::clone(&container_stats);
    let event_stats_tasks = Arc::clone(&stats_tasks);
    let event_monitor = monitor.clone();
    let events = monitor.spawn_watch(filter, move |event| {
        let event_containers = Arc::clone(&event_containers);
        let event_container_stats = Arc::clone(&event_container_stats);
        let event_stats_tasks = Arc::clone(&event_stats_tasks);
        let event_monitor = event_monitor.clone();

        async move {
            let action = event.action.as_deref().map_or("unknown", |action| action);

            match action {
                "die" => {
                    if let Some(container) = ContainerIdentity::from_event(&event)
                        && let Err(error) = remove_container(
                            &event_containers,
                            &event_container_stats,
                            &event_stats_tasks,
                            &container.id,
                        )
                    {
                        eprintln!("{error:#}");
                    }
                }
                _ => {
                    if let Some(container) = ContainerIdentity::from_event(&event)
                        && let Err(error) = ensure_container_stream(
                            &event_monitor,
                            &event_containers,
                            &event_container_stats,
                            &event_stats_tasks,
                            container,
                        )
                        .await
                    {
                        eprintln!("{error:#}");
                    }
                }
            }
        }
    });

    for container in monitor
        .list_containers(true)
        .await
        .context("failed to list running containers")?
    {
        ensure_container_stream(
            &monitor,
            &containers,
            &container_stats,
            &stats_tasks,
            container,
        )
        .await?;
    }

    update_metrics_body(&metrics_body, &containers, &container_stats)?;

    let renderer = tokio::spawn(render_prometheus_metrics_loop(
        config.render_interval,
        Arc::clone(&metrics_body),
        Arc::clone(&containers),
        Arc::clone(&container_stats),
    ));
    let metrics = tokio::spawn(serve_prometheus_metrics(
        config.listen_addr,
        Arc::clone(&metrics_body),
    ));

    let mut terminate = signal(SignalKind::terminate()).context("failed to listen for SIGTERM")?;

    tokio::select! {
        result = events => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error).context("Docker event watcher failed"),
                Err(error) => return Err(error).context("Docker event task ended unexpectedly"),
            }
        }
        result = renderer => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error).context("Prometheus metrics renderer failed"),
                Err(error) => return Err(error).context("Prometheus metrics renderer task ended unexpectedly"),
            }
        }
        result = metrics => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error).context("Prometheus metrics server failed"),
                Err(error) => return Err(error).context("Prometheus metrics server task ended unexpectedly"),
            }
        }
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for ctrl-c")?;
        }
        _ = terminate.recv() => {}
    }

    Ok(())
}

fn parse_config() -> Result<Config> {
    use lexopt::prelude::*;

    let mut config = Config::default();
    let mut parser = lexopt::Parser::from_env();

    while let Some(arg) = parser.next()? {
        match arg {
            Short('l') | Long("listen") => {
                config.listen_addr = parser
                    .value()?
                    .parse()
                    .context("failed to parse listen address")?;
            }
            Short('r') | Long("render-seconds") => {
                config.render_interval = seconds_arg(
                    "render-seconds",
                    parser
                        .value()?
                        .parse()
                        .context("failed to parse render-seconds")?,
                )?;
            }
            Long("help") => {
                print_usage();
                std::process::exit(0);
            }
            _ => return Err(arg.unexpected().into()),
        }
    }

    Ok(config)
}

fn seconds_arg(name: &'static str, seconds: u64) -> Result<Duration> {
    if seconds == 0 {
        bail!("{name} must be greater than zero")
    } else {
        Ok(Duration::from_secs(seconds))
    }
}

fn print_usage() {
    println!(concat!(
        "Usage: docker-stats [OPTIONS]\n",
        "\n",
        "Options:\n",
        "  -l, --listen ADDR             Address for the Prometheus metrics endpoint [default: 127.0.0.1:9100]\n",
        "  -r, --render-seconds SECONDS  How often to render the current metrics body [default: 5]\n",
        "      --help                    Show this help"
    ));
}

async fn ensure_container_stream(
    monitor: &DockerEventMonitor,
    containers: &Containers,
    container_stats: &ContainerStatsById,
    stats_tasks: &StatsTasksById,
    container: ContainerIdentity,
) -> Result<()> {
    let container_id = container.id.clone();
    let container_name = container.name.clone();
    let container = match monitor.inspect_container_identity(container.clone()).await {
        Ok(container) => container,
        Err(error) => {
            eprintln!("failed to inspect container {container_name} {container_id}: {error}");
            container
        }
    };

    insert_container(containers, container.clone())?;
    spawn_stats_stream(monitor, container_stats, stats_tasks, container)
}

fn insert_container(containers: &Containers, container: ContainerIdentity) -> Result<()> {
    containers
        .write()
        .map_err(|_| anyhow!("container lock is poisoned"))?
        .insert(container.id.clone(), container);

    Ok(())
}

fn remove_container(
    containers: &Containers,
    container_stats: &ContainerStatsById,
    stats_tasks: &StatsTasksById,
    container_id: &str,
) -> Result<()> {
    containers
        .write()
        .map_err(|_| anyhow!("container lock is poisoned"))?
        .remove(container_id);
    container_stats
        .write()
        .map_err(|_| anyhow!("container stats lock is poisoned"))?
        .remove(container_id);
    if let Some(task) = stats_tasks
        .write()
        .map_err(|_| anyhow!("stats task lock is poisoned"))?
        .remove(container_id)
    {
        task.abort();
    }

    Ok(())
}

fn spawn_stats_stream(
    monitor: &DockerEventMonitor,
    container_stats: &ContainerStatsById,
    stats_tasks: &StatsTasksById,
    container: ContainerIdentity,
) -> Result<()> {
    let mut stats_tasks = stats_tasks
        .write()
        .map_err(|_| anyhow!("stats task lock is poisoned"))?;

    if stats_tasks.contains_key(&container.id) {
        return Ok(());
    }

    let monitor = monitor.clone();
    let container_stats = Arc::clone(container_stats);
    let container_id = container.id.clone();
    let container_name = container.name.clone();
    let task = tokio::spawn(async move {
        loop {
            let result = monitor
                .stream_container_stats(&container_id, |stats| {
                    if let Ok(mut container_stats) = container_stats.write() {
                        container_stats.insert(container_id.clone(), stats);
                    }
                })
                .await;

            if let Err(error) = result {
                eprintln!("stats stream failed for {container_name} {container_id}: {error}");
            }

            if let Ok(mut container_stats) = container_stats.write() {
                container_stats.remove(&container_id);
            }

            tokio::time::sleep(STATS_RECONNECT_DELAY).await;
        }
    });

    stats_tasks.insert(container.id, task);

    Ok(())
}

async fn serve_prometheus_metrics(
    listen_addr: SocketAddr,
    metrics_body: MetricsBody,
) -> Result<()> {
    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("failed to bind metrics listener on {listen_addr}"))?;

    println!("serving Prometheus metrics on http://{listen_addr}/metrics");

    loop {
        let (stream, _) = listener.accept().await?;
        let metrics_body = Arc::clone(&metrics_body);

        tokio::spawn(async move {
            if let Err(error) = handle_metrics_connection(stream, metrics_body).await {
                eprintln!("metrics request failed: {error:#}");
            }
        });
    }
}

async fn handle_metrics_connection(mut stream: TcpStream, metrics_body: MetricsBody) -> Result<()> {
    let mut buffer = [0; 1024];
    let bytes_read = stream.read(&mut buffer).await?;

    if bytes_read == 0 {
        return Ok(());
    }

    let request = String::from_utf8_lossy(&buffer[..bytes_read]);
    let Some(request_line) = request.lines().next() else {
        write_http_response(
            &mut stream,
            "400 Bad Request",
            "text/plain; charset=utf-8",
            "bad request\n",
        )
        .await?;
        return Ok(());
    };

    let mut request_parts = request_line.split_whitespace();
    let method = request_parts.next();
    let path = request_parts.next();

    match (method, path) {
        (Some("GET"), Some("/health")) => {
            write_http_response(&mut stream, "200 OK", "text/plain; charset=utf-8", "ok\n").await?;
        }
        (Some("GET"), Some("/metrics")) => {
            let body = metrics_body
                .read()
                .map_err(|_| anyhow!("metrics body lock is poisoned"))?
                .clone();
            write_http_response(
                &mut stream,
                "200 OK",
                "text/plain; version=0.0.4; charset=utf-8",
                &body,
            )
            .await?;
        }
        _ => {
            write_http_response(
                &mut stream,
                "404 Not Found",
                "text/plain; charset=utf-8",
                "not found\n",
            )
            .await?;
        }
    }

    Ok(())
}

async fn render_prometheus_metrics_loop(
    render_interval: Duration,
    metrics_body: MetricsBody,
    containers: Containers,
    container_stats: ContainerStatsById,
) -> Result<()> {
    let mut interval = tokio::time::interval(render_interval);

    loop {
        interval.tick().await;

        if let Err(error) = update_metrics_body(&metrics_body, &containers, &container_stats) {
            eprintln!("failed to render Prometheus metrics: {error:#}");
        }
    }
}

fn update_metrics_body(
    metrics_body: &MetricsBody,
    containers: &Containers,
    container_stats: &ContainerStatsById,
) -> Result<()> {
    let metrics = render_prometheus_metrics(containers, container_stats)?;
    *metrics_body
        .write()
        .map_err(|_| anyhow!("metrics body lock is poisoned"))? = metrics;

    Ok(())
}

async fn write_http_response(
    stream: &mut TcpStream,
    status: &str,
    content_type: &str,
    body: &str,
) -> Result<()> {
    let response = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;

    Ok(())
}

fn render_prometheus_metrics(
    containers: &Containers,
    container_stats: &ContainerStatsById,
) -> Result<String> {
    let mut containers = containers
        .read()
        .map_err(|_| anyhow!("container lock is poisoned"))?
        .values()
        .cloned()
        .collect::<Vec<_>>();

    containers.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    let container_stats = container_stats
        .read()
        .map_err(|_| anyhow!("container stats lock is poisoned"))?;
    let mut metrics = String::from(
        "# HELP container_cpu_usage_seconds_total Cumulative cpu time consumed in seconds.\n\
         # TYPE container_cpu_usage_seconds_total counter\n\
         # HELP container_cpu_system_seconds_total Cumulative system cpu time consumed in seconds.\n\
         # TYPE container_cpu_system_seconds_total counter\n\
         # HELP container_cpu_user_seconds_total Cumulative user cpu time consumed seconds.\n\
         # TYPE container_cpu_user_seconds_total counter\n\
         # HELP container_spec_memory_limit_bytes Memory limit for the container in bytes.\n\
         # TYPE container_spec_memory_limit_bytes gauge\n\
         # HELP container_memory_usage_bytes Current memory usage in bytes.\n\
         # TYPE container_memory_usage_bytes gauge\n\
         # HELP container_memory_rss Size of RSS in bytes.\n\
         # TYPE container_memory_rss gauge\n\
         # HELP container_memory_cache Number of bytes of page cache memory.\n\
         # TYPE container_memory_cache gauge\n\
         # HELP container_network_receive_bytes_total Cumulative count of bytes received.\n\
         # TYPE container_network_receive_bytes_total counter\n\
         # HELP container_network_transmit_bytes_total Cumulative count of bytes transmitted.\n\
         # TYPE container_network_transmit_bytes_total counter\n\
         # HELP container_start_time_seconds Start time of the container since Unix epoch in seconds.\n\
         # TYPE container_start_time_seconds gauge\n",
    );

    for container in containers {
        let labels = prometheus_container_labels(&container);
        if let Some(start_time_seconds) = container
            .started_at
            .as_deref()
            .and_then(parse_rfc3339_unix_seconds)
        {
            push_prometheus_sample_without_timestamp(
                &mut metrics,
                "container_start_time_seconds",
                &labels,
                start_time_seconds,
            );
        }

        let Some(stats) = container_stats.get(&container.id) else {
            continue;
        };

        let collected_at_ms = unix_timestamp_millis(stats.collected_at)?;
        if let Some(cpu_usage_total_ns) = stats.cpu_usage_total_ns {
            push_prometheus_sample(
                &mut metrics,
                "container_cpu_usage_seconds_total",
                &labels,
                cpu_usage_total_ns as f64 / 1_000_000_000.0,
                collected_at_ms,
            );
        }
        if let Some(cpu_usage_system_ns) = stats.cpu_usage_system_ns {
            push_prometheus_sample(
                &mut metrics,
                "container_cpu_system_seconds_total",
                &labels,
                cpu_usage_system_ns as f64 / 1_000_000_000.0,
                collected_at_ms,
            );
        }
        if let Some(cpu_usage_user_ns) = stats.cpu_usage_user_ns {
            push_prometheus_sample(
                &mut metrics,
                "container_cpu_user_seconds_total",
                &labels,
                cpu_usage_user_ns as f64 / 1_000_000_000.0,
                collected_at_ms,
            );
        }
        if let Some(memory_limit_bytes) = stats.memory_limit_bytes {
            push_prometheus_sample(
                &mut metrics,
                "container_spec_memory_limit_bytes",
                &labels,
                memory_limit_bytes as f64,
                collected_at_ms,
            );
        }
        if let Some(memory_rss_bytes) = stats.memory_usage_bytes {
            push_prometheus_sample(
                &mut metrics,
                "container_memory_usage_bytes",
                &labels,
                memory_rss_bytes as f64,
                collected_at_ms,
            );
        }
        if let Some(memory_rss_bytes) = stats.memory_rss_bytes {
            push_prometheus_sample(
                &mut metrics,
                "container_memory_rss",
                &labels,
                memory_rss_bytes as f64,
                collected_at_ms,
            );
        }
        if let Some(memory_cache_bytes) = stats.memory_cache_bytes {
            push_prometheus_sample(
                &mut metrics,
                "container_memory_cache",
                &labels,
                memory_cache_bytes as f64,
                collected_at_ms,
            );
        }
        if let Some(network_rx_bytes) = stats.network_rx_bytes {
            push_prometheus_sample(
                &mut metrics,
                "container_network_receive_bytes_total",
                &labels,
                network_rx_bytes as f64,
                collected_at_ms,
            );
        }
        if let Some(network_tx_bytes) = stats.network_tx_bytes {
            push_prometheus_sample(
                &mut metrics,
                "container_network_transmit_bytes_total",
                &labels,
                network_tx_bytes as f64,
                collected_at_ms,
            );
        }
    }

    Ok(metrics)
}

fn push_prometheus_sample(
    metrics: &mut String,
    name: &str,
    labels: &str,
    value: f64,
    timestamp_ms: u128,
) {
    metrics.push_str(&format!("{name}{{{labels}}} {value:.9} {timestamp_ms}\n"));
}

fn push_prometheus_sample_without_timestamp(
    metrics: &mut String,
    name: &str,
    labels: &str,
    value: f64,
) {
    metrics.push_str(&format!("{name}{{{labels}}} {value:.9}\n"));
}

fn unix_timestamp_millis(time: SystemTime) -> Result<u128> {
    let duration = time
        .duration_since(UNIX_EPOCH)
        .context("stats collection timestamp is before the Unix epoch")?;

    Ok(duration.as_millis())
}

fn parse_rfc3339_unix_seconds(value: &str) -> Option<f64> {
    let (date, time_with_offset) = value.split_once('T')?;
    let (year, month, day) = parse_rfc3339_date(date)?;
    let (time, offset_seconds) = parse_rfc3339_offset(time_with_offset)?;
    let (hour, minute, second) = parse_rfc3339_time(time)?;

    let days = days_from_civil(year, month, day);
    let seconds = days * 86_400 + i64::from(hour) * 3_600 + i64::from(minute) * 60;

    Some(seconds as f64 + second - f64::from(offset_seconds))
}

fn parse_rfc3339_date(value: &str) -> Option<(i32, u32, u32)> {
    let mut parts = value.split('-');
    let year = parts.next()?.parse().ok()?;
    let month = parts.next()?.parse().ok()?;
    let day = parts.next()?.parse().ok()?;

    if parts.next().is_none() {
        Some((year, month, day))
    } else {
        None
    }
}

fn parse_rfc3339_offset(value: &str) -> Option<(&str, i32)> {
    if let Some(time) = value.strip_suffix('Z') {
        return Some((time, 0));
    }

    let offset_start = value.rfind('+').or_else(|| value.rfind('-'))?;
    let time = &value[..offset_start];
    let offset = &value[offset_start..];
    let sign = if offset.starts_with('-') { -1 } else { 1 };
    let mut parts = offset[1..].split(':');
    let hours = parts.next()?.parse::<i32>().ok()?;
    let minutes = parts.next()?.parse::<i32>().ok()?;

    if parts.next().is_none() {
        Some((time, sign * (hours * 3_600 + minutes * 60)))
    } else {
        None
    }
}

fn parse_rfc3339_time(value: &str) -> Option<(u32, u32, f64)> {
    let mut parts = value.split(':');
    let hour = parts.next()?.parse().ok()?;
    let minute = parts.next()?.parse().ok()?;
    let second = parse_rfc3339_second(parts.next()?)?;

    if parts.next().is_none() {
        Some((hour, minute, second))
    } else {
        None
    }
}

fn parse_rfc3339_second(value: &str) -> Option<f64> {
    let (second, fraction) = value.split_once('.').unwrap_or((value, ""));
    let second = second.parse::<u32>().ok()? as f64;

    if fraction.is_empty() {
        return Some(second);
    }

    if !fraction.chars().all(|character| character.is_ascii_digit()) {
        return None;
    }

    let fraction = fraction.parse::<u32>().ok()? as f64 / 10_f64.powi(fraction.len() as i32);
    Some(second + fraction)
}

fn days_from_civil(year: i32, month: u32, day: u32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = if year >= 0 { year } else { year - 399 } / 400;
    let year_of_era = year - era * 400;
    let month = i64::from(month);
    let day_of_year = (153 * (month + if month > 2 { -3 } else { 9 }) + 2) / 5 + i64::from(day) - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

    era * 146_097 + day_of_era - 719_468
}

fn prometheus_container_labels(container: &ContainerIdentity) -> String {
    let mut labels = vec![
        prometheus_label("name", &container.name),
        prometheus_label("image", container.image.as_deref().unwrap_or_default()),
    ];

    labels.extend(container.labels.iter().map(|(name, value)| {
        prometheus_label(
            &format!("container_label_{}", prometheus_label_name(name)),
            value,
        )
    }));
    labels.sort();
    labels.join(",")
}

fn prometheus_label(name: &str, value: &str) -> String {
    format!("{name}=\"{}\"", escape_prometheus_label_value(value))
}

fn prometheus_label_name(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || character == '_' {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn escape_prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('\n', r"\n")
        .replace('"', r#"\""#)
}
