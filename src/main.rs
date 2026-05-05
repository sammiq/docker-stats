#![deny(clippy::expect_used, clippy::panic, clippy::unwrap_used)]

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result, anyhow, bail};
use docker_stats::docker_events::{
    ContainerIdentity, ContainerStats, DockerEventFilter, DockerEventMonitor,
};
use tokio::task::JoinHandle;

type Containers = Arc<RwLock<HashMap<String, ContainerIdentity>>>;
type ContainerStatsById = Arc<RwLock<HashMap<String, ContainerStats>>>;
type StatsTasksById = Arc<RwLock<HashMap<String, JoinHandle<()>>>>;

const STATS_RECONNECT_DELAY: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
struct Config {
    output_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            output_interval: Duration::from_secs(10),
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

    let printer_containers = Arc::clone(&containers);
    let printer_container_stats = Arc::clone(&container_stats);
    let output_interval = config.output_interval;
    let printer = tokio::spawn(async move {
        let mut interval = tokio::time::interval(output_interval);

        loop {
            interval.tick().await;
            print_containers(&printer_containers, &printer_container_stats)?;
        }

        #[allow(unreachable_code)]
        Ok::<(), anyhow::Error>(())
    });

    tokio::select! {
        result = events => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error).context("Docker event watcher failed"),
                Err(error) => return Err(error).context("Docker event task ended unexpectedly"),
            }
        }
        result = printer => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error).context("container printer failed"),
                Err(error) => return Err(error).context("container printer task ended unexpectedly"),
            }
        }
        signal = tokio::signal::ctrl_c() => {
            signal.context("failed to listen for ctrl-c")?;
        }
    }

    Ok(())
}

fn parse_config() -> Result<Config> {
    use lexopt::prelude::*;

    let mut config = Config::default();
    let mut parser = lexopt::Parser::from_env();

    while let Some(arg) = parser.next()? {
        match arg {
            Short('o') | Long("output-seconds") => {
                config.output_interval = seconds_arg(
                    "output-seconds",
                    parser
                        .value()?
                        .parse()
                        .context("failed to parse output-seconds")?,
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
        "  -o, --output-seconds SECONDS   How often to print the current stats [default: 10]\n",
        "      --help                     Show this help"
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

fn print_containers(containers: &Containers, container_stats: &ContainerStatsById) -> Result<()> {
    let mut containers = containers
        .read()
        .map_err(|_| anyhow!("container lock is poisoned"))?
        .values()
        .cloned()
        .collect::<Vec<_>>();

    containers.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    println!("containers ({}):", containers.len());

    let container_stats = container_stats
        .read()
        .map_err(|_| anyhow!("container stats lock is poisoned"))?;

    for container in containers {
        let stats = container_stats.get(&container.id);
        let cpu = stats
            .and_then(|stats| stats.cpu_percent)
            .map(|cpu| format!("{cpu:>6.2}%"))
            .unwrap_or_else(|| "     -".to_string());
        let cpu_time = stats
            .and_then(|stats| stats.cpu_usage_ns)
            .map(format_duration_ns)
            .unwrap_or_else(|| "-".to_string());
        let memory = stats
            .and_then(|stats| stats.memory_usage_bytes)
            .map(format_bytes)
            .unwrap_or_else(|| "-".to_string());
        let rss = stats
            .and_then(|stats| stats.memory_rss_bytes)
            .map(format_bytes)
            .unwrap_or_else(|| "-".to_string());
        let cache = stats
            .and_then(|stats| stats.memory_cache_bytes)
            .map(format_bytes)
            .unwrap_or_else(|| "-".to_string());
        let rx = stats
            .and_then(|stats| stats.network_rx_bytes)
            .map(format_bytes)
            .unwrap_or_else(|| "-".to_string());
        let tx = stats
            .and_then(|stats| stats.network_tx_bytes)
            .map(format_bytes)
            .unwrap_or_else(|| "-".to_string());

        println!(
            "  cpu={} cpu_time={:>9} memory={:>9} rss={:>9} cache={:>9} rx={:>9} tx={:>9} {} {}",
            cpu, cpu_time, memory, rss, cache, rx, tx, container.id, container.name
        );
        println!(
            "    image={} image_id={} created={} started={} workdir={} labels={}",
            format_optional(container.image.as_deref()),
            format_optional(container.image_id.as_deref()),
            format_optional(container.created_at.as_deref()),
            format_optional(container.started_at.as_deref()),
            format_optional(container.working_dir.as_deref()),
            format_labels(&container.labels)
        );
    }

    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;

    let bytes = bytes as f64;

    if bytes >= GIB {
        format!("{:.2} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.2} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.2} KiB", bytes / KIB)
    } else {
        format!("{bytes:.0} B")
    }
}

fn format_duration_ns(nanoseconds: u64) -> String {
    const SECOND: f64 = 1_000_000_000.0;

    format!("{:.2}s", nanoseconds as f64 / SECOND)
}

fn format_optional(value: Option<&str>) -> &str {
    value.filter(|value| !value.is_empty()).unwrap_or("-")
}

fn format_labels(labels: &HashMap<String, String>) -> String {
    if labels.is_empty() {
        return "-".to_string();
    }

    let mut labels = labels
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>();
    labels.sort();
    labels.join(",")
}
