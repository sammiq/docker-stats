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

type Containers = Arc<RwLock<HashMap<String, ContainerIdentity>>>;
type ContainerStatsById = Arc<RwLock<HashMap<String, ContainerStats>>>;

#[derive(Clone, Debug)]
struct Config {
    output_interval: Duration,
    watch_interval: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            output_interval: Duration::from_secs(10),
            watch_interval: Duration::from_secs(5),
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
    let events = monitor.spawn_watch(filter, move |event| {
        let event_containers = Arc::clone(&event_containers);
        let event_container_stats = Arc::clone(&event_container_stats);

        async move {
            let action = event.action.as_deref().map_or("unknown", |action| action);

            match action {
                "die" => {
                    if let Some(container) = ContainerIdentity::from_event(&event)
                        && let Err(error) = remove_container(
                            &event_containers,
                            &event_container_stats,
                            &container.id,
                        )
                    {
                        eprintln!("{error:#}");
                    }
                }
                _ => {
                    if let Some(container) = ContainerIdentity::from_event(&event)
                        && let Err(error) = insert_container(&event_containers, container)
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
        insert_container(&containers, container)?;
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

    let cpu_monitor = monitor.clone();
    let cpu_containers = Arc::clone(&containers);
    let stats_container_stats = Arc::clone(&container_stats);
    let watch_interval = config.watch_interval;
    let cpu = tokio::spawn(async move {
        let mut interval = tokio::time::interval(watch_interval);

        loop {
            interval.tick().await;
            update_container_stats(&cpu_monitor, &cpu_containers, &stats_container_stats).await?;
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
        result = cpu => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error).context("container stats task failed"),
                Err(error) => return Err(error).context("container stats task ended unexpectedly"),
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
            Short('w') | Long("watch-seconds") => {
                config.watch_interval = seconds_arg(
                    "watch-seconds",
                    parser
                        .value()?
                        .parse()
                        .context("failed to parse watch-seconds")?,
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
    println!(
        "Usage: docker-stats [OPTIONS]\n\
\n\
Options:\n\
  -o, --output-seconds SECONDS  How often to print the current stats [default: 10]\n\
  -w, --watch-seconds SECONDS   How often to poll Docker stats [default: 5]\n\
      --help                    Show this help"
    );
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
    }

    Ok(())
}

async fn update_container_stats(
    monitor: &DockerEventMonitor,
    containers: &Containers,
    container_stats: &ContainerStatsById,
) -> Result<()> {
    let mut containers = containers
        .read()
        .map_err(|_| anyhow!("container lock is poisoned"))?
        .values()
        .cloned()
        .collect::<Vec<_>>();

    containers.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    for container in containers {
        match monitor.container_stats(&container.id).await {
            Ok(Some(stats)) => {
                container_stats
                    .write()
                    .map_err(|_| anyhow!("container stats lock is poisoned"))?
                    .insert(container.id.clone(), stats);
            }
            Ok(None) => {
                container_stats
                    .write()
                    .map_err(|_| anyhow!("container stats lock is poisoned"))?
                    .remove(&container.id);
            }
            Err(error) => {
                eprintln!(
                    "failed to get container stats for {} {}: {error}",
                    container.name, container.id
                );
            }
        }
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
