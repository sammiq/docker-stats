use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use docker_stats::docker_events::{
    ContainerIdentity, ContainerStats, DockerEventFilter, DockerEventMonitor,
};

type Containers = Arc<RwLock<HashMap<String, ContainerIdentity>>>;
type ContainerStatsById = Arc<RwLock<HashMap<String, ContainerStats>>>;

#[tokio::main]
async fn main() -> Result<(), bollard::errors::Error> {
    let monitor = DockerEventMonitor::connect_to_socket()?;
    let containers = Containers::default();
    let container_stats = ContainerStatsById::default();
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
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
            let action = event.action.as_deref().unwrap_or("unknown");

            match action {
                "die" => {
                    if let Some(container) = ContainerIdentity::from_event(&event) {
                        event_containers.write().unwrap().remove(&container.id);
                        event_container_stats.write().unwrap().remove(&container.id);
                    }
                }
                _ => {
                    if let Some(container) = ContainerIdentity::from_event(&event) {
                        event_containers
                            .write()
                            .unwrap()
                            .insert(container.id.clone(), container);
                    }
                }
            }
        }
    });

    for container in monitor.list_containers(true).await? {
        containers
            .write()
            .unwrap()
            .insert(container.id.clone(), container);
    }

    let printer_containers = Arc::clone(&containers);
    let printer_container_stats = Arc::clone(&container_stats);
    let printer = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            interval.tick().await;
            print_containers(&printer_containers, &printer_container_stats);
        }

        #[allow(unreachable_code)]
        ()
    });

    let cpu_monitor = monitor.clone();
    let cpu_containers = Arc::clone(&containers);
    let stats_container_stats = Arc::clone(&container_stats);
    let cpu = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));

        loop {
            interval.tick().await;
            update_container_stats(&cpu_monitor, &cpu_containers, &stats_container_stats).await;
        }

        #[allow(unreachable_code)]
        ()
    });

    tokio::select! {
        result = events => {
            match result {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => eprintln!("docker event task ended unexpectedly: {error}"),
            }
        }
        result = printer => {
            if let Err(error) = result {
                eprintln!("container printer task ended unexpectedly: {error}");
            }
        }
        result = cpu => {
            if let Err(error) = result {
                eprintln!("cpu usage task ended unexpectedly: {error}");
            }
        }
        signal = tokio::signal::ctrl_c() => {
            if let Err(error) = signal {
                eprintln!("failed to listen for ctrl-c: {error}");
            }
        }
    }

    Ok(())
}

fn print_containers(containers: &Containers, container_stats: &ContainerStatsById) {
    let mut containers = containers
        .read()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();

    containers.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    println!("containers ({}):", containers.len());

    let container_stats = container_stats.read().unwrap();

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
}

async fn update_container_stats(
    monitor: &DockerEventMonitor,
    containers: &Containers,
    container_stats: &ContainerStatsById,
) {
    let mut containers = containers
        .read()
        .unwrap()
        .values()
        .cloned()
        .collect::<Vec<_>>();

    containers.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    for container in containers {
        match monitor.container_stats(&container.id).await {
            Ok(Some(stats)) => {
                container_stats
                    .write()
                    .unwrap()
                    .insert(container.id.clone(), stats);
            }
            Ok(None) => {
                container_stats.write().unwrap().remove(&container.id);
            }
            Err(error) => {
                eprintln!(
                    "failed to get container stats for {} {}: {error}",
                    container.name, container.id
                );
            }
        }
    }
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
