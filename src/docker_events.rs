use std::{collections::HashMap, future::Future, time::SystemTime};

use bollard::{
    Docker,
    errors::Error,
    models::{ContainerInspectResponse, ContainerStatsResponse, ContainerSummary, EventMessage},
    query_parameters::{
        EventsOptions, EventsOptionsBuilder, InspectContainerOptionsBuilder,
        ListContainersOptionsBuilder, StatsOptionsBuilder,
    },
};
use futures_util::StreamExt;
use tokio::task::JoinHandle;

#[derive(Clone, Debug, Default)]
pub struct DockerEventFilter {
    since: Option<String>,
    until: Option<String>,
    filters: HashMap<String, Vec<String>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ContainerIdentity {
    pub id: String,
    pub name: String,
    pub created_at: Option<String>,
    pub started_at: Option<String>,
    pub image: Option<String>,
    pub image_id: Option<String>,
    pub working_dir: Option<String>,
    pub labels: HashMap<String, String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ContainerStats {
    pub id: String,
    pub name: Option<String>,
    pub collected_at: SystemTime,
    // CPU metrics
    pub cpu_usage_total_ns: Option<u64>,
    pub cpu_usage_system_ns: Option<u64>,
    pub cpu_usage_user_ns: Option<u64>,
    // Memory metrics
    pub memory_limit_bytes: Option<u64>,
    pub memory_usage_bytes: Option<u64>,
    pub memory_rss_bytes: Option<u64>,
    pub memory_cache_bytes: Option<u64>,
    // Network metrics
    pub network_rx_bytes: Option<u64>,
    pub network_tx_bytes: Option<u64>,
}

impl ContainerIdentity {
    pub fn from_event(event: &EventMessage) -> Option<Self> {
        let actor = event.actor.as_ref()?;
        let id = actor.id.clone()?;
        let attributes = actor.attributes.as_ref();
        let name = attributes
            .and_then(|attributes| attributes.get("name"))
            .cloned()
            .unwrap_or_else(|| id.clone());
        let image = attributes
            .and_then(|attributes| attributes.get("image"))
            .cloned();

        Some(Self {
            id,
            name,
            created_at: None,
            started_at: None,
            image,
            image_id: None,
            working_dir: None,
            labels: HashMap::new(),
        })
    }

    fn from_summary(summary: ContainerSummary) -> Option<Self> {
        let id = summary.id?;
        let name = summary
            .names
            .and_then(|names| names.into_iter().next())
            .map(|name| name.trim_start_matches('/').to_string())
            .unwrap_or_else(|| id.clone());

        Some(Self {
            id,
            name,
            created_at: summary.created.map(|created| created.to_string()),
            started_at: None,
            image: summary.image,
            image_id: summary.image_id,
            working_dir: None,
            labels: summary.labels.unwrap_or_default(),
        })
    }

    fn apply_inspect(mut self, inspect: ContainerInspectResponse) -> Self {
        if let Some(id) = inspect.id {
            self.id = id;
        }
        if let Some(name) = inspect.name {
            self.name = name.trim_start_matches('/').to_string();
        }

        self.created_at = inspect.created.map(|created| created.to_string());
        self.started_at = inspect
            .state
            .and_then(|state| empty_string_as_none(state.started_at));

        if let Some(config) = inspect.config {
            self.image = config.image.or(self.image);
            self.working_dir = empty_string_as_none(config.working_dir);
            self.labels = config.labels.unwrap_or(self.labels);
        }

        self.image_id = inspect.image.or(self.image_id);

        self
    }
}

impl DockerEventFilter {
    pub fn containers() -> Self {
        Self::default().with_filter("type", "container")
    }

    pub fn with_since(mut self, since: impl Into<String>) -> Self {
        self.since = Some(since.into());
        self
    }

    pub fn with_until(mut self, until: impl Into<String>) -> Self {
        self.until = Some(until.into());
        self
    }

    pub fn with_filter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.filters
            .entry(key.into())
            .or_default()
            .push(value.into());
        self
    }

    pub fn with_filters<I, K, V>(mut self, key: K, values: I) -> Self
    where
        I: IntoIterator<Item = V>,
        K: Into<String>,
        V: Into<String>,
    {
        self.filters
            .entry(key.into())
            .or_default()
            .extend(values.into_iter().map(Into::into));
        self
    }

    fn into_options(self) -> EventsOptions {
        let mut builder = EventsOptionsBuilder::default();

        if let Some(since) = self.since.as_deref() {
            builder = builder.since(since);
        }

        if let Some(until) = self.until.as_deref() {
            builder = builder.until(until);
        }

        if !self.filters.is_empty() {
            builder = builder.filters(&self.filters);
        }

        builder.build()
    }
}

#[derive(Clone, Debug)]
pub struct DockerEventMonitor {
    docker: Docker,
}

impl DockerEventMonitor {
    pub fn connect_to_socket() -> Result<Self, Error> {
        let docker = Docker::connect_with_socket_defaults()?;
        Ok(Self { docker })
    }

    pub async fn watch<F, Fut>(
        &self,
        filter: DockerEventFilter,
        mut callback: F,
    ) -> Result<(), Error>
    where
        F: FnMut(EventMessage) -> Fut,
        Fut: Future<Output = ()>,
    {
        let mut events = self.docker.events(Some(filter.into_options()));

        while let Some(event) = events.next().await {
            callback(event?).await;
        }

        Ok(())
    }

    pub async fn list_containers(
        &self,
        running_only: bool,
    ) -> Result<Vec<ContainerIdentity>, Error> {
        let options = ListContainersOptionsBuilder::default()
            .all(!running_only)
            .build();
        let containers = self.docker.list_containers(Some(options)).await?;

        Ok(containers
            .into_iter()
            .filter_map(ContainerIdentity::from_summary)
            .collect())
    }

    pub async fn inspect_container_identity(
        &self,
        container: ContainerIdentity,
    ) -> Result<ContainerIdentity, Error> {
        let options = InspectContainerOptionsBuilder::default()
            .size(false)
            .build();
        let inspect = self
            .docker
            .inspect_container(&container.id, Some(options))
            .await?;

        Ok(container.apply_inspect(inspect))
    }

    pub async fn container_stats(
        &self,
        container_id: &str,
    ) -> Result<Option<ContainerStats>, Error> {
        let options = StatsOptionsBuilder::default().stream(false).build();
        let mut stats = self.docker.stats(container_id, Some(options));

        let Some(stats) = stats.next().await else {
            return Ok(None);
        };

        let stats = stats?;
        let collected_at = SystemTime::now();

        Ok(container_stats(stats, collected_at))
    }

    pub async fn stream_container_stats<F>(
        &self,
        container_id: &str,
        mut callback: F,
    ) -> Result<(), Error>
    where
        F: FnMut(ContainerStats),
    {
        let options = StatsOptionsBuilder::default().stream(true).build();
        let mut stats = self.docker.stats(container_id, Some(options));

        while let Some(stats) = stats.next().await {
            let stats = stats?;
            let collected_at = SystemTime::now();

            if let Some(stats) = container_stats(stats, collected_at) {
                callback(stats);
            }
        }

        Ok(())
    }

    pub fn spawn_watch<F, Fut>(
        &self,
        filter: DockerEventFilter,
        callback: F,
    ) -> JoinHandle<Result<(), Error>>
    where
        F: FnMut(EventMessage) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let monitor = self.clone();

        tokio::spawn(async move { monitor.watch(filter, callback).await })
    }
}

fn container_stats(
    stats: ContainerStatsResponse,
    collected_at: SystemTime,
) -> Option<ContainerStats> {
    let cpu_usage = stats.cpu_stats.and_then(|cu| cu.cpu_usage);
    let cpu_usage_total_ns = cpu_usage.as_ref().and_then(|cu| cu.total_usage);
    let cpu_usage_system_ns = cpu_usage.as_ref().and_then(|cu| cu.usage_in_kernelmode);
    let cpu_usage_user_ns = cpu_usage.as_ref().and_then(|cu| cu.usage_in_usermode);

    let memory_stats = stats.memory_stats.as_ref();
    let memory_limit_bytes = memory_stats.and_then(|ms| ms.limit);
    let memory_usage_bytes = memory_stats.and_then(|ms| ms.usage);
    let memory_rss_bytes = memory_stats.and_then(|memory_stats| {
        let stats = memory_stats.stats.as_ref()?;

        stats
            .get("rss")
            .or_else(|| stats.get("total_rss"))
            .copied()
            .or_else(|| {
                let anon = stats.get("anon")?;
                let shmem = stats.get("shmem").copied().unwrap_or_default();
                Some(anon + shmem)
            })
    });
    let memory_cache_bytes = memory_stats.and_then(|memory_stats| {
        memory_stats
            .stats
            .as_ref()
            .and_then(|stats| {
                stats
                    .get("cache")
                    .or_else(|| stats.get("total_cache"))
                    .copied()
            })
            .or_else(|| memory_usage_bytes?.checked_sub(memory_rss_bytes?))
    });

    let network_rx_bytes = stats.networks.as_ref().map(|networks| {
        networks
            .values()
            .filter_map(|network| network.rx_bytes)
            .sum()
    });
    let network_tx_bytes = stats.networks.as_ref().map(|networks| {
        networks
            .values()
            .filter_map(|network| network.tx_bytes)
            .sum()
    });

    Some(ContainerStats {
        id: stats.id?,
        name: stats.name,
        collected_at,
        cpu_usage_total_ns,
        cpu_usage_system_ns,
        cpu_usage_user_ns,
        memory_limit_bytes,
        memory_usage_bytes,
        memory_rss_bytes,
        memory_cache_bytes,
        network_rx_bytes,
        network_tx_bytes,
    })
}

fn empty_string_as_none(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}
