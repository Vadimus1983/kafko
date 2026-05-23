use crate::consumer::Consumer;
use crate::error::{KafkoError, Result};
use crate::log::LogConfig;
use crate::partition::Partition;
use crate::producer::Producer;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct Kafko {
    dir: PathBuf,
    topics: RwLock<HashMap<String, Arc<Partition>>>,
    default_log_config: LogConfig,
}

impl Kafko {
    pub async fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_config(dir, LogConfig::default()).await
    }

    pub async fn open_with_config(
        dir: impl AsRef<Path>,
        default_log_config: LogConfig,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&dir).await?;

        let mut topics = HashMap::new();
        let mut entries = tokio::fs::read_dir(&dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let file_type = entry.file_type().await?;
            if !file_type.is_dir() {
                continue;
            }
            let name = match entry.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue,
            };
            let partition = Partition::open(&entry.path(), default_log_config).await?;
            topics.insert(name, Arc::new(partition));
        }

        Ok(Self {
            dir,
            topics: RwLock::new(topics),
            default_log_config,
        })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn default_log_config(&self) -> &LogConfig {
        &self.default_log_config
    }

    pub async fn create_topic(&self, name: &str) -> Result<()> {
        self.create_topic_with_config(name, self.default_log_config)
            .await
    }

    pub async fn create_topic_with_config(&self, name: &str, log_config: LogConfig) -> Result<()> {
        let mut topics = self.topics.write().await;
        if topics.contains_key(name) {
            return Err(KafkoError::TopicAlreadyExists(name.to_string()));
        }
        let topic_dir = self.dir.join(name);
        let partition = Partition::open(&topic_dir, log_config).await?;
        topics.insert(name.to_string(), Arc::new(partition));
        Ok(())
    }

    pub async fn delete_topic(&self, name: &str) -> Result<()> {
        let mut topics = self.topics.write().await;
        let partition = match topics.remove(name) {
            Some(p) => p,
            None => return Err(KafkoError::TopicNotFound(name.to_string())),
        };

        match Arc::try_unwrap(partition) {
            Ok(owned) => {
                let topic_dir = self.dir.join(name);
                owned.shutdown().await?;
                tokio::fs::remove_dir_all(&topic_dir).await?;
                Ok(())
            }
            Err(arc) => {
                // External refs exist; restore registry state and report.
                topics.insert(name.to_string(), arc);
                Err(KafkoError::TopicInUse(name.to_string()))
            }
        }
    }

    pub async fn list_topics(&self) -> Vec<String> {
        let mut names: Vec<String> = self.topics.read().await.keys().cloned().collect();
        names.sort();
        names
    }

    pub async fn has_topic(&self, name: &str) -> bool {
        self.topics.read().await.contains_key(name)
    }

    pub async fn topic(&self, name: &str) -> Option<Arc<Partition>> {
        self.topics.read().await.get(name).cloned()
    }

    pub async fn producer_for(&self, name: &str) -> Result<Producer> {
        let partition = self
            .topic(name)
            .await
            .ok_or_else(|| KafkoError::TopicNotFound(name.to_string()))?;
        Ok(Producer::new(partition))
    }

    pub async fn consumer_for(&self, name: &str) -> Result<Consumer> {
        let partition = self
            .topic(name)
            .await
            .ok_or_else(|| KafkoError::TopicNotFound(name.to_string()))?;
        Ok(Consumer::from_partition(partition))
    }

    pub async fn shutdown(self) -> Result<()> {
        let topics = self.topics.into_inner();
        for (_, partition) in topics {
            if let Ok(owned) = Arc::try_unwrap(partition) {
                let _ = owned.shutdown().await;
            }
        }
        Ok(())
    }
}
