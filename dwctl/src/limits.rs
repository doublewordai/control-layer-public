
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::config::{FileLimitsConfig, LimitsConfig};
use crate::errors::{Error, Result};

pub const MULTIPART_OVERHEAD: u64 = 10 * 1024;

#[derive(Debug, Default, Clone)]
pub struct Limiters {
    pub inference_body_size: usize,
    pub file_uploads: Option<Arc<UploadLimiter>>,
}

impl Limiters {
    pub fn new(config: &LimitsConfig) -> Self {
        Self {
            inference_body_size: usize::try_from(config.requests.max_body_size).unwrap_or(usize::MAX),
            file_uploads: UploadLimiter::new(&config.files).map(Arc::new),
        }
    }
}

#[derive(Debug)]
pub struct UploadLimiter {
    semaphore: Arc<Semaphore>,
    waiting_count: AtomicUsize,
    max_waiting: Option<usize>,
    max_wait: Duration,
}

impl UploadLimiter {
    pub fn new(config: &FileLimitsConfig) -> Option<Self> {
        if config.max_concurrent_uploads == 0 {
            return None;
        }

        Some(Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent_uploads)),
            waiting_count: AtomicUsize::new(0),
            max_waiting: if config.max_waiting_uploads == 0 {
                None
            } else {
                Some(config.max_waiting_uploads)
            },
            max_wait: Duration::from_secs(config.max_upload_wait_secs),
        })
    }

    pub async fn acquire(&self) -> Result<UploadPermit> {
        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                return Ok(UploadPermit { _permit: permit });
            }
            Err(_) => {
            }
        }

        let current_waiting = self.waiting_count.fetch_add(1, Ordering::SeqCst);
        if let Some(max_waiting) = self.max_waiting
            && current_waiting >= max_waiting
        {
            self.waiting_count.fetch_sub(1, Ordering::SeqCst);
            return Err(Error::TooManyRequests {
                message: "Too many file uploads in progress. Please retry later.".to_string(),
            });
        }

        match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => {
                self.waiting_count.fetch_sub(1, Ordering::SeqCst);
                return Ok(UploadPermit { _permit: permit });
            }
            Err(_) => {
            }
        }

        let result = if self.max_wait.is_zero() {
            Err(Error::TooManyRequests {
                message: "Too many file uploads in progress. Please retry later.".to_string(),
            })
        } else {
            match tokio::time::timeout(self.max_wait, self.semaphore.clone().acquire_owned()).await {
                Ok(Ok(permit)) => Ok(UploadPermit { _permit: permit }),
                Ok(Err(_)) => {
                    Err(Error::TooManyRequests {
                        message: "Upload service temporarily unavailable.".to_string(),
                    })
                }
                Err(_) => {
                    Err(Error::TooManyRequests {
                        message: "Timed out waiting for upload slot. Please retry later.".to_string(),
                    })
                }
            }
        };

        self.waiting_count.fetch_sub(1, Ordering::SeqCst);

        result
    }
}

#[must_use]
pub struct UploadPermit {
    _permit: OwnedSemaphorePermit,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config(max_concurrent: usize, max_waiting: usize, max_wait_secs: u64) -> FileLimitsConfig {
        FileLimitsConfig {
            max_concurrent_uploads: max_concurrent,
            max_waiting_uploads: max_waiting,
            max_upload_wait_secs: max_wait_secs,
            ..Default::default()
        }
    }

    #[test]
    fn test_unlimited_returns_none() {
        let config = test_config(0, 20, 60);
        assert!(UploadLimiter::new(&config).is_none());
    }

    #[tokio::test]
    async fn test_acquire_when_available() {
        let config = test_config(2, 10, 60);
        let limiter = UploadLimiter::new(&config).unwrap();

        let permit1 = limiter.acquire().await;
        assert!(permit1.is_ok());

        let permit2 = limiter.acquire().await;
        assert!(permit2.is_ok());
    }

    #[tokio::test]
    async fn test_acquire_waits_and_succeeds() {
        let config = test_config(1, 10, 5);
        let limiter = Arc::new(UploadLimiter::new(&config).unwrap());

        let permit1 = limiter.acquire().await.unwrap();

        let limiter_clone = limiter.clone();
        let handle = tokio::spawn(async move { limiter_clone.acquire().await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        drop(permit1);

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_acquire_rejects_when_queue_full() {
        let config = test_config(1, 1, 60);
        let limiter = Arc::new(UploadLimiter::new(&config).unwrap());

        let _permit1 = limiter.acquire().await.unwrap();

        let limiter_clone = limiter.clone();
        let _handle1 = tokio::spawn(async move { limiter_clone.acquire().await });

        tokio::time::sleep(Duration::from_millis(50)).await;

        let result = limiter.acquire().await;
        assert!(result.is_err());
        if let Err(Error::TooManyRequests { message }) = result {
            assert!(message.contains("Too many file uploads"));
        } else {
            panic!("Expected TooManyRequests error");
        }
    }

    #[tokio::test]
    async fn test_acquire_times_out() {
        let config = test_config(1, 10, 1);
        let limiter = Arc::new(UploadLimiter::new(&config).unwrap());

        let _permit1 = limiter.acquire().await.unwrap();

        let start = std::time::Instant::now();
        let result = limiter.acquire().await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert!(elapsed >= Duration::from_secs(1));
        assert!(elapsed < Duration::from_secs(2));

        if let Err(Error::TooManyRequests { message }) = result {
            assert!(message.contains("Timed out"));
        } else {
            panic!("Expected TooManyRequests error");
        }
    }

    #[tokio::test]
    async fn test_zero_wait_rejects_immediately() {
        let config = test_config(1, 10, 0);
        let limiter = UploadLimiter::new(&config).unwrap();

        let _permit1 = limiter.acquire().await.unwrap();

        let start = std::time::Instant::now();
        let result = limiter.acquire().await;
        let elapsed = start.elapsed();

        assert!(result.is_err());
        assert!(elapsed < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn test_permit_released_on_drop() {
        let config = test_config(1, 10, 1);
        let limiter = UploadLimiter::new(&config).unwrap();

        {
            let _permit = limiter.acquire().await.unwrap();
        }

        let result = limiter.acquire().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_unlimited_waiting_queue() {
        let config = test_config(1, 0, 5);
        let limiter = Arc::new(UploadLimiter::new(&config).unwrap());

        let permit1 = limiter.acquire().await.unwrap();

        let mut handles = vec![];
        for _ in 0..10 {
            let limiter_clone = limiter.clone();
            handles.push(tokio::spawn(async move { limiter_clone.acquire().await }));
        }

        tokio::time::sleep(Duration::from_millis(50)).await;

        drop(permit1);

        let result = handles.remove(0).await.unwrap();
        assert!(result.is_ok());
    }
}
