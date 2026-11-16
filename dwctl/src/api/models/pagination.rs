
use serde::{Deserialize, Serialize};
use serde_with::{DisplayFromStr, serde_as};
use utoipa::{IntoParams, ToSchema};

pub const DEFAULT_LIMIT: i64 = 10;

pub const MAX_LIMIT: i64 = 101;

#[serde_as]
#[derive(Debug, Default, Deserialize, IntoParams, ToSchema)]
pub struct Pagination {
    #[param(default = 0, minimum = 0)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub skip: Option<i64>,

    #[param(default = 10, minimum = 1, maximum = 100)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub limit: Option<i64>,
}

impl Pagination {
    #[inline]
    pub fn skip(&self) -> i64 {
        self.skip.unwrap_or(0).max(0)
    }

    #[inline]
    pub fn limit(&self) -> i64 {
        self.limit.unwrap_or(DEFAULT_LIMIT).clamp(1, MAX_LIMIT)
    }

    #[inline]
    pub fn params(&self) -> (i64, i64) {
        (self.skip(), self.limit())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PaginatedResponse<T: ToSchema> {
    pub data: Vec<T>,
    pub total_count: i64,
    pub skip: i64,
    pub limit: i64,
}

impl<T: ToSchema> PaginatedResponse<T> {
    pub fn new(data: Vec<T>, total_count: i64, skip: i64, limit: i64) -> Self {
        Self {
            data,
            total_count,
            skip,
            limit,
        }
    }
}

pub const DEFAULT_CURSOR_LIMIT: i64 = 20;

pub const MAX_CURSOR_LIMIT: i64 = 101;

#[serde_as]
#[derive(Debug, Default, Deserialize, IntoParams, ToSchema)]
pub struct CursorPagination {
    pub after: Option<String>,

    #[param(default = 20, minimum = 1, maximum = 100)]
    #[serde_as(as = "Option<DisplayFromStr>")]
    pub limit: Option<i64>,
}

impl CursorPagination {
    #[inline]
    pub fn after(&self) -> Option<&str> {
        self.after.as_deref()
    }

    #[inline]
    pub fn limit(&self) -> i64 {
        self.limit.unwrap_or(DEFAULT_CURSOR_LIMIT).clamp(1, MAX_CURSOR_LIMIT)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let p = Pagination::default();
        assert_eq!(p.skip(), 0);
        assert_eq!(p.limit(), DEFAULT_LIMIT);
    }

    #[test]
    fn test_limit_clamping() {
        let p = Pagination {
            skip: None,
            limit: Some(0),
        };
        assert_eq!(p.limit(), 1);

        let p = Pagination {
            skip: None,
            limit: Some(-5),
        };
        assert_eq!(p.limit(), 1);

        let p = Pagination {
            skip: None,
            limit: Some(1000),
        };
        assert_eq!(p.limit(), MAX_LIMIT);

        let p = Pagination {
            skip: None,
            limit: Some(50),
        };
        assert_eq!(p.limit(), 50);
    }

    #[test]
    fn test_skip_clamping() {
        let p = Pagination {
            skip: Some(-10),
            limit: None,
        };
        assert_eq!(p.skip(), 0);

        let p = Pagination {
            skip: Some(100),
            limit: None,
        };
        assert_eq!(p.skip(), 100);
    }

    #[test]
    fn test_params() {
        let p = Pagination {
            skip: Some(20),
            limit: Some(50),
        };
        assert_eq!(p.params(), (20, 50));
    }

    #[test]
    fn test_cursor_default_values() {
        let p = CursorPagination::default();
        assert_eq!(p.after(), None);
        assert_eq!(p.limit(), DEFAULT_CURSOR_LIMIT);
    }

    #[test]
    fn test_cursor_limit_clamping() {
        let p = CursorPagination {
            after: None,
            limit: Some(0),
        };
        assert_eq!(p.limit(), 1);

        let p = CursorPagination {
            after: None,
            limit: Some(1000),
        };
        assert_eq!(p.limit(), MAX_CURSOR_LIMIT);

        let p = CursorPagination {
            after: None,
            limit: Some(50),
        };
        assert_eq!(p.limit(), 50);
    }

    #[test]
    fn test_cursor_after() {
        let p = CursorPagination {
            after: Some("cursor_123".to_string()),
            limit: None,
        };
        assert_eq!(p.after(), Some("cursor_123"));
    }

    #[test]
    fn test_cursor_limit_negative() {
        let p = CursorPagination {
            after: None,
            limit: Some(-5),
        };
        assert_eq!(p.limit(), 1);
    }

    #[test]
    fn test_pagination_deserialization_from_query_string() {
        let query = "skip=10&limit=25";
        let p: Pagination = serde_urlencoded::from_str(query).unwrap();
        assert_eq!(p.skip(), 10);
        assert_eq!(p.limit(), 25);

        let query = "limit=50";
        let p: Pagination = serde_urlencoded::from_str(query).unwrap();
        assert_eq!(p.skip(), 0);
        assert_eq!(p.limit(), 50);

        let query = "skip=100";
        let p: Pagination = serde_urlencoded::from_str(query).unwrap();
        assert_eq!(p.skip(), 100);
        assert_eq!(p.limit(), DEFAULT_LIMIT);

        let query = "";
        let p: Pagination = serde_urlencoded::from_str(query).unwrap();
        assert_eq!(p.skip(), 0);
        assert_eq!(p.limit(), DEFAULT_LIMIT);
    }

    #[test]
    fn test_cursor_pagination_deserialization_from_query_string() {
        let query = "limit=11";
        let p: CursorPagination = serde_urlencoded::from_str(query).unwrap();
        assert_eq!(p.limit(), 11);
        assert_eq!(p.after(), None);

        let query = "after=cursor_abc&limit=30";
        let p: CursorPagination = serde_urlencoded::from_str(query).unwrap();
        assert_eq!(p.limit(), 30);
        assert_eq!(p.after(), Some("cursor_abc"));

        let query = "after=cursor_xyz";
        let p: CursorPagination = serde_urlencoded::from_str(query).unwrap();
        assert_eq!(p.limit(), DEFAULT_CURSOR_LIMIT);
        assert_eq!(p.after(), Some("cursor_xyz"));

        let query = "";
        let p: CursorPagination = serde_urlencoded::from_str(query).unwrap();
        assert_eq!(p.limit(), DEFAULT_CURSOR_LIMIT);
        assert_eq!(p.after(), None);
    }

    #[test]
    fn test_paginated_response_creation() {
        #[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
        struct TestItem {
            id: i64,
        }

        let items = vec![TestItem { id: 1 }, TestItem { id: 2 }];
        let response = PaginatedResponse::new(items.clone(), 100, 10, 20);

        assert_eq!(response.data.len(), 2);
        assert_eq!(response.data[0].id, 1);
        assert_eq!(response.total_count, 100);
        assert_eq!(response.skip, 10);
        assert_eq!(response.limit, 20);
    }

    #[test]
    fn test_paginated_response_serialization() {
        #[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
        struct TestItem {
            id: i64,
        }

        let items = vec![TestItem { id: 1 }];
        let response = PaginatedResponse::new(items, 50, 0, 10);

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"total_count\":50"));
        assert!(json.contains("\"skip\":0"));
        assert!(json.contains("\"limit\":10"));

        let deserialized: PaginatedResponse<TestItem> = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.total_count, 50);
        assert_eq!(deserialized.data.len(), 1);
    }
}
