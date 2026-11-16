
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::api::models::users::UserResponse;

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RegistrationInfo {
    pub enabled: bool,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct LoginInfo {
    pub enabled: bool,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct RegisterRequest {
    pub username: String,
    pub email: String,
    pub password: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AuthResponse {
    pub user: UserResponse,
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct AuthSuccessResponse {
    pub message: String,
}

use axum::{
    Json,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};

pub struct RegisterResponse {
    pub auth_response: AuthResponse,
    pub cookie: String,
}

impl IntoResponse for RegisterResponse {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(header::SET_COOKIE, self.cookie.parse().unwrap());
        (StatusCode::CREATED, headers, Json(self.auth_response)).into_response()
    }
}

pub struct LoginResponse {
    pub auth_response: AuthResponse,
    pub cookie: String,
}

impl IntoResponse for LoginResponse {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.insert(header::SET_COOKIE, self.cookie.parse().unwrap());
        (StatusCode::OK, headers, Json(self.auth_response)).into_response()
    }
}

pub struct LogoutResponse {
    pub auth_response: AuthSuccessResponse,
    pub cookie: String,
    pub extra_cookies: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PasswordResetRequest {
    pub email: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PasswordResetConfirmRequest {
    pub token: String,
    pub new_password: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct PasswordResetResponse {
    pub message: String,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct ChangePasswordRequest {
    pub current_password: String,
    pub new_password: String,
}

impl IntoResponse for LogoutResponse {
    fn into_response(self) -> Response {
        let mut headers = HeaderMap::new();
        headers.append(header::SET_COOKIE, self.cookie.parse().unwrap());
        for cookie in &self.extra_cookies {
            headers.append(header::SET_COOKIE, cookie.parse().unwrap());
        }
        (StatusCode::OK, headers, Json(self.auth_response)).into_response()
    }
}
