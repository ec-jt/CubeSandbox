// SPDX-License-Identifier: Apache-2.0
// Local mock-Master tests. No live control plane or sandbox is contacted.
use super::build_router;
use crate::{
    config::ServerConfig,
    logging::{arc, noop::NoopLogger},
    state::AppState,
};
use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
    Json, Router,
};
use axum_test::TestServer;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct Mock {
    calls: Arc<Mutex<Vec<Value>>>,
    status: StatusCode,
    body: String,
    delay: std::time::Duration,
}

async fn master(State(mock): State<Mock>, Json(body): Json<Value>) -> (StatusCode, String) {
    mock.calls.lock().unwrap().push(body);
    tokio::time::sleep(mock.delay).await;
    (mock.status, mock.body)
}

async fn server(
    body: &str,
    status: StatusCode,
    auth: bool,
    delay: std::time::Duration,
) -> (
    TestServer,
    Arc<Mutex<Vec<Value>>>,
    tokio::task::JoinHandle<()>,
) {
    let mock = Mock {
        calls: Arc::default(),
        status,
        body: body.into(),
        delay,
    };
    let calls = mock.calls.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/cube/sandbox/update", post(master))
                .with_state(mock),
        )
        .await
        .unwrap();
    });
    let mut config = ServerConfig::default();
    config.cubemaster_url = format!("http://{address}");
    config.sandbox_domain = "cube.app".into();
    if auth {
        config.cube_api_key = Some("test-key".into());
    }
    let state = AppState::new(config, arc(NoopLogger)).await;
    (TestServer::new(build_router(state)).unwrap(), calls, task)
}

const SUCCESS: &str = r#"{"ret":{"ret_code":200,"ret_msg":"success"}}"#;
const PATH: &str = "/sandboxes/sb-1/ports/3000";

#[tokio::test]
async fn dynamic_ports_forward_wire_shape_without_count_cap() {
    let (api, calls, task) =
        server(SUCCESS, StatusCode::OK, false, std::time::Duration::ZERO).await;
    for (body, class) in [
        (json!({}), "user"),
        (json!({"portLimit":1001,"portClass":"user"}), "user"),
        (
            json!({"portLimit":-1,"portClass":"infrastructure"}),
            "infrastructure",
        ),
    ] {
        let response = api.post(PATH).json(&body).await;
        assert_eq!(response.status_code(), StatusCode::OK);
        assert_eq!(
            response.json::<Value>(),
            json!({"containerPort":3000,"publicURL":"https://3000-sb-1.cube.app"})
        );
        let requests = calls.lock().unwrap();
        let forwarded = requests.last().unwrap();
        assert_eq!(forwarded["action"], "exposePort");
        assert_eq!(forwarded["sandbox_id"], "sb-1");
        assert_eq!(forwarded["container_port"], 3000);
        assert_eq!(forwarded["port_class"], class);
        assert_eq!(forwarded.get("port_limit"), body.get("portLimit"));
        assert!(forwarded["requestID"].is_string());
        assert!(forwarded.get("timeout").is_none());
    }
    assert_eq!(
        api.delete(PATH)
            .json(&json!({"portClass":"ignored"}))
            .await
            .status_code(),
        StatusCode::NO_CONTENT
    );
    let requests = calls.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[3]["action"], "closePort");
    assert!(requests[3].get("port_class").is_none());
    assert!(requests[3].get("port_limit").is_none());
    task.abort();
}

#[tokio::test]
async fn dynamic_ports_reject_invalid_input_before_forwarding() {
    let (api, calls, task) =
        server(SUCCESS, StatusCode::OK, false, std::time::Duration::ZERO).await;
    for port in [
        "0", "65536", "-1", "true", "1.5", "17300", "17301", "49983", "49999",
    ] {
        let path = format!("/sandboxes/sb-1/ports/{port}");
        for class in ["user", "infrastructure"] {
            assert_eq!(
                api.post(&path)
                    .json(&json!({"portClass":class}))
                    .await
                    .status_code(),
                StatusCode::BAD_REQUEST
            );
        }
        assert_eq!(
            api.delete(&path).await.status_code(),
            StatusCode::BAD_REQUEST
        );
    }
    for body in [
        json!({"portClass":"admin"}),
        json!({"portClass":""}),
        json!({"portClass":true}),
        json!({"portLimit":true}),
        json!({"portLimit":1.2}),
        json!({"portLimit":"100"}),
        json!(null),
    ] {
        assert_eq!(
            api.post(PATH).json(&body).await.status_code(),
            StatusCode::BAD_REQUEST
        );
    }
    assert!(calls.lock().unwrap().is_empty());
    task.abort();
}

#[tokio::test]
async fn dynamic_ports_require_explicit_success_and_preserve_business_errors() {
    for (body, status, expected) in [
        ("null", StatusCode::OK, 500),
        ("{}", StatusCode::OK, 500),
        (r#"{"ret":null}"#, StatusCode::OK, 500),
        ("not json", StatusCode::OK, 500),
        (
            r#"{"ret":{"ret_code":"200","ret_msg":"bad"}}"#,
            StatusCode::OK,
            500,
        ),
        (
            r#"{"ret":{"ret_code":0,"ret_msg":"legacy"}}"#,
            StatusCode::OK,
            500,
        ),
        (
            r#"{"ret":{"ret_code":130404,"ret_msg":"missing"}}"#,
            StatusCode::OK,
            404,
        ),
        (
            r#"{"ret":{"ret_code":130409,"ret_msg":"conflict"}}"#,
            StatusCode::OK,
            409,
        ),
        (
            r#"{"ret":{"ret_code":500,"ret_msg":"failed"}}"#,
            StatusCode::OK,
            500,
        ),
        (SUCCESS, StatusCode::INTERNAL_SERVER_ERROR, 500),
    ] {
        let (api, calls, task) = server(body, status, false, std::time::Duration::ZERO).await;
        assert_eq!(
            api.post(PATH).json(&json!({})).await.status_code().as_u16(),
            expected,
            "{body}"
        );
        assert_eq!(
            api.delete(PATH).await.status_code().as_u16(),
            expected,
            "{body}"
        );
        assert_eq!(calls.lock().unwrap().len(), 2, "no retries");
        task.abort();
    }
}

#[tokio::test]
async fn dynamic_ports_shared_key_auth_before_forwarding() {
    let (api, calls, task) = server(SUCCESS, StatusCode::OK, true, std::time::Duration::ZERO).await;
    assert_eq!(
        api.post(PATH).json(&json!({})).await.status_code(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        api.delete(PATH).await.status_code(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        api.post(PATH)
            .add_header("X-API-Key".parse().unwrap(), "wrong".parse().unwrap())
            .json(&json!({}))
            .await
            .status_code(),
        StatusCode::UNAUTHORIZED
    );
    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(
        api.post(PATH)
            .add_header("X-API-Key".parse().unwrap(), "test-key".parse().unwrap())
            .json(&json!({}))
            .await
            .status_code(),
        StatusCode::OK
    );
    assert_eq!(
        api.delete(PATH)
            .add_header(
                "Authorization".parse().unwrap(),
                "Bearer test-key".parse().unwrap()
            )
            .await
            .status_code(),
        StatusCode::NO_CONTENT
    );
    assert_eq!(calls.lock().unwrap().len(), 2);
    task.abort();
}

#[tokio::test]
async fn dynamic_ports_callback_can_deny_other_owners_before_forwarding() {
    // CubeAPI has no tenant store. Ownership is an external callback decision.
    let callbacks = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let seen = callbacks.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route(
                "/auth",
                post(move |headers: HeaderMap| {
                    let seen = seen.clone();
                    async move {
                        assert_eq!(headers["X-API-Key"], "tenant-key");
                        seen.lock().unwrap().push((
                            headers["X-Request-Path"].to_str().unwrap().into(),
                            headers["X-Request-Method"].to_str().unwrap().into(),
                        ));
                        StatusCode::FORBIDDEN
                    }
                }),
            ),
        )
        .await
        .unwrap();
    });
    let mut config = ServerConfig::default();
    config.cubemaster_url = "http://127.0.0.1:9".into();
    config.auth_callback_url = Some(format!("http://{address}/auth"));
    let api = TestServer::new(build_router(AppState::new(config, arc(NoopLogger)).await)).unwrap();
    assert_eq!(
        api.post(PATH)
            .add_header("X-API-Key".parse().unwrap(), "tenant-key".parse().unwrap())
            .json(&json!({}))
            .await
            .status_code(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        api.delete(PATH)
            .add_header("X-API-Key".parse().unwrap(), "tenant-key".parse().unwrap())
            .await
            .status_code(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        *callbacks.lock().unwrap(),
        vec![(PATH.into(), "POST".into()), (PATH.into(), "DELETE".into())]
    );
    task.abort();
}

#[tokio::test]
async fn dynamic_ports_transport_failure_is_not_success() {
    let mut config = ServerConfig::default();
    config.cubemaster_url = "http://127.0.0.1:9".into();
    let api = TestServer::new(build_router(AppState::new(config, arc(NoopLogger)).await)).unwrap();
    assert_eq!(
        api.post(PATH).json(&json!({})).await.status_code(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
    assert_eq!(
        api.delete(PATH).await.status_code(),
        StatusCode::INTERNAL_SERVER_ERROR
    );
}

#[tokio::test]
async fn dynamic_ports_deadline_is_bounded_without_retry() {
    let (api, calls, task) = server(
        SUCCESS,
        StatusCode::OK,
        false,
        std::time::Duration::from_secs(60),
    )
    .await;
    let start = std::time::Instant::now();
    let response = api.post(PATH).json(&json!({})).await;
    assert_eq!(response.status_code(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(start.elapsed() >= std::time::Duration::from_secs(30));
    assert!(start.elapsed() < super::PORT_ROUTE_TIMEOUT);
    assert_eq!(calls.lock().unwrap().len(), 1);
    task.abort();
}
