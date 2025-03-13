use axum::{
    response::IntoResponse,
    routing::{post, get, Router},
    extract::{Multipart, State},
    extract::{ws::{WebSocketUpgrade, Message, WebSocket}},
    http::Method,
    Json,
};
use futures::stream::StreamExt;

use serde_json::json;
use tokio::fs;
use std::{hash::Hash, path::Path};
use tower_http::cors::{AllowCredentials, Any, CorsLayer};
use serde::Deserialize;
use std::collections::HashMap;
use lazy_static::lazy_static;
use once_cell::sync::Lazy;
use tokio::sync::RwLock;
use axum::{response::{Response}};
use hyper::StatusCode;
use serde::{Serialize};
use std::sync::Arc;

mod main_process {
    pub mod main_processer;
}
use main_process::main_processer::NodeManager;
use main_process::main_processer::ProcessedNode;

mod nodes;

use sqlx::{decode, sqlite::SqlitePool};
use sqlx::{MySqlPool, mysql::MySqlQueryResult};
use sqlx::query;
use axum_extra::extract::cookie::{Cookie, CookieJar};
use rand::prelude::*;
mod verification_custom;
use http::header::{AUTHORIZATION, CONTENT_TYPE, COOKIE};
use tokio::task;
use tokio::fs::File;
use tokio::spawn;
use tokio::io::AsyncWriteExt; // Import the trait for write_all
use base64::Engine;
// TODO: Multiple Users (half done)
lazy_static! {
    static ref FILE_STORE: RwLock<HashMap<String, String>> = RwLock::new(HashMap::new());
    static ref NODE_DICT: RwLock<HashMap<u32, NodePayload>> = RwLock::new(HashMap::new());
}

pub type FileStore = HashMap<String, String>;
pub type NodeDict = HashMap<u32, NodePayload>;
#[derive(Clone)]
pub struct SessionData {
    pub file_store: Arc<RwLock<FileStore>>,
    pub node_dict: Arc<RwLock<NodeDict>>,
}

pub static SESSIONS: Lazy<RwLock<HashMap<String, SessionData>>> = Lazy::new(|| {
    RwLock::new(HashMap::new())
});

pub async fn get_session(session_id: &str) -> Option<SessionData> {
    SESSIONS
        .read()
        .await
        .get(session_id)
        .cloned()
}

pub async fn create_session(session_id: &str) -> SessionData {
    // Create a new SessionData.
    let new_session = SessionData {
        file_store: Arc::new(RwLock::new(HashMap::new())),
        node_dict: Arc::new(RwLock::new(HashMap::new())),
    };

    // Insert the new session into the global sessions dictionary.
    {
        let mut sessions = SESSIONS.write().await;
        sessions.insert(session_id.to_string(), new_session.clone());
    }

    // Retrieve the session.
    get_session(session_id)
        .await
        .expect("Session should exist after creation")
}

#[derive(Deserialize)]
struct RegisterUser {
    email: String,
    password: String, 
}
#[derive(Serialize)]

struct ApiResponse {
    message: String,
}

#[derive(Serialize)]
struct LoginResponse {
    message: String,
}

#[derive(Deserialize, Clone, Debug)]
pub struct NodePayload {
    node_id: u32,
    r#type: String, // Use `r#type` to avoid conflict with the Rust `type` keyword
    neighbors_dependent: Vec<u32>,
    neighbors_pointing: Vec<u32>,
    data: String, 
}

#[derive(Deserialize, Debug)]
struct FilePayload {
    id: String,
    file_name: String,
    file_type: String,
    file_data: String,  // Base64 encoded file content
}

impl PartialEq for NodePayload {
    fn eq(&self, other: &Self) -> bool {
        self.data == other.data
    }
}

async fn register_user(
    State(pool): State<Arc<MySqlPool>>,
    Json(user): Json<RegisterUser>,
) -> impl IntoResponse {
    let exists = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM users WHERE email = ?")
        .bind(&user.email)
        .fetch_one(&*pool)
        .await
        .unwrap_or(0);

    if exists > 0 {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                message: "User with this email already exists".to_string(),
            }),
        );
    }

    let query = "INSERT INTO users (email, password) VALUES (?, ?)";
    
    let result = sqlx::query(query)
        .bind(user.email)
        .bind(user.password)
        .execute(&*pool)  // Dereference Arc<SqlitePool>
        .await;
    print!("Result: {:?}", result);
    match result {
        Ok(_) => (
            StatusCode::OK,
            Json(ApiResponse {
                message: "User registered".to_string(),
            }),
        ),
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse {
                message: "Error registering user".to_string(),
            }),
        ),
    }
}

async fn login_user(
    State(pool): State<Arc<MySqlPool>>,
    jar: CookieJar,  // Handle cookies
    Json(user): Json<RegisterUser>,
) -> impl IntoResponse {
    let stored_password = sqlx::query_scalar::<_, String>("SELECT password FROM users WHERE email = ?")
        .bind(&user.email)
        .fetch_one(&*pool)
        .await
        .unwrap_or("".to_string());

    if stored_password.is_empty() {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            jar,
            Json(LoginResponse {
                message: "User not found".to_string(),
            }),
        );
    }

    if stored_password != user.password {
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            jar,
            Json(LoginResponse {
                message: "Invalid password".to_string(),
            }),
        );
    }

    let mut chars: Vec<char> = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        .chars()
        .collect();

    chars.shuffle(&mut rand::rng());

    let session_id: String = chars.iter().take(32).collect();

    sqlx::query(
        "INSERT INTO sessions (session_id, email) VALUES (?, ?)"
    )
    .bind(&session_id)
    .bind(user.email) // Retrieved from login process
    .execute(&*pool)
    .await
    .unwrap();

    let cookie: Cookie = Cookie::build(("session_id", session_id))
        .domain("localhost")
        .path("/")
        .http_only(false)
        .build();

    let jar = jar.add(cookie);

    (
        StatusCode::OK,
        jar,
        Json(LoginResponse {
            message: "Login successful".to_string(),
        }),
    )
}

async fn websocket_upgrade(ws: WebSocketUpgrade, jar: CookieJar, State(pool): State<Arc<MySqlPool>>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| async move {
        let session_id = jar
            .get("session_id")
            .map(|cookie| cookie.value().to_string())
            .unwrap_or_else(|| "Unknown".to_string());

        // Now that we're inside an async closure, we can await:
        let session = create_session(&session_id).await; // TODO: verify if there is already an active session 

        println!("🔑 User session ID: {}", session_id);

        match sqlx::query_scalar::<_, String>(
            "SELECT email FROM sessions WHERE session_id = ?"
        )
        .bind(&session_id)
        .fetch_one(&*pool)
        .await
        {
            Ok(email) => {
                println!("📩 Found email: {}", email);

                // Create session object if necessary
                let session = create_session(&session_id).await; 

                // Spawn a task to handle WebSocket connection
                spawn(async move {
                    handle_socket(socket, session, email).await;
                });
            }
            Err(_) => {
                println!("❌ No active session found for session_id: {}", session_id);
                return; // Exit early if session ID is invalid
            }
        }
    })
}
async fn handle_socket(mut socket: WebSocket, session: SessionData, email: String) {
    println!("✅ WebSocket connection established");

    while let Some(Ok(msg)) = socket.next().await {
        match msg {
            Message::Text(text) => {
                println!("📩 Received WebSocket message: {}", text); // Debug: Log incoming messages

                // Try parsing as `NodePayload`
                match serde_json::from_str::<Vec<NodePayload>>(&text) {
                    Ok(nodes) => {
                        println!("✅ Parsed as NodePayload, processing nodes...");
                        for node in nodes {
                            handle_node(node, &session).await;
                        }
                        continue;
                    }
                    Err(err) => {
                        println!("⚠️ Failed to parse as NodePayload: {}", err);
                    }
                }

                // Try parsing as `FilePayload`
                match serde_json::from_str::<FilePayload>(&text) {
                    Ok(file_payload) => {
                        println!("✅ Parsed as FilePayload");
                        println!("📂 Received file: {} (ID: {})", file_payload.file_name, file_payload.id);

                        let dir_path = format!("./uploads/{}/{}/", email, file_payload.id); // TODO: Also add the project_id the user is current on 
                        let file_path = format!("{}/{}", dir_path, file_payload.file_name);
                        
                        if let Err(err) = fs::create_dir_all(&dir_path).await {
                            println!("❌ Failed to create directory {}: {}", dir_path, err);
                            continue;
                        }

                        match base64::engine::general_purpose::STANDARD.decode(&file_payload.file_data) {
                            Ok(decoded_bytes) => {
                                match File::create(&file_path).await {
                                    Ok(mut file) => {
                                        if let Ok(_) = file.write_all(&decoded_bytes).await {
                                            println!("✅ File saved at {}", file_path);
                                        } else {
                                            println!("❌ Failed to write file to disk");
                                        }
                                    }
                                    Err(err) => {
                                        println!("❌ Failed to create file {}: {}", file_path, err);
                                    }
                                }
                            }
                            Err(err) => {
                                println!("❌ Failed to decode Base64 file data: {}", err);
                            }
                        }
                    }
                    Err(err) => {
                        println!("❌ Failed to parse as FilePayload: {}", err);
                    }
                }
            }
            Message::Close(_) => {
                println!("❌ Client disconnected");
                return;
            }
            _ => {
                println!("⚠️ Received an unsupported WebSocket message type");
            }
        }
    }
}

async fn add_to_node_store(session: &SessionData, key: u32, value: NodePayload) {
    let mut node_lock = session.node_dict.write().await;
    
    if let Some(existing_value) = node_lock.get_mut(&key) { // TODO: make more efficient
        *existing_value = value;
    } else {
        println!("Inserting new Node ID {}", key);
        node_lock.insert(key, value);
    }
}

async fn add_to_file_store(key: String, value: String) {
    let mut store_lock = FILE_STORE.write().await;
    store_lock.insert(key, value);
}

async fn upload_csv(mut multipart: Multipart) -> impl IntoResponse {
    let mut id: Option<String> = None;
    let mut file_path: Option<String> = None;

    while let Some(field) = multipart.next_field().await.unwrap() {
        if let Some(name) = field.name() {
            if name == "id" {
                id = Some(field.text().await.unwrap());
            } else if name == "file" {
                if let Some(file_name) = field.file_name() {
                    let path = format!("./uploads/{}", file_name);
                    let bytes = field.bytes().await.unwrap();

                    // Write file to disk
                    if let Ok(_) = tokio::fs::write(&path, bytes).await {
                        file_path = Some(path);
                    }
                }
            }
        }
    }

    if let (Some(id), Some(file_path)) = (id, file_path) {
        // Store in the global dictionary using the helper function
        add_to_file_store(id.clone(), file_path.clone()).await;
    
        // Return success response
        return Json(json!({
            "status": "success",
            "message": format!("File saved to {}", file_path),
            "id": id
        }));
    }
    

    return Json(json!({
        "status": "error",
        "message": format!("File not saved! ")
    }));
}

async fn handle_node(node: NodePayload, session: &SessionData,) {
    println!(
        "Received payload: ID = {}, Type = {}, Neighbors Dependent = {:?}, Neighbors Pointing = {:?}, Data = {}",
        node.node_id,
        node.r#type,
        node.neighbors_dependent,
        node.neighbors_pointing,
        node.data
    );
    add_to_node_store(session, node.node_id, node).await;
}

async fn deep_copy_node_dict() -> HashMap<u32, NodePayload> {
    let store_lock = NODE_DICT.read().await; // Acquire a read lock
    let deep_copy = store_lock.clone(); // Clone the entire HashMap
    deep_copy
}

async fn deep_copy_file_dict() -> HashMap<String, String> {
    let store_lock = FILE_STORE.read().await; // Acquire a read lock
    let deep_copy = store_lock.clone(); // Clone the entire HashMap
    deep_copy
}
pub struct ProcessedNodesResponse(pub Json<Vec<ProcessedNode>>);

impl IntoResponse for ProcessedNodesResponse {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::OK, self.0).into_response()
    }
}

async fn process_nodes() -> impl IntoResponse {
    {
        let store_lock: tokio::sync::RwLockReadGuard<'_, HashMap<u32, NodePayload>> = NODE_DICT.read().await;
        for (node_id, node) in store_lock.iter() {
            println!(
                "Stored Node: ID = {}, Type = {}, Neighbors Dependent = {:?}, Neighbors Pointing = {:?}, Node Data = {}",
                node_id, node.r#type, node.neighbors_dependent, node.neighbors_pointing, node.data
            );
        }
    }
    let mut id_queue: Vec<u32> = Vec::new();
    {
        let store_lock: tokio::sync::RwLockReadGuard<'_, HashMap<u32, NodePayload>> = NODE_DICT.read().await;
        for (id, payload) in store_lock.iter() {
            if payload.neighbors_dependent.is_empty() {
                id_queue.push(*id);
            }
        }
    }


    let mut copied_node_dict: HashMap<u32, NodePayload> = deep_copy_node_dict().await;
    let mut results: Vec<u32> = Vec::new();
    while !id_queue.is_empty() {
        let id = id_queue.remove(0);
        results.push(id);
    
        let neighbors_pointing: Vec<u32> = copied_node_dict
            .get(&id) 
            .unwrap()
            .neighbors_pointing
            .clone(); 
    
        for neighbor_id in neighbors_pointing {
            if let Some(neighbor_node) = copied_node_dict.get_mut(&neighbor_id) {
                neighbor_node.neighbors_dependent.retain(|&dependent_id| dependent_id != id);
                if neighbor_node.neighbors_dependent.is_empty() {
                    id_queue.push(neighbor_id);
                }
            }
        }        
    }
    
    // if results.len() != copied_node_dict.len() {
    //     println!("Cycle detected in the graph!");
    //     return ProcessedNodesResponse(Json(vec![]));
    // }

    println!("Results: {:?}", results);

    let copied_node_dict: HashMap<u32, NodePayload> = deep_copy_node_dict().await;
    let mut copied_file_dict: HashMap<String, String> = deep_copy_file_dict().await;
    let mut manager = NodeManager::new(results, &copied_node_dict, &mut copied_file_dict);
    
    manager.print_state();
    let defults = manager.process_nodes_in_order(); // Extract Vec<ProcessedNode> from Json
    defults
}

#[tokio::main]
async fn main() {
    let db_url = "mysql://root:Zlh615915%23@127.0.0.1:3306/mydb"; // MySQL Connection URL

    let cors = CorsLayer::new()
    .allow_origin("http://localhost:5173".parse::<axum::http::HeaderValue>().unwrap()) // Allow your frontend origin
    .allow_methods([Method::GET, Method::POST]) // Allow specific HTTP methods
    .allow_headers([CONTENT_TYPE, AUTHORIZATION, COOKIE]) // Allow all headers (or specify the ones you need)
    .allow_credentials(true); // Allow cookies
    
    // Ensure the upload directory exists
    let upload_dir = Path::new("./uploads");
    if !upload_dir.exists() {
        fs::create_dir(upload_dir).await.unwrap();
    }

    let pool = MySqlPool::connect(db_url).await.unwrap();
    let pool = Arc::new(pool);

    query(
        "CREATE TABLE IF NOT EXISTS users (
            email VARCHAR(255) PRIMARY KEY NOT NULL,
            password VARCHAR(225) NOT NULL
        )"
    )
    .execute(&*pool)
    .await
    .unwrap();

    query(
        "CREATE TABLE IF NOT EXISTS sessions (
            session_id CHAR(36) PRIMARY KEY,
            email VARCHAR(255) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(email) REFERENCES users(email) ON DELETE CASCADE
        )"
    )
    .execute(&*pool)
    .await
    .unwrap();

    // Define Route
    let app = Router::new()
        .route("/upload-csv", post(upload_csv))
        .route("/process-nodes", post(process_nodes))
        .route("/signup", post(register_user))
        .route("/login", post(login_user))
        .route("/ws", get(websocket_upgrade))
        .with_state(pool.clone())
        .layer(cors);

    println!("Running on http://localhost:3000");
    // Start Server
    axum_server::bind("127.0.0.1:3000".parse().unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}
