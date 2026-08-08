use anyhow::{Context, Result};
use reqwest::{Client, StatusCode};
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::info;
use uuid::Uuid;

use crate::db::{execute_async, DbPool};
use crate::types::{
    NodeGroupObservation, ObservationIngestRequest, OrchestratorAction, OrchestratorIntent,
    WorkloadObservation, WorkloadPolicyRequest, WorkloadSloRequest,
};

const FAST_LOOP_SECONDS: u64 = 5;
const SLOW_LOOP_SECONDS: u64 = 120;
const DISPATCH_LOOP_SECONDS: u64 = 2;
const MAX_DISPATCH_BATCH: usize = 100;
const PLATFORM_TENANT_ID: &str = "platform";

const RUNTIME_NON_TERMINAL: [&str; 3] = ["accepted", "queued", "running"];
const RUNTIME_SUCCESS: &str = "succeeded";
const RUNTIME_FAILURES: [&str; 3] = ["failed", "cancelled", "timed_out"];

const RC_TARGET_NOT_FOUND: &str = "ELASTICITY_TARGET_NOT_FOUND";
const RC_INVALID_ARGUMENT: &str = "ELASTICITY_INVALID_ARGUMENT";
const RC_RESOURCE_PRESSURE: &str = "ELASTICITY_RESOURCE_PRESSURE";
const RC_OPERATION_FAILED: &str = "ELASTICITY_OPERATION_FAILED";
const RC_IDEMPOTENCY_REPLAY: &str = "ELASTICITY_IDEMPOTENCY_REPLAY";
const RC_ORCH_TTL_EXPIRED: &str = "ORCH_TTL_EXPIRED";
const RC_ORCH_ROLLBACK_TRIGGERED: &str = "ORCH_ROLLBACK_TRIGGERED";
const RC_ORCH_ACTION_UNSUPPORTED: &str = "ORCH_ACTION_UNSUPPORTED";
const RC_ORCH_DEPENDENCY_UNAVAILABLE: &str = "ORCH_DEPENDENCY_UNAVAILABLE";

#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    pub control_base_url: Option<String>,
    pub control_api_key: Option<String>,
    pub local_scheduler: bool,
}

#[derive(Debug, Clone)]
struct WorkloadPolicyRow {
    runtime_function_id: String,
    max_concurrency: u32,
    hard_quota: u32,
    soft_burst: u32,
    absolute_limit: u32,
    cooldown_seconds: u32,
    hysteresis_pct: f64,
    burst_cpu_cap: f64,
    burst_mem_mb: u32,
    burst_ttl_seconds: u32,
    target_container_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct WorkloadSloRow {
    p95_latency_ms: u32,
    max_cold_start_pct: f64,
    max_reject_pct: f64,
    max_cost_per_compute_unit: f64,
}

#[derive(Debug, Clone)]
struct DispatchAction {
    action_id: String,
    tenant_id: String,
    workload_id: String,
    action_type: String,
    payload_json: Value,
    ttl_seconds: u32,
    rollback_action_json: Option<Value>,
    idempotency_key: String,
    status: String,
    effective_at: i64,
    runtime_operation_id: Option<String>,
    attempt_count: u32,
    created_at: i64,
}

#[derive(Debug, Clone)]
struct RuntimeOperation {
    operation_id: String,
    operation_type: Option<String>,
    status: String,
    reason_code: Option<String>,
    reason_message: Option<String>,
}

fn now_unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock should be monotonic enough")
        .as_secs() as i64
}

fn clamp_u32(v: i64, min: u32, max: u32) -> u32 {
    if v < min as i64 {
        return min;
    }
    if v > max as i64 {
        return max;
    }
    v as u32
}

fn canonical_json(value: &Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<_> = map.keys().cloned().collect();
            keys.sort_unstable();
            let mut new_map = Map::new();
            for key in keys {
                if let Some(v) = map.get(&key) {
                    new_map.insert(key, canonical_json(v));
                }
            }
            Value::Object(new_map)
        }
        Value::Array(items) => Value::Array(items.iter().map(canonical_json).collect()),
        _ => value.clone(),
    }
}

fn decision_window_start(now: i64, window_seconds: i64) -> i64 {
    now - (now % window_seconds)
}

fn deterministic_idempotency_key(
    tenant_id: &str,
    workload_id: &str,
    action_type: &str,
    payload: &Value,
    decision_window_start: i64,
) -> String {
    let canonical = canonical_json(payload).to_string();
    let input = format!(
        "{}|{}|{}|{}|{}",
        tenant_id, workload_id, action_type, canonical, decision_window_start
    );
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

fn is_runtime_non_terminal(status: &str) -> bool {
    RUNTIME_NON_TERMINAL.contains(&status)
}

fn is_runtime_terminal_success(status: &str) -> bool {
    status == RUNTIME_SUCCESS
}

fn is_runtime_terminal_failure(status: &str) -> bool {
    RUNTIME_FAILURES.contains(&status)
}

fn normalize_reason_code(code: Option<&str>) -> String {
    match code {
        Some(RC_TARGET_NOT_FOUND) => RC_TARGET_NOT_FOUND.to_string(),
        Some(RC_INVALID_ARGUMENT) => RC_INVALID_ARGUMENT.to_string(),
        Some(RC_RESOURCE_PRESSURE) => RC_RESOURCE_PRESSURE.to_string(),
        Some(RC_OPERATION_FAILED) => RC_OPERATION_FAILED.to_string(),
        Some(RC_IDEMPOTENCY_REPLAY) => RC_IDEMPOTENCY_REPLAY.to_string(),
        Some(other) => other.to_string(),
        None => RC_OPERATION_FAILED.to_string(),
    }
}

fn parse_dispatch_error(message: &str) -> (String, Option<bool>, String) {
    let mut parts = message.splitn(3, '|');
    let first = parts.next();
    let second = parts.next();
    let third = parts.next();
    match (first, second, third) {
        (Some(code), Some(retry_tag), Some(detail)) if retry_tag.starts_with("retryable=") => {
            let retryable = match retry_tag.trim_start_matches("retryable=") {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            };
            (code.to_string(), retryable, detail.to_string())
        }
        _ => {
            let known = [
                RC_INVALID_ARGUMENT,
                RC_TARGET_NOT_FOUND,
                RC_RESOURCE_PRESSURE,
                RC_OPERATION_FAILED,
                RC_IDEMPOTENCY_REPLAY,
                RC_ORCH_ACTION_UNSUPPORTED,
                RC_ORCH_DEPENDENCY_UNAVAILABLE,
            ];
            for code in known {
                if message.starts_with(code) {
                    return (code.to_string(), None, message.to_string());
                }
            }
            (
                RC_ORCH_DEPENDENCY_UNAVAILABLE.to_string(),
                None,
                message.to_string(),
            )
        }
    }
}

pub async fn upsert_workload_policy(
    db: &DbPool,
    tenant_id: &str,
    workload_id: &str,
    req: WorkloadPolicyRequest,
) -> Result<()> {
    if req.runtime_function_id.trim().is_empty() {
        anyhow::bail!("runtime_function_id must be non-empty");
    }
    let now = now_unix_seconds();
    let tenant_id = tenant_id.to_string();
    let workload_id = workload_id.to_string();
    execute_async(db, move |conn| {
        conn.execute(
            "INSERT INTO orchestrator_workload_policy
             (tenant_id, workload_id, runtime_function_id, max_concurrency, hard_quota, soft_burst, absolute_limit, priority, cooldown_seconds, hysteresis_pct, burst_cpu_cap, burst_mem_mb, burst_ttl_seconds, target_container_ids_json, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
             ON CONFLICT(tenant_id, workload_id) DO UPDATE SET
               runtime_function_id=excluded.runtime_function_id,
               max_concurrency=excluded.max_concurrency,
               hard_quota=excluded.hard_quota,
               soft_burst=excluded.soft_burst,
               absolute_limit=excluded.absolute_limit,
               priority=excluded.priority,
               cooldown_seconds=excluded.cooldown_seconds,
               hysteresis_pct=excluded.hysteresis_pct,
               burst_cpu_cap=excluded.burst_cpu_cap,
               burst_mem_mb=excluded.burst_mem_mb,
               burst_ttl_seconds=excluded.burst_ttl_seconds,
               target_container_ids_json=excluded.target_container_ids_json,
               updated_at=excluded.updated_at",
            params![
                tenant_id,
                workload_id,
                req.runtime_function_id,
                req.max_concurrency,
                req.hard_quota,
                req.soft_burst,
                req.absolute_limit,
                req.priority,
                req.cooldown_seconds,
                req.hysteresis_pct,
                req.burst_cpu_cap,
                req.burst_mem_mb,
                req.burst_ttl_seconds,
                serde_json::to_string(&req.target_container_ids)?,
                now
            ],
        )
        .context("Failed to upsert workload policy")?;
        Ok(())
    })
    .await
}

pub async fn upsert_workload_slo(
    db: &DbPool,
    tenant_id: &str,
    workload_id: &str,
    req: WorkloadSloRequest,
) -> Result<()> {
    let now = now_unix_seconds();
    let tenant_id = tenant_id.to_string();
    let workload_id = workload_id.to_string();
    execute_async(db, move |conn| {
        conn.execute(
            "INSERT INTO orchestrator_workload_slo
             (tenant_id, workload_id, p95_latency_ms, max_cold_start_pct, max_reject_pct, rto_seconds, max_cost_per_compute_unit, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(tenant_id, workload_id) DO UPDATE SET
               p95_latency_ms=excluded.p95_latency_ms,
               max_cold_start_pct=excluded.max_cold_start_pct,
               max_reject_pct=excluded.max_reject_pct,
               rto_seconds=excluded.rto_seconds,
               max_cost_per_compute_unit=excluded.max_cost_per_compute_unit,
               updated_at=excluded.updated_at",
            params![
                tenant_id,
                workload_id,
                req.p95_latency_ms,
                req.max_cold_start_pct,
                req.max_reject_pct,
                req.rto_seconds,
                req.max_cost_per_compute_unit,
                now
            ],
        )
        .context("Failed to upsert workload SLO")?;
        Ok(())
    })
    .await
}

pub async fn ingest_observations(db: &DbPool, req: ObservationIngestRequest) -> Result<()> {
    let now = now_unix_seconds();
    execute_async(db, move |conn| {
        let tx = conn.unchecked_transaction()?;
        for w in req.workloads {
            tx.execute(
                "INSERT INTO orchestrator_workload_observation
                 (tenant_id, workload_id, node_group, queue_depth, cpu_pressure, mem_pressure, io_pressure, cold_start_pct, invoke_p95_ms, reject_pct, active_compute_units, cost_per_compute_unit, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
                 ON CONFLICT(tenant_id, workload_id) DO UPDATE SET
                   node_group=excluded.node_group,
                   queue_depth=excluded.queue_depth,
                   cpu_pressure=excluded.cpu_pressure,
                   mem_pressure=excluded.mem_pressure,
                   io_pressure=excluded.io_pressure,
                   cold_start_pct=excluded.cold_start_pct,
                   invoke_p95_ms=excluded.invoke_p95_ms,
                   reject_pct=excluded.reject_pct,
                   active_compute_units=excluded.active_compute_units,
                   cost_per_compute_unit=excluded.cost_per_compute_unit,
                   updated_at=excluded.updated_at",
                params![
                    w.tenant_id,
                    w.workload_id,
                    w.node_group,
                    w.queue_depth,
                    w.cpu_pressure,
                    w.mem_pressure,
                    w.io_pressure,
                    w.cold_start_pct,
                    w.invoke_p95_ms,
                    w.reject_pct,
                    w.active_compute_units,
                    w.cost_per_compute_unit,
                    now
                ],
            )?;
        }
        for n in req.node_groups {
            tx.execute(
                "INSERT INTO orchestrator_node_group_observation
                 (node_group, cpu_pressure, mem_pressure, io_pressure, warm_ready, warm_hit_rate, capacity_units, used_units, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
                 ON CONFLICT(node_group) DO UPDATE SET
                   cpu_pressure=excluded.cpu_pressure,
                   mem_pressure=excluded.mem_pressure,
                   io_pressure=excluded.io_pressure,
                   warm_ready=excluded.warm_ready,
                   warm_hit_rate=excluded.warm_hit_rate,
                   capacity_units=excluded.capacity_units,
                   used_units=excluded.used_units,
                   updated_at=excluded.updated_at",
                params![
                    n.node_group,
                    n.cpu_pressure,
                    n.mem_pressure,
                    n.io_pressure,
                    n.warm_ready,
                    n.warm_hit_rate,
                    n.capacity_units,
                    n.used_units,
                    now
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    })
    .await
}

pub async fn list_intents(db: &DbPool) -> Result<Vec<OrchestratorIntent>> {
    execute_async(db, move |conn| {
        let mut stmt = conn.prepare(
            "SELECT tenant_id, workload_id, target_concurrency, burst_cpu_cap, burst_mem_mb, burst_ttl_seconds,
                    pool_min_ready, pool_target_ready, pool_max_ready, preferred_node_group, anti_affinity,
                    reason_code, effective_at, ttl_seconds, updated_at
             FROM orchestrator_intent
             ORDER BY tenant_id, workload_id",
        )?;
        let rows = stmt
            .query_map([], |row| {
                Ok(OrchestratorIntent {
                    tenant_id: row.get(0)?,
                    workload_id: row.get(1)?,
                    target_concurrency: row.get(2)?,
                    burst_cpu_cap: row.get(3)?,
                    burst_mem_mb: row.get(4)?,
                    burst_ttl_seconds: row.get(5)?,
                    pool_min_ready: row.get(6)?,
                    pool_target_ready: row.get(7)?,
                    pool_max_ready: row.get(8)?,
                    preferred_node_group: row.get(9)?,
                    anti_affinity: row.get::<_, i64>(10)? != 0,
                    reason_code: row.get(11)?,
                    effective_at: row.get(12)?,
                    ttl_seconds: row.get(13)?,
                    updated_at: row.get(14)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(rows)
    })
    .await
}

pub async fn list_actions(
    db: &DbPool,
    tenant_id: Option<&str>,
    status: Option<&str>,
    limit: usize,
) -> Result<Vec<OrchestratorAction>> {
    let tenant_id = tenant_id.map(ToString::to_string);
    let status = status.map(ToString::to_string);
    execute_async(db, move |conn| {
        let sql = match (tenant_id.is_some(), status.is_some()) {
            (true, true) => "SELECT action_id, tenant_id, workload_id, action_type, payload_json, ttl_seconds, rollback_action_json, parent_action_id, idempotency_key, decision_window_start, status, reason_code, reason_message, effective_at, outbound_requested_at, runtime_operation_id, runtime_operation_type, terminal_status, terminal_at, total_latency_ms, attempt_count, next_retry_at, created_at, updated_at FROM orchestrator_action WHERE tenant_id = ?1 AND status = ?2 ORDER BY created_at DESC LIMIT ?3",
            (true, false) => "SELECT action_id, tenant_id, workload_id, action_type, payload_json, ttl_seconds, rollback_action_json, parent_action_id, idempotency_key, decision_window_start, status, reason_code, reason_message, effective_at, outbound_requested_at, runtime_operation_id, runtime_operation_type, terminal_status, terminal_at, total_latency_ms, attempt_count, next_retry_at, created_at, updated_at FROM orchestrator_action WHERE tenant_id = ?1 ORDER BY created_at DESC LIMIT ?2",
            (false, true) => "SELECT action_id, tenant_id, workload_id, action_type, payload_json, ttl_seconds, rollback_action_json, parent_action_id, idempotency_key, decision_window_start, status, reason_code, reason_message, effective_at, outbound_requested_at, runtime_operation_id, runtime_operation_type, terminal_status, terminal_at, total_latency_ms, attempt_count, next_retry_at, created_at, updated_at FROM orchestrator_action WHERE status = ?1 ORDER BY created_at DESC LIMIT ?2",
            (false, false) => "SELECT action_id, tenant_id, workload_id, action_type, payload_json, ttl_seconds, rollback_action_json, parent_action_id, idempotency_key, decision_window_start, status, reason_code, reason_message, effective_at, outbound_requested_at, runtime_operation_id, runtime_operation_type, terminal_status, terminal_at, total_latency_ms, attempt_count, next_retry_at, created_at, updated_at FROM orchestrator_action ORDER BY created_at DESC LIMIT ?1",
        };
        let mut stmt = conn.prepare(sql)?;
        let rows = match (tenant_id, status) {
            (Some(t), Some(s)) => stmt
                .query_map(params![t, s, limit as i64], row_to_action)?
                .collect::<Result<Vec<_>, _>>()?,
            (Some(t), None) => stmt
                .query_map(params![t, limit as i64], row_to_action)?
                .collect::<Result<Vec<_>, _>>()?,
            (None, Some(s)) => stmt
                .query_map(params![s, limit as i64], row_to_action)?
                .collect::<Result<Vec<_>, _>>()?,
            (None, None) => stmt
                .query_map(params![limit as i64], row_to_action)?
                .collect::<Result<Vec<_>, _>>()?,
        };
        Ok(rows)
    })
    .await
}

pub async fn update_action_result(
    db: &DbPool,
    action_id: &str,
    status: &str,
    reason_code: Option<&str>,
    reason_message: Option<&str>,
) -> Result<()> {
    let now = now_unix_seconds();
    let action_id = action_id.to_string();
    let status = status.to_string();
    let reason_code = reason_code.map(ToString::to_string);
    let reason_message = reason_message.map(ToString::to_string);
    execute_async(db, move |conn| {
        let rows = conn.execute(
            "UPDATE orchestrator_action
             SET status = ?1, reason_code = ?2, reason_message = ?3, updated_at = ?4
             WHERE action_id = ?5",
            params![status, reason_code, reason_message, now, action_id],
        )?;
        if rows == 0 {
            anyhow::bail!("Action not found");
        }
        Ok(())
    })
    .await
}

fn row_to_action(row: &rusqlite::Row<'_>) -> rusqlite::Result<OrchestratorAction> {
    let payload: String = row.get(4)?;
    let rollback_raw: Option<String> = row.get(6)?;
    Ok(OrchestratorAction {
        action_id: row.get(0)?,
        tenant_id: row.get(1)?,
        workload_id: row.get(2)?,
        action_type: row.get(3)?,
        payload_json: serde_json::from_str(&payload).unwrap_or_else(|_| json!({})),
        ttl_seconds: row.get(5)?,
        rollback_action_json: rollback_raw.and_then(|v| serde_json::from_str::<Value>(&v).ok()),
        parent_action_id: row.get(7)?,
        idempotency_key: row.get(8)?,
        decision_window_start: row.get(9)?,
        status: row.get(10)?,
        reason_code: row.get(11)?,
        reason_message: row.get(12)?,
        effective_at: row.get(13)?,
        outbound_requested_at: row.get(14)?,
        runtime_operation_id: row.get(15)?,
        runtime_operation_type: row.get(16)?,
        terminal_status: row.get(17)?,
        terminal_at: row.get(18)?,
        total_latency_ms: row.get(19)?,
        attempt_count: row.get(20)?,
        next_retry_at: row.get(21)?,
        created_at: row.get(22)?,
        updated_at: row.get(23)?,
    })
}

fn load_policy(
    conn: &Connection,
    tenant_id: &str,
    workload_id: &str,
) -> Result<Option<WorkloadPolicyRow>> {
    let mut stmt = conn.prepare(
        "SELECT runtime_function_id, max_concurrency, hard_quota, soft_burst, absolute_limit, cooldown_seconds, hysteresis_pct, burst_cpu_cap, burst_mem_mb, burst_ttl_seconds, target_container_ids_json
         FROM orchestrator_workload_policy
         WHERE tenant_id = ?1 AND workload_id = ?2",
    )?;
    let row = stmt
        .query_row(params![tenant_id, workload_id], |row| {
            let targets: String = row.get(10)?;
            Ok(WorkloadPolicyRow {
                runtime_function_id: row.get(0)?,
                max_concurrency: row.get(1)?,
                hard_quota: row.get(2)?,
                soft_burst: row.get(3)?,
                absolute_limit: row.get(4)?,
                cooldown_seconds: row.get(5)?,
                hysteresis_pct: row.get(6)?,
                burst_cpu_cap: row.get(7)?,
                burst_mem_mb: row.get(8)?,
                burst_ttl_seconds: row.get(9)?,
                target_container_ids: serde_json::from_str(&targets).unwrap_or_default(),
            })
        })
        .optional()?;
    Ok(row)
}

fn load_slo(
    conn: &Connection,
    tenant_id: &str,
    workload_id: &str,
) -> Result<Option<WorkloadSloRow>> {
    let mut stmt = conn.prepare(
        "SELECT p95_latency_ms, max_cold_start_pct, max_reject_pct, max_cost_per_compute_unit
         FROM orchestrator_workload_slo
         WHERE tenant_id = ?1 AND workload_id = ?2",
    )?;
    let row = stmt
        .query_row(params![tenant_id, workload_id], |row| {
            Ok(WorkloadSloRow {
                p95_latency_ms: row.get(0)?,
                max_cold_start_pct: row.get(1)?,
                max_reject_pct: row.get(2)?,
                max_cost_per_compute_unit: row.get(3)?,
            })
        })
        .optional()?;
    Ok(row)
}

fn load_current_intent(
    conn: &Connection,
    tenant_id: &str,
    workload_id: &str,
) -> Result<Option<(u32, i64)>> {
    let mut stmt = conn.prepare(
        "SELECT target_concurrency, updated_at
         FROM orchestrator_intent
         WHERE tenant_id = ?1 AND workload_id = ?2",
    )?;
    let row = stmt
        .query_row(params![tenant_id, workload_id], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })
        .optional()?;
    Ok(row)
}

fn list_workload_observations(conn: &Connection) -> Result<Vec<WorkloadObservation>> {
    let mut stmt = conn.prepare(
        "SELECT tenant_id, workload_id, node_group, queue_depth, cpu_pressure, mem_pressure, io_pressure, cold_start_pct, invoke_p95_ms, reject_pct, active_compute_units, cost_per_compute_unit
         FROM orchestrator_workload_observation",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(WorkloadObservation {
                tenant_id: row.get(0)?,
                workload_id: row.get(1)?,
                node_group: row.get(2)?,
                queue_depth: row.get(3)?,
                cpu_pressure: row.get(4)?,
                mem_pressure: row.get(5)?,
                io_pressure: row.get(6)?,
                cold_start_pct: row.get(7)?,
                invoke_p95_ms: row.get(8)?,
                reject_pct: row.get(9)?,
                active_compute_units: row.get(10)?,
                cost_per_compute_unit: row.get(11)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn list_node_group_observations(conn: &Connection) -> Result<Vec<NodeGroupObservation>> {
    let mut stmt = conn.prepare(
        "SELECT node_group, cpu_pressure, mem_pressure, io_pressure, warm_ready, warm_hit_rate, capacity_units, used_units
         FROM orchestrator_node_group_observation",
    )?;
    let rows = stmt
        .query_map([], |row| {
            Ok(NodeGroupObservation {
                node_group: row.get(0)?,
                cpu_pressure: row.get(1)?,
                mem_pressure: row.get(2)?,
                io_pressure: row.get(3)?,
                warm_ready: row.get(4)?,
                warm_hit_rate: row.get(5)?,
                capacity_units: row.get(6)?,
                used_units: row.get(7)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn upsert_intent(conn: &Connection, intent: &OrchestratorIntent) -> Result<()> {
    conn.execute(
        "INSERT INTO orchestrator_intent
         (tenant_id, workload_id, target_concurrency, burst_cpu_cap, burst_mem_mb, burst_ttl_seconds, pool_min_ready, pool_target_ready, pool_max_ready, preferred_node_group, anti_affinity, reason_code, effective_at, ttl_seconds, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
         ON CONFLICT(tenant_id, workload_id) DO UPDATE SET
           target_concurrency=excluded.target_concurrency,
           burst_cpu_cap=excluded.burst_cpu_cap,
           burst_mem_mb=excluded.burst_mem_mb,
           burst_ttl_seconds=excluded.burst_ttl_seconds,
           pool_min_ready=excluded.pool_min_ready,
           pool_target_ready=excluded.pool_target_ready,
           pool_max_ready=excluded.pool_max_ready,
           preferred_node_group=excluded.preferred_node_group,
           anti_affinity=excluded.anti_affinity,
           reason_code=excluded.reason_code,
           effective_at=excluded.effective_at,
           ttl_seconds=excluded.ttl_seconds,
           updated_at=excluded.updated_at",
        params![
            intent.tenant_id,
            intent.workload_id,
            intent.target_concurrency,
            intent.burst_cpu_cap,
            intent.burst_mem_mb,
            intent.burst_ttl_seconds,
            intent.pool_min_ready,
            intent.pool_target_ready,
            intent.pool_max_ready,
            intent.preferred_node_group,
            if intent.anti_affinity { 1 } else { 0 },
            intent.reason_code,
            intent.effective_at,
            intent.ttl_seconds,
            intent.updated_at
        ],
    )?;
    Ok(())
}

fn enqueue_action(
    conn: &Connection,
    tenant_id: &str,
    workload_id: &str,
    action_type: &str,
    payload: Value,
    ttl_seconds: u32,
    decision_window_start: i64,
    rollback_action: Option<Value>,
    parent_action_id: Option<&str>,
) -> Result<()> {
    let now = now_unix_seconds();
    let idempotency_key = deterministic_idempotency_key(
        tenant_id,
        workload_id,
        action_type,
        &payload,
        decision_window_start,
    );
    conn.execute(
        "INSERT INTO orchestrator_action
         (action_id, tenant_id, workload_id, action_type, payload_json, ttl_seconds, rollback_action_json, parent_action_id, idempotency_key, decision_window_start, status, reason_code, reason_message, effective_at, outbound_requested_at, runtime_operation_id, runtime_operation_type, terminal_status, terminal_at, total_latency_ms, attempt_count, next_retry_at, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'pending', NULL, NULL, ?11, NULL, NULL, NULL, NULL, NULL, NULL, 0, ?11, ?11, ?11)
         ON CONFLICT(idempotency_key) DO NOTHING",
        params![
            Uuid::new_v4().to_string(),
            tenant_id,
            workload_id,
            action_type,
            canonical_json(&payload).to_string(),
            ttl_seconds,
            rollback_action.map(|v| canonical_json(&v).to_string()),
            parent_action_id,
            idempotency_key,
            decision_window_start,
            now
        ],
    )?;
    Ok(())
}

fn hottest_group_for(workload_node_group: &str, groups: &[NodeGroupObservation]) -> Option<String> {
    let mut candidate = None;
    let mut max_pressure = 0.0;
    for g in groups {
        let p = g.cpu_pressure.max(g.mem_pressure).max(g.io_pressure);
        if p > max_pressure {
            max_pressure = p;
            candidate = Some(g.node_group.clone());
        }
    }
    if max_pressure >= 0.85 && candidate.as_deref() == Some(workload_node_group) {
        candidate
    } else {
        None
    }
}

fn coolest_group(groups: &[NodeGroupObservation], fallback: &str) -> String {
    groups
        .iter()
        .min_by(|a, b| {
            let ap = a.cpu_pressure.max(a.mem_pressure).max(a.io_pressure);
            let bp = b.cpu_pressure.max(b.mem_pressure).max(b.io_pressure);
            ap.partial_cmp(&bp).unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|g| g.node_group.clone())
        .unwrap_or_else(|| fallback.to_string())
}

fn run_fast_loop_tx(conn: &Connection) -> Result<()> {
    let observations = list_workload_observations(conn)?;
    let node_groups = list_node_group_observations(conn)?;
    let now = now_unix_seconds();
    let window_start = decision_window_start(now, FAST_LOOP_SECONDS as i64);

    for obs in observations {
        let Some(policy) = load_policy(conn, &obs.tenant_id, &obs.workload_id)? else {
            continue;
        };
        let slo = load_slo(conn, &obs.tenant_id, &obs.workload_id)?;
        let pressure = obs.cpu_pressure.max(obs.mem_pressure).max(obs.io_pressure);

        let queue_boost = if obs.queue_depth > 0 {
            1.0 + (obs.queue_depth as f64 / 100.0).min(1.0)
        } else {
            1.0
        };
        let mut desired = (obs.active_compute_units as f64 * queue_boost).ceil() as i64;
        desired = desired.max(policy.max_concurrency as i64);
        desired = desired.min((policy.hard_quota + policy.soft_burst) as i64);
        desired = desired.min(policy.absolute_limit as i64);

        let mut reason_code = "STEADY_STATE";
        let mut burst_cpu = policy.burst_cpu_cap;
        let mut burst_mem = policy.burst_mem_mb as i64;
        if let Some(s) = &slo {
            let violation = obs.invoke_p95_ms > s.p95_latency_ms
                || obs.cold_start_pct > s.max_cold_start_pct
                || obs.reject_pct > s.max_reject_pct
                || obs.cost_per_compute_unit > s.max_cost_per_compute_unit;
            if violation {
                desired = (desired + policy.soft_burst as i64).min(policy.absolute_limit as i64);
                burst_cpu *= 1.2;
                burst_mem = (burst_mem as f64 * 1.15) as i64;
                reason_code = "SLO_GUARD";
            }
        }
        if pressure > 0.85 {
            reason_code = "HOT_NODE_GROUP";
        } else if obs.queue_depth > 0 {
            reason_code = "QUEUE_PRESSURE";
        }

        let min_ready = (desired as f64 * 0.10).ceil() as i64;
        let target_ready = (desired as f64 * 0.20).ceil() as i64;
        let max_ready = (desired as f64 * 0.30).ceil() as i64;
        let anti_affinity = hottest_group_for(&obs.node_group, &node_groups).is_some();
        let preferred_group = if anti_affinity {
            coolest_group(&node_groups, &obs.node_group)
        } else {
            obs.node_group.clone()
        };

        let intent = OrchestratorIntent {
            tenant_id: obs.tenant_id.clone(),
            workload_id: obs.workload_id.clone(),
            target_concurrency: clamp_u32(desired, 1, policy.absolute_limit),
            burst_cpu_cap: burst_cpu,
            burst_mem_mb: clamp_u32(burst_mem, 64, policy.burst_mem_mb.saturating_mul(2)),
            burst_ttl_seconds: policy.burst_ttl_seconds,
            pool_min_ready: clamp_u32(min_ready, 0, u32::MAX),
            pool_target_ready: clamp_u32(target_ready, 0, u32::MAX),
            pool_max_ready: clamp_u32(max_ready, 0, u32::MAX),
            preferred_node_group: preferred_group.clone(),
            anti_affinity,
            reason_code: reason_code.to_string(),
            effective_at: now,
            ttl_seconds: policy.burst_ttl_seconds,
            updated_at: now,
        };
        if let Some((current_target, current_updated_at)) =
            load_current_intent(conn, &obs.tenant_id, &obs.workload_id)?
        {
            let cooldown_open = now - current_updated_at < policy.cooldown_seconds as i64;
            let threshold = (current_target as f64 * (policy.hysteresis_pct / 100.0)).ceil() as u32;
            let delta = intent.target_concurrency.abs_diff(current_target);
            if cooldown_open || delta <= threshold.max(1) {
                continue;
            }
        }
        upsert_intent(conn, &intent)?;

        enqueue_action(
            conn,
            &obs.tenant_id,
            &obs.workload_id,
            "SetPoolTarget",
            json!({
                "function_id": policy.runtime_function_id,
                "min_instances": intent.pool_min_ready,
                "max_instances": intent.pool_max_ready
            }),
            policy.burst_ttl_seconds,
            window_start,
            None,
            None,
        )?;

        for container_id in &policy.target_container_ids {
            enqueue_action(
                conn,
                &obs.tenant_id,
                &obs.workload_id,
                "SetBurstPolicy",
                json!({
                    "container_id": container_id,
                    "memory_limit_mb": intent.burst_mem_mb,
                    "cpu_limit_percent": (intent.burst_cpu_cap * 100.0).round() as u32
                }),
                policy.burst_ttl_seconds,
                window_start,
                None,
                None,
            )?;
        }

        enqueue_action(
            conn,
            &obs.tenant_id,
            &obs.workload_id,
            "SetPlacementPreference",
            json!({
                "tenant_id": obs.tenant_id,
                "workload_id": obs.workload_id,
                "node_group": preferred_group,
                "anti_affinity": anti_affinity
            }),
            policy.burst_ttl_seconds,
            window_start,
            None,
            None,
        )?;
    }
    Ok(())
}

fn run_slow_loop_tx(conn: &Connection) -> Result<()> {
    let groups = list_node_group_observations(conn)?;
    let now = now_unix_seconds();
    let window_start = decision_window_start(now, SLOW_LOOP_SECONDS as i64);
    for group in groups {
        let utilization = if group.capacity_units == 0 {
            0.0
        } else {
            group.used_units as f64 / group.capacity_units as f64
        };
        if utilization > 0.80 {
            enqueue_action(
                conn,
                PLATFORM_TENANT_ID,
                &group.node_group,
                "ScaleNodeGroupUp",
                json!({
                    "node_group": group.node_group,
                    "delta_units": 1
                }),
                SLOW_LOOP_SECONDS as u32,
                window_start,
                None,
                None,
            )?;
        } else if utilization < 0.35 && group.warm_ready > 0 {
            enqueue_action(
                conn,
                PLATFORM_TENANT_ID,
                &group.node_group,
                "ScaleNodeGroupDown",
                json!({
                    "node_group": group.node_group,
                    "delta_units": 1
                }),
                SLOW_LOOP_SECONDS as u32,
                window_start,
                None,
                None,
            )?;
        }
    }
    Ok(())
}

pub async fn run_fast_loop(db: &DbPool) -> Result<()> {
    let db = db.clone();
    execute_async(&db, move |conn| {
        let tx = conn.unchecked_transaction()?;
        run_fast_loop_tx(&tx)?;
        tx.commit()?;
        Ok(())
    })
    .await
}

pub async fn run_slow_loop(db: &DbPool) -> Result<()> {
    let db = db.clone();
    execute_async(&db, move |conn| {
        let tx = conn.unchecked_transaction()?;
        run_slow_loop_tx(&tx)?;
        tx.commit()?;
        Ok(())
    })
    .await
}

fn load_dispatch_candidates(conn: &Connection, now: i64) -> Result<Vec<DispatchAction>> {
    let mut stmt = conn.prepare(
        "SELECT action_id, tenant_id, workload_id, action_type, payload_json, ttl_seconds, rollback_action_json, idempotency_key, status, effective_at, runtime_operation_id, attempt_count, created_at
         FROM orchestrator_action
         WHERE status IN ('pending','accepted','queued','running') AND next_retry_at <= ?1
         ORDER BY created_at ASC
         LIMIT ?2",
    )?;
    let rows = stmt
        .query_map(params![now, MAX_DISPATCH_BATCH as i64], |row| {
            let payload_raw: String = row.get(4)?;
            let rollback_raw: Option<String> = row.get(6)?;
            Ok(DispatchAction {
                action_id: row.get(0)?,
                tenant_id: row.get(1)?,
                workload_id: row.get(2)?,
                action_type: row.get(3)?,
                payload_json: serde_json::from_str(&payload_raw).unwrap_or_else(|_| json!({})),
                ttl_seconds: row.get(5)?,
                rollback_action_json: rollback_raw
                    .and_then(|v| serde_json::from_str::<Value>(&v).ok()),
                idempotency_key: row.get(7)?,
                status: row.get(8)?,
                effective_at: row.get(9)?,
                runtime_operation_id: row.get(10)?,
                attempt_count: row.get(11)?,
                created_at: row.get(12)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(rows)
}

fn set_action_status(
    conn: &Connection,
    action_id: &str,
    status: &str,
    reason_code: Option<&str>,
    reason_message: Option<&str>,
) -> Result<()> {
    let now = now_unix_seconds();
    conn.execute(
        "UPDATE orchestrator_action
         SET status = ?1, reason_code = ?2, reason_message = ?3, updated_at = ?4
         WHERE action_id = ?5",
        params![status, reason_code, reason_message, now, action_id],
    )?;
    Ok(())
}

fn schedule_retry(
    conn: &Connection,
    action_id: &str,
    attempt_count: u32,
    reason_code: &str,
    reason_message: &str,
) -> Result<()> {
    let now = now_unix_seconds();
    let next = now + (2_i64.pow(attempt_count.min(6)));
    conn.execute(
        "UPDATE orchestrator_action
         SET attempt_count = ?1, next_retry_at = ?2, reason_code = ?3, reason_message = ?4, updated_at = ?5
         WHERE action_id = ?6",
        params![attempt_count, next, reason_code, reason_message, now, action_id],
    )?;
    Ok(())
}

fn mark_terminal(
    conn: &Connection,
    action: &DispatchAction,
    status: &str,
    reason_code: Option<&str>,
    reason_message: Option<&str>,
) -> Result<()> {
    let now = now_unix_seconds();
    let latency = (now - action.created_at) * 1000;
    conn.execute(
        "UPDATE orchestrator_action
         SET status = ?1, terminal_status = ?1, reason_code = ?2, reason_message = ?3, terminal_at = ?4, total_latency_ms = ?5, updated_at = ?4
         WHERE action_id = ?6",
        params![status, reason_code, reason_message, now, latency, action.action_id],
    )?;
    Ok(())
}

fn enqueue_rollback(conn: &Connection, action: &DispatchAction) -> Result<()> {
    let Some(rollback_payload) = &action.rollback_action_json else {
        return Ok(());
    };
    let now = now_unix_seconds();
    let window_start = decision_window_start(now, FAST_LOOP_SECONDS as i64);
    enqueue_action(
        conn,
        &action.tenant_id,
        &action.workload_id,
        "RollbackAction",
        rollback_payload.clone(),
        action.ttl_seconds,
        window_start,
        None,
        Some(&action.action_id),
    )?;
    set_action_status(
        conn,
        &action.action_id,
        "failed",
        Some(RC_ORCH_ROLLBACK_TRIGGERED),
        Some("Rollback action enqueued"),
    )?;
    Ok(())
}

fn workload_ownership_valid(conn: &Connection, tenant_id: &str, workload_id: &str) -> Result<bool> {
    let exists: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM orchestrator_workload_policy WHERE tenant_id = ?1 AND workload_id = ?2",
            params![tenant_id, workload_id],
            |row| row.get(0),
        )
        .optional()?;
    Ok(exists.is_some())
}

async fn dispatch_control_operation(
    http: &Client,
    cfg: &ExecutionConfig,
    action: &DispatchAction,
    path: String,
    payload: Value,
) -> Result<RuntimeOperation> {
    let Some(base) = &cfg.control_base_url else {
        anyhow::bail!("{}", RC_ORCH_DEPENDENCY_UNAVAILABLE);
    };
    let url = format!("{base}{path}");
    let mut req = http
        .post(url)
        .header("Idempotency-Key", action.idempotency_key.clone())
        .header("X-Tenant-Id", action.tenant_id.clone())
        .header("X-Orch-Action-Id", action.action_id.clone())
        .json(&payload);
    if let Some(api_key) = &cfg.control_api_key {
        req = req.header("X-Api-Key", api_key);
    }
    let resp = req.send().await.context("runtime request failed")?;
    parse_runtime_response(resp.status(), resp.text().await.unwrap_or_default())
}

async fn poll_control_operation(
    http: &Client,
    cfg: &ExecutionConfig,
    action: &DispatchAction,
    operation_id: &str,
) -> Result<RuntimeOperation> {
    let Some(base) = &cfg.control_base_url else {
        anyhow::bail!("{}", RC_ORCH_DEPENDENCY_UNAVAILABLE);
    };
    let url = format!("{base}/api/elasticity/control/operations/{operation_id}");
    let mut req = http
        .get(url)
        .header("X-Tenant-Id", action.tenant_id.clone())
        .header("X-Orch-Action-Id", action.action_id.clone());
    if let Some(api_key) = &cfg.control_api_key {
        req = req.header("X-Api-Key", api_key);
    }
    let resp = req.send().await.context("runtime poll failed")?;
    parse_runtime_response(resp.status(), resp.text().await.unwrap_or_default())
}

fn parse_runtime_response(status: StatusCode, body: String) -> Result<RuntimeOperation> {
    let value: Value = serde_json::from_str(&body).unwrap_or_else(|_| json!({}));
    let reason_code = value
        .get("reason_code")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let reason_message = value
        .get("reason_message")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let retryable = value.get("retryable").and_then(Value::as_bool);

    if status.is_server_error() {
        anyhow::bail!("{}|retryable=true|{}", RC_ORCH_DEPENDENCY_UNAVAILABLE, body);
    }

    let op = RuntimeOperation {
        operation_id: value
            .get("operation_id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        operation_type: value
            .get("operation_type")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        status: value
            .get("status")
            .and_then(Value::as_str)
            .unwrap_or("accepted")
            .to_string(),
        reason_code,
        reason_message,
    };
    if status.is_client_error() {
        if op.reason_code.as_deref() == Some(RC_IDEMPOTENCY_REPLAY) && !op.operation_id.is_empty() {
            return Ok(op);
        }
        let code = normalize_reason_code(op.reason_code.as_deref());
        anyhow::bail!(
            "{}|retryable={}|{}",
            code,
            retryable
                .map(|v| if v { "true" } else { "false" })
                .unwrap_or("unknown"),
            op.reason_message
                .clone()
                .unwrap_or_else(|| "client_error".to_string())
        );
    }
    Ok(op)
}

async fn process_action(
    db: &DbPool,
    http: &Client,
    cfg: &ExecutionConfig,
    action: DispatchAction,
) -> Result<()> {
    let now = now_unix_seconds();
    let ttl_expired = now > action.effective_at + action.ttl_seconds as i64;
    if ttl_expired {
        let db = db.clone();
        execute_async(&db, move |conn| {
            mark_terminal(
                conn,
                &action,
                "failed",
                Some(RC_ORCH_TTL_EXPIRED),
                Some("TTL expired before completion"),
            )?;
            enqueue_rollback(conn, &action)?;
            Ok(())
        })
        .await?;
        return Ok(());
    }

    if action.status != "pending" && action.runtime_operation_id.is_some() {
        let Some(op_id) = action.runtime_operation_id.clone() else {
            return Ok(());
        };
        match poll_control_operation(http, cfg, &action, &op_id).await {
            Ok(op) => {
                let db = db.clone();
                execute_async(&db, move |conn| {
                    let now = now_unix_seconds();
                    if is_runtime_non_terminal(&op.status) {
                        conn.execute(
                            "UPDATE orchestrator_action
                             SET status = ?1, reason_code = ?2, reason_message = ?3, updated_at = ?4
                             WHERE action_id = ?5",
                            params![
                                op.status,
                                op.reason_code,
                                op.reason_message,
                                now,
                                action.action_id
                            ],
                        )?;
                    } else if is_runtime_terminal_success(&op.status) {
                        mark_terminal(
                            conn,
                            &action,
                            RUNTIME_SUCCESS,
                            op.reason_code.as_deref(),
                            op.reason_message.as_deref(),
                        )?;
                    } else if is_runtime_terminal_failure(&op.status) {
                        mark_terminal(
                            conn,
                            &action,
                            &op.status,
                            op.reason_code.as_deref(),
                            op.reason_message.as_deref(),
                        )?;
                    }
                    Ok(())
                })
                .await?;
            }
            Err(err) => {
                let msg = err.to_string();
                let attempt = action.attempt_count + 1;
                let db = db.clone();
                execute_async(&db, move |conn| {
                    schedule_retry(
                        conn,
                        &action.action_id,
                        attempt,
                        RC_ORCH_DEPENDENCY_UNAVAILABLE,
                        &msg,
                    )?;
                    Ok(())
                })
                .await?;
            }
        }
        return Ok(());
    }

    let db = db.clone();
    let action_for_validate = action.clone();
    let ownership_ok = execute_async(&db, move |conn| {
        let valid = workload_ownership_valid(
            conn,
            &action_for_validate.tenant_id,
            &action_for_validate.workload_id,
        )? || action_for_validate.tenant_id == PLATFORM_TENANT_ID;
        if !valid {
            mark_terminal(
                conn,
                &action_for_validate,
                "failed",
                Some(RC_ORCH_DEPENDENCY_UNAVAILABLE),
                Some("Cross-tenant or unknown workload ownership"),
            )?;
        }
        Ok(valid)
    })
    .await?;
    if !ownership_ok {
        return Ok(());
    }

    let dispatch_result = match action.action_type.as_str() {
        "SetPoolTarget" => dispatch_control_operation(
            http,
            cfg,
            &action,
            format!(
                "/api/elasticity/control/functions/{}/pool-target",
                action
                    .payload_json
                    .get("function_id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .ok_or_else(|| anyhow::anyhow!(RC_INVALID_ARGUMENT))?
            ),
            json!({
                "min_instances": action.payload_json.get("min_instances").and_then(Value::as_u64).ok_or_else(|| anyhow::anyhow!(RC_INVALID_ARGUMENT))?,
                "max_instances": action.payload_json.get("max_instances").and_then(Value::as_u64).ok_or_else(|| anyhow::anyhow!(RC_INVALID_ARGUMENT))?
            }),
        )
        .await
        .map(Some),
        "SetBurstPolicy" => {
            let container_id = action
                .payload_json
                .get("container_id")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow::anyhow!(RC_INVALID_ARGUMENT))?;
            let memory_limit_mb = action
                .payload_json
                .get("memory_limit_mb")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow::anyhow!(RC_INVALID_ARGUMENT))?;
            let cpu_limit_percent = action
                .payload_json
                .get("cpu_limit_percent")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow::anyhow!(RC_INVALID_ARGUMENT))?;
            dispatch_control_operation(
                http,
                cfg,
                &action,
                format!("/api/elasticity/control/containers/{container_id}/resize"),
                json!({
                    "memory_limit_mb": memory_limit_mb,
                    "cpu_limit_percent": cpu_limit_percent
                }),
            )
            .await
            .map(Some)
        }
        "SetPlacementPreference" => {
            if cfg.local_scheduler {
                let workload_id = action.payload_json.get("workload_id").and_then(Value::as_str).unwrap_or("unknown");
                if let Some(best_node) = super::scheduler::local_schedule_workload(&action.tenant_id, workload_id) {
                    tracing::info!("Local scheduler picked node {} for workload {}", best_node, workload_id);
                    Ok(Some(RuntimeOperation {
                        operation_id: format!("local-placement-{}", uuid::Uuid::new_v4()),
                        operation_type: Some("SetPlacementPreference".to_string()),
                        status: "succeeded".to_string(),
                        reason_code: None,
                        reason_message: Some(format!("Assigned to node {}", best_node)),
                    }))
                } else {
                    tracing::warn!("Local scheduler could not find a suitable node");
                    Err(anyhow::anyhow!(RC_RESOURCE_PRESSURE))
                }
            } else {
                dispatch_control_operation(
                    http,
                    cfg,
                    &action,
                    format!(
                        "/api/elasticity/control/workloads/{}/placement-preference",
                        action
                            .payload_json
                            .get("workload_id")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| anyhow::anyhow!(RC_INVALID_ARGUMENT))?
                    ),
                    json!({
                        "node_group": action
                            .payload_json
                            .get("node_group")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .filter(|v| !v.is_empty())
                            .ok_or_else(|| anyhow::anyhow!(RC_INVALID_ARGUMENT))?,
                        "anti_affinity": action
                            .payload_json
                            .get("anti_affinity")
                            .and_then(Value::as_bool)
                            .unwrap_or(false)
                    }),
                )
                .await
                .map(Some)
            }
        },
        "ScaleNodeGroupUp" | "ScaleNodeGroupDown" => {
            dispatch_control_operation(
                http,
                cfg,
                &action,
                format!(
                    "/api/elasticity/control/node-groups/{}/scale",
                    action
                        .payload_json
                        .get("node_group")
                        .and_then(Value::as_str)
                        .map(str::trim)
                        .filter(|v| !v.is_empty())
                        .ok_or_else(|| anyhow::anyhow!(RC_INVALID_ARGUMENT))?
                ),
                json!({
                    "delta_units": action
                        .payload_json
                        .get("delta_units")
                        .and_then(Value::as_i64)
                        .ok_or_else(|| anyhow::anyhow!(RC_INVALID_ARGUMENT))?
                }),
            )
            .await
            .map(Some)
        }
        "RollbackAction" => dispatch_control_operation(
            http,
            cfg,
            &action,
            format!(
                "/api/elasticity/control/actions/{}/rollback",
                action
                    .payload_json
                    .get("target_action_id")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .unwrap_or(action.action_id.as_str())
            ),
            json!({
                "target_action_id": action.payload_json.get("target_action_id").and_then(Value::as_str),
                "target_operation_id": action.payload_json.get("target_operation_id").and_then(Value::as_str),
                "reason_code": action.payload_json.get("reason_code").and_then(Value::as_str),
                "reason_message": action.payload_json.get("reason_message").and_then(Value::as_str),
                "payload": action.payload_json.get("payload").cloned()
            }),
        )
        .await
        .map(Some),
        _ => Err(anyhow::anyhow!(RC_ORCH_ACTION_UNSUPPORTED)),
    };

    match dispatch_result {
        Ok(Some(op)) => {
            let db = db.clone();
            execute_async(&db, move |conn| {
                let now = now_unix_seconds();
                if is_runtime_terminal_success(&op.status) || is_runtime_terminal_failure(&op.status) {
                    let latency = (now - action.created_at) * 1000;
                    conn.execute(
                        "UPDATE orchestrator_action
                         SET outbound_requested_at = ?1, runtime_operation_id = ?2, runtime_operation_type = ?3, status = ?4, terminal_status = ?4, reason_code = ?5, reason_message = ?6, terminal_at = ?1, total_latency_ms = ?7, attempt_count = attempt_count + 1, next_retry_at = ?1, updated_at = ?1
                         WHERE action_id = ?8",
                        params![
                            now,
                            op.operation_id,
                            op.operation_type,
                            op.status,
                            op.reason_code,
                            op.reason_message,
                            latency,
                            action.action_id
                        ],
                    )?;
                } else {
                    conn.execute(
                        "UPDATE orchestrator_action
                         SET outbound_requested_at = ?1, runtime_operation_id = ?2, runtime_operation_type = ?3, status = ?4, reason_code = ?5, reason_message = ?6, attempt_count = attempt_count + 1, next_retry_at = ?1, updated_at = ?1
                         WHERE action_id = ?7",
                        params![
                            now,
                            op.operation_id,
                            op.operation_type,
                            op.status,
                            op.reason_code,
                            op.reason_message,
                            action.action_id
                        ],
                    )?;
                }
                Ok(())
            })
            .await?;
        }
        Ok(None) => {
            let db = db.clone();
            execute_async(&db, move |conn| {
                mark_terminal(conn, &action, RUNTIME_SUCCESS, None, None)?;
                Ok(())
            })
            .await?;
        }
        Err(err) => {
            let message = err.to_string();
            let (reason_code_owned, retryable_hint, detail) = parse_dispatch_error(&message);

            let db = db.clone();
            execute_async(&db, move |conn| {
                let reason_code = reason_code_owned.as_str();
                if action.action_type == "SetPoolTarget" {
                    let non_retry_terminal = reason_code == RC_INVALID_ARGUMENT
                        || reason_code == RC_TARGET_NOT_FOUND
                        || (reason_code == RC_OPERATION_FAILED && retryable_hint != Some(true))
                        || (reason_code == RC_RESOURCE_PRESSURE && retryable_hint != Some(true));
                    if non_retry_terminal {
                        mark_terminal(conn, &action, "failed", Some(reason_code), Some(&detail))?;
                    } else if reason_code == RC_RESOURCE_PRESSURE && retryable_hint == Some(true) {
                        let attempt = action.attempt_count + 1;
                        let now = now_unix_seconds();
                        conn.execute(
                            "UPDATE orchestrator_action
                             SET attempt_count = ?1, next_retry_at = ?2, reason_code = ?3, reason_message = ?4, updated_at = ?5
                             WHERE action_id = ?6",
                            params![
                                attempt,
                                now + 15,
                                reason_code,
                                detail,
                                now,
                                action.action_id
                            ],
                        )?;
                    } else if retryable_hint == Some(true) {
                        let attempt = action.attempt_count + 1;
                        schedule_retry(conn, &action.action_id, attempt, reason_code, &detail)?;
                    } else {
                        mark_terminal(conn, &action, "failed", Some(reason_code), Some(&detail))?;
                    }
                } else if reason_code == RC_INVALID_ARGUMENT || reason_code == RC_ORCH_ACTION_UNSUPPORTED {
                    mark_terminal(conn, &action, "failed", Some(reason_code), Some(&detail))?;
                } else if reason_code == RC_RESOURCE_PRESSURE {
                    let attempt = action.attempt_count + 1;
                    let now = now_unix_seconds();
                    conn.execute(
                        "UPDATE orchestrator_action
                         SET attempt_count = ?1, next_retry_at = ?2, reason_code = ?3, reason_message = ?4, updated_at = ?5
                         WHERE action_id = ?6",
                        params![attempt, now + 15, reason_code, detail, now, action.action_id],
                    )?;
                } else {
                    let attempt = action.attempt_count + 1;
                    schedule_retry(conn, &action.action_id, attempt, reason_code, &detail)?;
                }
                Ok(())
            })
            .await?;
        }
    }
    Ok(())
}

pub async fn run_dispatch_cycle(db: &DbPool, cfg: &ExecutionConfig) -> Result<()> {
    let now = now_unix_seconds();
    let candidates = {
        let db = db.clone();
        execute_async(&db, move |conn| load_dispatch_candidates(conn, now)).await?
    };
    if candidates.is_empty() {
        return Ok(());
    }
    let http = Client::new();
    for action in candidates {
        process_action(db, &http, cfg, action).await?;
    }
    Ok(())
}

pub async fn start_loops(db: DbPool, config: ExecutionConfig) -> Result<()> {
    let fast_db = db.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(FAST_LOOP_SECONDS));
        loop {
            ticker.tick().await;
            if let Err(e) = run_fast_loop(&fast_db).await {
                tracing::error!("Fast loop failed: {}", e);
            }
        }
    });

    let slow_db = db.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(SLOW_LOOP_SECONDS));
        loop {
            ticker.tick().await;
            if let Err(e) = run_slow_loop(&slow_db).await {
                tracing::error!("Slow loop failed: {}", e);
            }
        }
    });

    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(std::time::Duration::from_secs(DISPATCH_LOOP_SECONDS));
        loop {
            ticker.tick().await;
            if let Err(e) = run_dispatch_cycle(&db, &config).await {
                tracing::error!("Dispatch loop failed: {}", e);
            }
        }
    });

    info!(
        "Orchestrator loops started (fast={}s, slow={}s, dispatch={}s)",
        FAST_LOOP_SECONDS, SLOW_LOOP_SECONDS, DISPATCH_LOOP_SECONDS
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;

    fn setup_conn() -> Connection {
        let conn = Connection::open_in_memory().expect("in-memory sqlite");
        conn.execute_batch(include_str!("../../migrations/003_orchestrator.sql"))
            .expect("migrations");
        conn
    }

    #[test]
    fn idempotency_key_is_deterministic() {
        let payload = json!({"b":1,"a":2});
        let k1 = deterministic_idempotency_key("t1", "w1", "SetPoolTarget", &payload, 100);
        let k2 =
            deterministic_idempotency_key("t1", "w1", "SetPoolTarget", &json!({"a":2,"b":1}), 100);
        assert_eq!(k1, k2);
    }

    #[test]
    fn status_mapping_helpers() {
        assert!(is_runtime_non_terminal("accepted"));
        assert!(is_runtime_terminal_success("succeeded"));
        assert!(is_runtime_terminal_failure("timed_out"));
    }

    #[test]
    fn reason_normalization_keeps_contract_codes() {
        let known = normalize_reason_code(Some(RC_IDEMPOTENCY_REPLAY));
        assert_eq!(known, RC_IDEMPOTENCY_REPLAY);
        let unknown = normalize_reason_code(None);
        assert_eq!(unknown, RC_OPERATION_FAILED);
    }

    #[test]
    fn parse_dispatch_error_reads_retryable_hint() {
        let (code, retryable, detail) =
            parse_dispatch_error("ELASTICITY_RESOURCE_PRESSURE|retryable=true|busy");
        assert_eq!(code, RC_RESOURCE_PRESSURE);
        assert_eq!(retryable, Some(true));
        assert_eq!(detail, "busy");
    }

    #[test]
    fn parse_dispatch_error_keeps_plain_reason_code() {
        let (code, retryable, detail) = parse_dispatch_error("ELASTICITY_INVALID_ARGUMENT");
        assert_eq!(code, RC_INVALID_ARGUMENT);
        assert_eq!(retryable, None);
        assert_eq!(detail, "ELASTICITY_INVALID_ARGUMENT");
    }

    #[test]
    fn duplicate_dispatch_key_collapses_to_single_action() {
        let conn = setup_conn();
        enqueue_action(
            &conn,
            "tenant-a",
            "workload-a",
            "SetPoolTarget",
            json!({"min_instances": 1, "max_instances": 3}),
            30,
            100,
            None,
            None,
        )
        .expect("first enqueue");
        enqueue_action(
            &conn,
            "tenant-a",
            "workload-a",
            "SetPoolTarget",
            json!({"max_instances": 3, "min_instances": 1}),
            30,
            100,
            None,
            None,
        )
        .expect("second enqueue");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM orchestrator_action", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(count, 1);
    }

    #[test]
    fn replay_reason_code_is_treated_as_success_path() {
        let op = parse_runtime_response(
            StatusCode::CONFLICT,
            json!({
                "operation_id": "op_123",
                "operation_type": "elasticity.resize_container",
                "status": "accepted",
                "reason_code": RC_IDEMPOTENCY_REPLAY,
                "reason_message": "replayed"
            })
            .to_string(),
        )
        .expect("replay should not fail");
        assert_eq!(op.operation_id, "op_123");
        assert_eq!(op.reason_code.as_deref(), Some(RC_IDEMPOTENCY_REPLAY));
    }
}
