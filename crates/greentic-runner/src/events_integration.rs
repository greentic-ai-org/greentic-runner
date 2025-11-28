use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use greentic_interfaces_host::events_broker;
use greentic_interfaces_host::events_broker::greentic::interfaces_types::types::Impersonation as WitImpersonation;
use greentic_interfaces_host::events_broker::greentic::interfaces_types::types::TenantCtx as WitTenantCtx;
use greentic_types::{EventEnvelope, EventId, GreenticError, TenantCtx};
use serde_json::Value;
use wasmtime::{Store, component::Linker, component::ResourceTable};

type WitEventEnvelope = events_broker::greentic::events::event_types::EventEnvelope;

/// Helper wrapper for invoking the broker world on a component.
///
/// Note: this currently assumes the component exports `greentic:events/broker@1.0.0`
/// without additional imports. Host imports can be layered onto the linker by the
/// caller before invoking `publish`.
pub struct EventsBrokerInvoker {
    engine: wasmtime::Engine,
    component: wasmtime::component::Component,
}

impl EventsBrokerInvoker {
    /// Create a new invoker bound to a compiled broker component.
    pub fn new(engine: wasmtime::Engine, component: wasmtime::component::Component) -> Self {
        Self { engine, component }
    }

    /// Publish a single event via the component.
    pub fn publish(&self, tenant: &TenantCtx, event: &EventEnvelope) -> Result<()> {
        let _ = tenant;
        let mut store = Store::new(
            &self.engine,
            // Empty store data; callers can wrap this type to thread host state if needed.
            ResourceTable::new(),
        );
        let linker = Linker::new(&self.engine);
        let bindings = events_broker::Broker::instantiate(&mut store, &self.component, &linker)
            .with_context(|| "failed to instantiate greentic:events/broker@1.0.0 component")?;
        let exports = bindings.greentic_events_broker_api();
        let wit_event = to_wit_event(event)?;
        match exports.call_publish(&mut store, &wit_event)? {
            Ok(_ack) => Ok(()),
            Err(err) => bail!("broker publish failed: {} - {}", err.code, err.message),
        }
    }
}

fn tenant_to_wit(tenant: &TenantCtx) -> Result<WitTenantCtx> {
    let attributes = tenant
        .attributes
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let impersonation = tenant.impersonation.as_ref().map(|imp| WitImpersonation {
        actor_id: imp.actor_id.to_string(),
        reason: imp.reason.clone(),
    });
    Ok(WitTenantCtx {
        env: tenant.env.to_string(),
        tenant: tenant.tenant.to_string(),
        tenant_id: tenant.tenant_id.to_string(),
        team: tenant.team.as_ref().map(|v| v.to_string()),
        team_id: tenant.team_id.as_ref().map(|v| v.to_string()),
        user: tenant.user.as_ref().map(|v| v.to_string()),
        user_id: tenant.user_id.as_ref().map(|v| v.to_string()),
        trace_id: tenant.trace_id.clone(),
        correlation_id: tenant.correlation_id.clone(),
        attributes,
        session_id: tenant.session_id.clone(),
        flow_id: tenant.flow_id.clone(),
        node_id: tenant.node_id.clone(),
        provider_id: tenant.provider_id.clone(),
        deadline_ms: tenant.deadline.as_ref().map(|d| d.unix_millis() as i64),
        attempt: tenant.attempt,
        idempotency_key: tenant.idempotency_key.clone(),
        impersonation,
    })
}

#[allow(dead_code)]
fn tenant_from_wit(tenant: WitTenantCtx) -> Result<TenantCtx> {
    let env = tenant
        .env
        .parse()
        .map_err(|err: GreenticError| anyhow!(err))?;
    let tenant_id = tenant
        .tenant_id
        .parse()
        .map_err(|err: GreenticError| anyhow!(err))?;
    let mut ctx = TenantCtx::new(env, tenant_id);
    ctx.tenant = tenant
        .tenant
        .parse()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.team = tenant
        .team
        .map(|v| v.parse())
        .transpose()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.team_id = tenant
        .team_id
        .map(|v| v.parse())
        .transpose()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.user = tenant
        .user
        .map(|v| v.parse())
        .transpose()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.user_id = tenant
        .user_id
        .map(|v| v.parse())
        .transpose()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.trace_id = tenant.trace_id;
    ctx.correlation_id = tenant.correlation_id;
    ctx.attributes = tenant.attributes.into_iter().collect();
    ctx.session_id = tenant.session_id;
    ctx.flow_id = tenant.flow_id;
    ctx.node_id = tenant.node_id;
    ctx.provider_id = tenant.provider_id;
    ctx.deadline = tenant
        .deadline_ms
        .map(|ms| greentic_types::InvocationDeadline::from_unix_millis(ms as i128));
    ctx.attempt = tenant.attempt;
    ctx.idempotency_key = tenant.idempotency_key;
    ctx.impersonation = tenant
        .impersonation
        .map(|imp| {
            let actor = imp
                .actor_id
                .parse()
                .map_err(|err: GreenticError| anyhow!(err))?;
            Ok::<greentic_types::Impersonation, anyhow::Error>(greentic_types::Impersonation {
                actor_id: actor,
                reason: imp.reason,
            })
        })
        .transpose()?;
    Ok(ctx)
}

fn to_wit_event(event: &EventEnvelope) -> Result<WitEventEnvelope> {
    let metadata = event
        .metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Ok(WitEventEnvelope {
        id: event.id.to_string(),
        topic: event.topic.clone(),
        type_: event.r#type.clone(),
        source: event.source.clone(),
        tenant: tenant_to_wit(&event.tenant)?,
        subject: event.subject.clone(),
        time: event.time.to_rfc3339(),
        correlation_id: event.correlation_id.clone(),
        payload_json: serde_json::to_string(&event.payload)?,
        metadata,
    })
}

#[allow(dead_code)]
fn from_wit_event(event: WitEventEnvelope) -> Result<EventEnvelope> {
    let time = DateTime::parse_from_rfc3339(&event.time)?.with_timezone(&Utc);
    let payload: Value = serde_json::from_str(&event.payload_json)
        .unwrap_or_else(|_| Value::String(event.payload_json.clone()));
    Ok(EventEnvelope {
        id: EventId::new(event.id)?,
        topic: event.topic,
        r#type: event.type_,
        source: event.source,
        tenant: tenant_from_wit(event.tenant)?,
        subject: event.subject,
        time,
        correlation_id: event.correlation_id,
        payload,
        metadata: event.metadata.into_iter().collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use greentic_types::TenantId;
    use std::collections::BTreeMap;

    fn sample_tenant() -> TenantCtx {
        TenantCtx::new("local".parse().unwrap(), TenantId::new("demo").unwrap())
    }

    #[test]
    fn maps_event_roundtrip() {
        let tenant = sample_tenant();
        let original = EventEnvelope {
            id: EventId::new("evt-123").unwrap(),
            topic: "greentic.demo".into(),
            r#type: "com.greentic.demo.v1".into(),
            source: "tester".into(),
            tenant: tenant.clone(),
            subject: Some("subject".into()),
            time: Utc::now(),
            correlation_id: Some("corr-1".into()),
            payload: serde_json::json!({"ok": true}),
            metadata: BTreeMap::from([("k".into(), "v".into())]),
        };

        let wit = to_wit_event(&original).expect("to_wit");
        let roundtrip = from_wit_event(wit).expect("from_wit");
        assert_eq!(roundtrip.id, original.id);
        assert_eq!(roundtrip.topic, original.topic);
        assert_eq!(roundtrip.r#type, original.r#type);
        assert_eq!(roundtrip.source, original.source);
        assert_eq!(roundtrip.subject, original.subject);
        assert_eq!(roundtrip.correlation_id, original.correlation_id);
        assert_eq!(roundtrip.metadata, original.metadata);
        assert_eq!(roundtrip.tenant, tenant);
    }
}
