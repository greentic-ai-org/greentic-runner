use anyhow::{Result, anyhow};
use chrono::{DateTime, Utc};
use greentic_interfaces::{
    events_bridge_event_to_message_v1 as e2m_world, events_bridge_message_to_event_v1 as m2e_world,
};
use greentic_interfaces_host::events_bridge::{EventToMessageBridge, MessageToEventBridge};
use greentic_types::{
    Attachment, ChannelMessageEnvelope, EventEnvelope, EventId, GreenticError, TenantCtx,
};
use wasmtime::Store;
use wasmtime::component::{Linker, ResourceTable};

type WitTenantM2E = m2e_world::greentic::interfaces_types::types::TenantCtx;
type WitTenantE2M = e2m_world::greentic::interfaces_types::types::TenantCtx;
type WitImpersonationM2E = m2e_world::greentic::interfaces_types::types::Impersonation;
type WitImpersonationE2M = e2m_world::greentic::interfaces_types::types::Impersonation;
type WitEventM2E = m2e_world::greentic::events::event_types::EventEnvelope;
type WitEventE2M = e2m_world::greentic::events::event_types::EventEnvelope;
type WitMessageM2E = m2e_world::greentic::messaging::session_api::ChannelMessageEnvelope;
type WitMessageE2M = e2m_world::greentic::messaging::session_api::ChannelMessageEnvelope;
type WitAttachmentM2E = m2e_world::greentic::messaging::session_api::Attachment;
type WitAttachmentE2M = e2m_world::greentic::messaging::session_api::Attachment;

/// Invoke a message-to-event bridge component, returning mapped events.
pub fn invoke_message_to_event_bridge(
    engine: &wasmtime::Engine,
    component: &wasmtime::component::Component,
    tenant: &TenantCtx,
    message: &ChannelMessageEnvelope,
) -> Result<Vec<EventEnvelope>> {
    let mut store = Store::new(engine, ResourceTable::new());
    let linker = Linker::new(engine);
    let instance = MessageToEventBridge::instantiate(&mut store, component, &linker)
        .map_err(|err| anyhow!(err))?;
    let exports = instance.greentic_events_bridge_bridge_api();
    let wit_msg = to_wit_message_m2e(message, tenant)?;
    let results = exports.call_handle_message(&mut store, &wit_msg)?;
    results.into_iter().map(from_wit_event_m2e).collect()
}

/// Invoke an event-to-message bridge component, returning mapped channel messages.
pub fn invoke_event_to_message_bridge(
    engine: &wasmtime::Engine,
    component: &wasmtime::component::Component,
    tenant: &TenantCtx,
    event: &EventEnvelope,
) -> Result<Vec<ChannelMessageEnvelope>> {
    let mut store = Store::new(engine, ResourceTable::new());
    let linker = Linker::new(engine);
    let instance = EventToMessageBridge::instantiate(&mut store, component, &linker)
        .map_err(|err| anyhow!(err))?;
    let exports = instance.greentic_events_bridge_bridge_api();
    let wit_event = to_wit_event_e2m(event, tenant)?;
    let results = exports.call_handle_event(&mut store, &wit_event)?;
    results.into_iter().map(from_wit_message_e2m).collect()
}

fn tenant_to_wit_m2e(tenant: &TenantCtx) -> Result<WitTenantM2E> {
    let attributes = tenant
        .attributes
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let impersonation = tenant
        .impersonation
        .as_ref()
        .map(|imp| WitImpersonationM2E {
            actor_id: imp.actor_id.to_string(),
            reason: imp.reason.clone(),
        });
    Ok(WitTenantM2E {
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

fn tenant_to_wit_e2m(tenant: &TenantCtx) -> Result<WitTenantE2M> {
    let attributes = tenant
        .attributes
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    let impersonation = tenant
        .impersonation
        .as_ref()
        .map(|imp| WitImpersonationE2M {
            actor_id: imp.actor_id.to_string(),
            reason: imp.reason.clone(),
        });
    Ok(WitTenantE2M {
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

fn tenant_from_wit_m2e(tenant: WitTenantM2E) -> Result<TenantCtx> {
    tenant_from_wit(tenant)
}

fn tenant_from_wit_e2m(tenant: WitTenantE2M) -> Result<TenantCtx> {
    tenant_from_wit(tenant)
}

fn tenant_from_wit<T>(tenant: T) -> Result<TenantCtx>
where
    T: TenantFields,
{
    let env = tenant
        .env()
        .parse()
        .map_err(|err: GreenticError| anyhow!(err))?;
    let tenant_id = tenant
        .tenant_id()
        .parse()
        .map_err(|err: GreenticError| anyhow!(err))?;
    let mut ctx = TenantCtx::new(env, tenant_id);
    ctx.tenant = tenant
        .tenant()
        .parse()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.team = tenant
        .team()
        .map(|v| v.parse())
        .transpose()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.team_id = tenant
        .team_id()
        .map(|v| v.parse())
        .transpose()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.user = tenant
        .user()
        .map(|v| v.parse())
        .transpose()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.user_id = tenant
        .user_id()
        .map(|v| v.parse())
        .transpose()
        .map_err(|err: GreenticError| anyhow!(err))?;
    ctx.trace_id = tenant.trace_id().clone();
    ctx.correlation_id = tenant.correlation_id().clone();
    ctx.attributes = tenant.attributes().iter().cloned().collect();
    ctx.session_id = tenant.session_id().clone();
    ctx.flow_id = tenant.flow_id().clone();
    ctx.node_id = tenant.node_id().clone();
    ctx.provider_id = tenant.provider_id().clone();
    ctx.deadline = tenant
        .deadline_ms()
        .map(|ms| greentic_types::InvocationDeadline::from_unix_millis(ms as i128));
    ctx.attempt = tenant.attempt();
    ctx.idempotency_key = tenant.idempotency_key().clone();
    ctx.impersonation = tenant
        .impersonation()
        .map(|imp| {
            let actor = imp
                .actor_id
                .parse()
                .map_err(|err: GreenticError| anyhow!(err))?;
            Ok::<greentic_types::Impersonation, anyhow::Error>(greentic_types::Impersonation {
                actor_id: actor,
                reason: imp.reason.clone(),
            })
        })
        .transpose()?;
    Ok(ctx)
}

fn to_wit_event_e2m(event: &EventEnvelope, tenant: &TenantCtx) -> Result<WitEventE2M> {
    let metadata = event
        .metadata
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    Ok(WitEventE2M {
        id: event.id.to_string(),
        topic: event.topic.clone(),
        type_: event.r#type.clone(),
        source: event.source.clone(),
        tenant: tenant_to_wit_e2m(tenant)?,
        subject: event.subject.clone(),
        time: event.time.to_rfc3339(),
        correlation_id: event.correlation_id.clone(),
        payload_json: serde_json::to_string(&event.payload)?,
        metadata,
    })
}

fn from_wit_event_m2e(event: WitEventM2E) -> Result<EventEnvelope> {
    let time = DateTime::parse_from_rfc3339(&event.time)?.with_timezone(&Utc);
    let payload: serde_json::Value = serde_json::from_str(&event.payload_json)
        .unwrap_or_else(|_| serde_json::Value::String(event.payload_json.clone()));
    Ok(EventEnvelope {
        id: EventId::new(event.id)?,
        topic: event.topic,
        r#type: event.type_,
        source: event.source,
        tenant: tenant_from_wit_m2e(event.tenant)?,
        subject: event.subject,
        time,
        correlation_id: event.correlation_id,
        payload,
        metadata: event.metadata.into_iter().collect(),
    })
}

fn to_wit_message_m2e(
    message: &ChannelMessageEnvelope,
    tenant: &TenantCtx,
) -> Result<WitMessageM2E> {
    Ok(WitMessageM2E {
        id: message.id.clone(),
        tenant: tenant_to_wit_m2e(tenant)?,
        channel: message.channel.clone(),
        session_id: message.session_id.clone(),
        user_id: message.user_id.clone(),
        text: message.text.clone(),
        attachments: message
            .attachments
            .iter()
            .map(to_wit_attachment_m2e)
            .collect(),
        metadata: message
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect(),
    })
}

fn from_wit_message_e2m(message: WitMessageE2M) -> Result<ChannelMessageEnvelope> {
    let attachments = message
        .attachments
        .into_iter()
        .map(from_wit_attachment_e2m)
        .collect();
    Ok(ChannelMessageEnvelope {
        id: message.id,
        tenant: tenant_from_wit_e2m(message.tenant)?,
        channel: message.channel,
        session_id: message.session_id,
        user_id: message.user_id,
        text: message.text,
        attachments,
        metadata: message.metadata.into_iter().collect(),
    })
}

fn to_wit_attachment_m2e(attachment: &Attachment) -> WitAttachmentM2E {
    WitAttachmentM2E {
        mime_type: attachment.mime_type.clone(),
        url: attachment.url.clone(),
        name: attachment.name.clone(),
        size_bytes: attachment.size_bytes,
        description: None,
        metadata: Vec::new(),
    }
}

fn from_wit_attachment_e2m(attachment: WitAttachmentE2M) -> Attachment {
    Attachment {
        mime_type: attachment.mime_type,
        url: attachment.url,
        name: attachment.name,
        size_bytes: attachment.size_bytes,
    }
}

trait TenantFields {
    fn env(&self) -> &str;
    fn tenant(&self) -> &str;
    fn tenant_id(&self) -> &str;
    fn team(&self) -> Option<&str>;
    fn team_id(&self) -> Option<&str>;
    fn user(&self) -> Option<&str>;
    fn user_id(&self) -> Option<&str>;
    fn trace_id(&self) -> &Option<String>;
    fn correlation_id(&self) -> &Option<String>;
    fn attributes(&self) -> &Vec<(String, String)>;
    fn session_id(&self) -> &Option<String>;
    fn flow_id(&self) -> &Option<String>;
    fn node_id(&self) -> &Option<String>;
    fn provider_id(&self) -> &Option<String>;
    fn deadline_ms(&self) -> Option<i64>;
    fn attempt(&self) -> u32;
    fn idempotency_key(&self) -> &Option<String>;
    fn impersonation(&self) -> Option<ImpersonationView<'_>>;
}

struct ImpersonationView<'a> {
    actor_id: &'a str,
    reason: &'a Option<String>,
}

impl TenantFields for WitTenantM2E {
    fn env(&self) -> &str {
        &self.env
    }
    fn tenant(&self) -> &str {
        &self.tenant
    }
    fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
    fn team(&self) -> Option<&str> {
        self.team.as_deref()
    }
    fn team_id(&self) -> Option<&str> {
        self.team_id.as_deref()
    }
    fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }
    fn user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }
    fn trace_id(&self) -> &Option<String> {
        &self.trace_id
    }
    fn correlation_id(&self) -> &Option<String> {
        &self.correlation_id
    }
    fn attributes(&self) -> &Vec<(String, String)> {
        &self.attributes
    }
    fn session_id(&self) -> &Option<String> {
        &self.session_id
    }
    fn flow_id(&self) -> &Option<String> {
        &self.flow_id
    }
    fn node_id(&self) -> &Option<String> {
        &self.node_id
    }
    fn provider_id(&self) -> &Option<String> {
        &self.provider_id
    }
    fn deadline_ms(&self) -> Option<i64> {
        self.deadline_ms
    }
    fn attempt(&self) -> u32 {
        self.attempt
    }
    fn idempotency_key(&self) -> &Option<String> {
        &self.idempotency_key
    }
    fn impersonation(&self) -> Option<ImpersonationView<'_>> {
        self.impersonation.as_ref().map(|imp| ImpersonationView {
            actor_id: imp.actor_id.as_str(),
            reason: &imp.reason,
        })
    }
}

impl TenantFields for WitTenantE2M {
    fn env(&self) -> &str {
        &self.env
    }
    fn tenant(&self) -> &str {
        &self.tenant
    }
    fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
    fn team(&self) -> Option<&str> {
        self.team.as_deref()
    }
    fn team_id(&self) -> Option<&str> {
        self.team_id.as_deref()
    }
    fn user(&self) -> Option<&str> {
        self.user.as_deref()
    }
    fn user_id(&self) -> Option<&str> {
        self.user_id.as_deref()
    }
    fn trace_id(&self) -> &Option<String> {
        &self.trace_id
    }
    fn correlation_id(&self) -> &Option<String> {
        &self.correlation_id
    }
    fn attributes(&self) -> &Vec<(String, String)> {
        &self.attributes
    }
    fn session_id(&self) -> &Option<String> {
        &self.session_id
    }
    fn flow_id(&self) -> &Option<String> {
        &self.flow_id
    }
    fn node_id(&self) -> &Option<String> {
        &self.node_id
    }
    fn provider_id(&self) -> &Option<String> {
        &self.provider_id
    }
    fn deadline_ms(&self) -> Option<i64> {
        self.deadline_ms
    }
    fn attempt(&self) -> u32 {
        self.attempt
    }
    fn idempotency_key(&self) -> &Option<String> {
        &self.idempotency_key
    }
    fn impersonation(&self) -> Option<ImpersonationView<'_>> {
        self.impersonation.as_ref().map(|imp| ImpersonationView {
            actor_id: imp.actor_id.as_str(),
            reason: &imp.reason,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use greentic_types::TenantId;
    use std::collections::BTreeMap;

    fn sample_tenant() -> TenantCtx {
        TenantCtx::new("local".parse().unwrap(), TenantId::new("demo").unwrap())
    }

    #[test]
    fn maps_message_roundtrip() {
        let tenant = sample_tenant();
        let message = ChannelMessageEnvelope {
            id: "msg-1".into(),
            tenant: tenant.clone(),
            channel: "chan".into(),
            session_id: "sess".into(),
            user_id: Some("user".into()),
            text: Some("hello".into()),
            attachments: vec![Attachment {
                mime_type: "text/plain".into(),
                url: "https://example.com".into(),
                name: None,
                size_bytes: None,
            }],
            metadata: BTreeMap::from([("k".into(), "v".into())]),
        };
        let wit = to_wit_message_m2e(&message, &tenant).expect("to_wit");
        let back = from_wit_message_e2m(WitMessageE2M {
            id: wit.id.clone(),
            tenant: tenant_to_wit_e2m(&tenant).expect("tenant"),
            channel: wit.channel.clone(),
            session_id: wit.session_id.clone(),
            user_id: wit.user_id.clone(),
            text: wit.text.clone(),
            attachments: wit
                .attachments
                .iter()
                .map(|att| WitAttachmentE2M {
                    mime_type: att.mime_type.clone(),
                    url: att.url.clone(),
                    name: att.name.clone(),
                    size_bytes: att.size_bytes,
                    description: att.description.clone(),
                    metadata: att.metadata.clone(),
                })
                .collect(),
            metadata: wit.metadata.clone(),
        })
        .expect("from_wit");
        assert_eq!(back.id, message.id);
        assert_eq!(back.channel, message.channel);
        assert_eq!(back.session_id, message.session_id);
        assert_eq!(back.user_id, message.user_id);
        assert_eq!(back.text, message.text);
        assert_eq!(back.attachments.len(), message.attachments.len());
        assert_eq!(back.metadata, message.metadata);
        assert_eq!(back.tenant, tenant);
    }

    #[test]
    fn maps_event_roundtrip() {
        let tenant = sample_tenant();
        let event = EventEnvelope {
            id: EventId::new("evt-1").unwrap(),
            topic: "topic".into(),
            r#type: "type".into(),
            source: "source".into(),
            tenant: tenant.clone(),
            subject: None,
            time: Utc::now(),
            correlation_id: None,
            payload: serde_json::json!({"ok": true}),
            metadata: BTreeMap::from([("k".into(), "v".into())]),
        };
        let wit_e2m = to_wit_event_e2m(&event, &tenant).expect("to_wit");
        let back = from_wit_event_m2e(WitEventM2E {
            id: wit_e2m.id.clone(),
            topic: wit_e2m.topic.clone(),
            type_: wit_e2m.type_.clone(),
            source: wit_e2m.source.clone(),
            tenant: tenant_to_wit_m2e(&tenant).expect("tenant"),
            subject: wit_e2m.subject.clone(),
            time: wit_e2m.time.clone(),
            correlation_id: wit_e2m.correlation_id.clone(),
            payload_json: wit_e2m.payload_json.clone(),
            metadata: wit_e2m.metadata.clone(),
        })
        .expect("from_wit");
        assert_eq!(back.id, event.id);
        assert_eq!(back.topic, event.topic);
        assert_eq!(back.r#type, event.r#type);
        assert_eq!(back.source, event.source);
        assert_eq!(back.tenant, tenant);
        assert_eq!(back.metadata, event.metadata);
    }
}
