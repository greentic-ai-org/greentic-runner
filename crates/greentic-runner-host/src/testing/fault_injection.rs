use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use once_cell::sync::Lazy;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum FaultPoint {
    BeforeComponentCall,
    AfterComponentCall,
    BeforeToolCall,
    AfterToolCall,
    StateRead,
    StateWrite,
    TemplateRender,
    PackResolve,
    Timeout,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum FaultErrorKind {
    Transient,
    Permanent,
    Timeout,
    Trap,
    BadInput,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum FaultMode {
    Always,
    Once,
    Nth(u32),
    Rate(u32),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FaultSpec {
    pub point: FaultPoint,
    pub mode: FaultMode,
    pub error_kind: FaultErrorKind,
    pub message: String,
}

#[derive(Clone, Debug)]
pub struct FaultContext<'a> {
    pub pack_id: &'a str,
    pub flow_id: &'a str,
    pub node_id: Option<&'a str>,
    pub attempt: u32,
}

#[derive(Clone, Debug)]
pub struct FaultInjector {
    specs: Vec<FaultSpec>,
    pack_id: Option<String>,
    flow_id: Option<String>,
    seed: u64,
    counters: HashMap<usize, u32>,
    max_attempt: u32,
    injected: u32,
}

#[derive(Clone, Debug, Default)]
pub struct FaultStats {
    pub max_attempt: u32,
    pub injected: u32,
}

#[derive(Clone, Debug)]
pub struct InjectedError {
    pub kind: FaultErrorKind,
    pub message: String,
}

impl fmt::Display for InjectedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let prefix = match self.kind {
            FaultErrorKind::Transient => "transient",
            FaultErrorKind::Permanent => "permanent dlq_recommended",
            FaultErrorKind::Timeout => "timeout",
            FaultErrorKind::Trap => "wasm_trap",
            FaultErrorKind::BadInput => "bad_input",
        };
        write!(f, "{}: {}", prefix, self.message)
    }
}

impl std::error::Error for InjectedError {}

static INJECTOR: Lazy<Mutex<Option<Arc<Mutex<FaultInjector>>>>> = Lazy::new(|| Mutex::new(None));

pub fn set_injector(injector: FaultInjector) {
    let mut guard = INJECTOR.lock();
    *guard = Some(Arc::new(Mutex::new(injector)));
}

pub fn clear_injector() {
    let mut guard = INJECTOR.lock();
    *guard = None;
}

pub fn stats() -> FaultStats {
    let guard = INJECTOR.lock();
    guard
        .as_ref()
        .map(|injector| {
            let injector = injector.lock();
            FaultStats {
                max_attempt: injector.max_attempt,
                injected: injector.injected,
            }
        })
        .unwrap_or_default()
}

pub fn maybe_fail(point: FaultPoint, ctx: FaultContext<'_>) -> Result<(), InjectedError> {
    let injector = {
        let guard = INJECTOR.lock();
        guard.as_ref().map(Arc::clone)
    };
    let Some(injector) = injector else {
        return Ok(());
    };
    let mut injector = injector.lock();
    injector.maybe_fail(point, ctx)
}

impl FaultInjector {
    pub fn new(specs: Vec<FaultSpec>) -> Self {
        Self {
            specs,
            pack_id: None,
            flow_id: None,
            seed: 0,
            counters: HashMap::new(),
            max_attempt: 0,
            injected: 0,
        }
    }

    pub fn with_pack_id(mut self, pack_id: impl Into<String>) -> Self {
        self.pack_id = Some(pack_id.into());
        self
    }

    pub fn with_flow_id(mut self, flow_id: impl Into<String>) -> Self {
        self.flow_id = Some(flow_id.into());
        self
    }

    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    fn maybe_fail(
        &mut self,
        point: FaultPoint,
        ctx: FaultContext<'_>,
    ) -> Result<(), InjectedError> {
        if let Some(pack_id) = &self.pack_id
            && pack_id != ctx.pack_id
        {
            return Ok(());
        }
        if let Some(flow_id) = &self.flow_id
            && flow_id != ctx.flow_id
        {
            return Ok(());
        }
        if ctx.attempt > self.max_attempt {
            self.max_attempt = ctx.attempt;
        }

        for (idx, spec) in self.specs.iter().enumerate() {
            if spec.point != point {
                continue;
            }
            let count = self.counters.entry(idx).or_insert(0);
            *count += 1;
            if !should_inject(spec.mode, *count, self.seed) {
                continue;
            }
            self.injected += 1;
            return Err(InjectedError {
                kind: spec.error_kind,
                message: spec.message.clone(),
            });
        }
        Ok(())
    }
}

fn should_inject(mode: FaultMode, count: u32, seed: u64) -> bool {
    match mode {
        FaultMode::Always => true,
        FaultMode::Once => count == 1,
        FaultMode::Nth(n) => count == n.max(1),
        FaultMode::Rate(rate) => {
            let rate = rate.min(100);
            let bucket = (seed.wrapping_add(count as u64) % 100) as u32;
            bucket < rate
        }
    }
}
