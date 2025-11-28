#![allow(clippy::not_unsafe_ptr_arg_deref)]

wit_bindgen::generate!({
    path: "wit/worker.wit",
    world: "worker",
});

use exports::greentic::worker::worker_api::{
    Guest, WorkerError, WorkerMessage, WorkerRequest, WorkerResponse,
};

struct WorkerComponent;

impl Guest for WorkerComponent {
    fn exec(req: WorkerRequest) -> Result<WorkerResponse, WorkerError> {
        Ok(WorkerResponse {
            version: req.version.clone(),
            tenant: req.tenant,
            worker_id: req.worker_id.clone(),
            correlation_id: req.correlation_id.clone(),
            session_id: req.session_id.clone(),
            thread_id: req.thread_id.clone(),
            messages: vec![WorkerMessage {
                kind: "echo".to_string(),
                payload_json: req.payload_json.clone(),
            }],
            timestamp_utc: req.timestamp_utc.clone(),
        })
    }
}

export!(WorkerComponent);
