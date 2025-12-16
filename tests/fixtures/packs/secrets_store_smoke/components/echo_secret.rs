// Minimal component that asks for TEST_API_KEY via secrets-store.
wit_bindgen::generate!({
    path: "wit/echo-secret",
    world: "echo-secret",
    generate_all,
});

struct Component;

impl Guest for Component {
    fn run() -> String {
        use greentic::secrets_store::secrets_store;
        match secrets_store::get("TEST_API_KEY") {
            Ok(Some(_bytes)) => "{\"ok\":true,\"key_present\":true}".to_string(),
            Ok(None) => "{\"ok\":false,\"error\":\"missing\"}".to_string(),
            Err(_) => "{\"ok\":false,\"error\":\"missing\"}".to_string(),
        }
    }
}

export!(Component);
