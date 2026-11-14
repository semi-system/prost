use super::*;
#[test]
fn serialized_config_must_fit_the_startup_read_limit() -> Result<()> {
    let mut config = Config::parse("schema_version=1\n[upstreams]\n[[listeners]]\nid='test'\naddress='127.0.0.1:8080'\nprotocol='http'\n[[listeners.plugins]]\nname='custom'\nconfig={padding=[]}\n")?;
    config.listeners[0].plugins[0].config.insert(
        "padding".into(),
        toml::Value::Array(vec![toml::Value::String(String::new()); 800_000]),
    );
    assert!(config
        .encode()
        .unwrap_err()
        .to_string()
        .contains("serialized config exceeds"));
    Ok(())
}
