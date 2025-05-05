use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Metric {
    #[serde(rename = "t")]
    pub timestamp_ms: u128,
    #[serde(rename = "v")]
    pub value: U64OrF64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum U64OrF64 {
    U64(u64),
    F64(f64),
}
