pub mod autovacuum;
pub mod concurrency;
pub mod logging;
pub mod memory;
pub mod planner;
pub mod table_index;
pub mod wal;
pub mod workload;

use crate::models::PgConfigParam;
use std::collections::HashMap;

pub(crate) mod query_parser;

pub(crate) fn get_param<'a>(
    params: &'a HashMap<String, PgConfigParam>,
    name: &str,
) -> Option<&'a PgConfigParam> {
    params.get(name)
}

pub(crate) fn param_value_as_bytes(param: &PgConfigParam) -> Option<u64> {
    let value = parse_numeric(&param.current_value)?;
    let multiplier = match param.unit.as_deref().map(|u| u.to_ascii_lowercase()) {
        Some(ref unit) if unit == "8kb" => 8.0 * 1024.0,
        Some(ref unit) if unit == "kb" => 1024.0,
        Some(ref unit) if unit == "mb" => 1024.0 * 1024.0,
        Some(ref unit) if unit == "gb" => 1024.0 * 1024.0 * 1024.0,
        Some(ref unit) if unit == "b" => 1.0,
        // Units such as "blocks" or empty are treated as already byte-aligned
        _ => 1.0,
    };

    Some((value * multiplier) as u64)
}

pub(crate) fn param_value_as_megabytes(param: &PgConfigParam) -> Option<u64> {
    param_value_as_bytes(param).map(|bytes| bytes / (1024 * 1024))
}

pub(crate) fn param_value_as_gigabytes(param: &PgConfigParam) -> Option<u64> {
    param_value_as_bytes(param).map(|bytes| bytes / (1024 * 1024 * 1024))
}

pub(crate) fn param_value_as_seconds(param: &PgConfigParam) -> Option<u64> {
    let value = parse_numeric(&param.current_value)?;
    let seconds = match param.unit.as_deref().map(|u| u.to_ascii_lowercase()) {
        Some(ref unit) if unit == "ms" => value / 1000.0,
        Some(ref unit) if unit == "s" => value,
        Some(ref unit) if unit == "min" => value * 60.0,
        Some(ref unit) if unit == "h" => value * 3600.0,
        Some(ref unit) if unit == "d" => value * 86400.0,
        _ => value,
    };

    Some(seconds as u64)
}

fn parse_numeric(value: &str) -> Option<f64> {
    value.trim().parse::<f64>().ok()
}
