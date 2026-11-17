//! Translate pod metadata into a logging entitlement; no admission state lives here.

use std::collections::BTreeMap;

use k8s_openapi::{api::core::v1::Pod, apimachinery::pkg::api::resource::Quantity};
use rust_decimal::Decimal;

/// A pod's weight and fixed-point fill rate, shared across all of its log sources.
#[derive(Debug, PartialEq)]
pub struct PodEntitlement<'a> {
    /// Kubernetes UID, not a reusable pod name.
    pub uid: &'a str,
    /// Logging units multiplied by 1,000.
    pub lu_milli: u64,
    /// One minute of supply, with 1,000,000 microtokens per event.
    pub microtokens_per_minute: u64,
}

impl<'a> PodEntitlement<'a> {
    /// Invalid or missing entitlement metadata returns `None` for the collective
    /// unattributed class. It must not create a separate reserve for each pod.
    pub fn from_pod(pod: &'a Pod, events_per_minute_per_lu: u64) -> Option<Self> {
        let uid = pod.metadata.uid.as_deref().filter(|uid| !uid.is_empty())?;
        let metadata = |key: &str| {
            pod.metadata
                .labels
                .as_ref()
                .and_then(|map| map.get(key))
                .or_else(|| pod.metadata.annotations.as_ref()?.get(key))
                .map(String::as_str)
        };
        let lu_milli = if let Some(value) = metadata("dblet.dev/logging-unit-request") {
            decimal_milli(value, 0, 0)?
        } else {
            let cpu = effective_cpu_milli(pod)?;
            match metadata("dblet.dev/log-volume").unwrap_or("M") {
                "S" => cpu.div_ceil(2),
                "M" => cpu,
                "L" => cpu.checked_mul(2)?,
                "XL" => cpu.checked_mul(4)?,
                _ => return None,
            }
        };
        let microtokens_per_minute = events_per_minute_per_lu
            .checked_mul(lu_milli)?
            .checked_mul(1_000)?;
        (microtokens_per_minute > 0).then_some(Self {
            uid,
            lu_milli,
            microtokens_per_minute,
        })
    }
}

fn effective_cpu_milli(pod: &Pod) -> Option<u64> {
    let spec = pod.spec.as_ref()?;
    let container_cpu = |container: &k8s_openapi::api::core::v1::Container| {
        cpu_milli(
            container
                .resources
                .as_ref()
                .and_then(|r| r.requests.as_ref()),
        )
    };
    let regular = spec.containers.iter().try_fold(0_u64, |total, container| {
        total.checked_add(container_cpu(container)?)
    })?;
    let init = spec
        .init_containers
        .iter()
        .flatten()
        .try_fold(0_u64, |maximum, container| {
            Some(maximum.max(container_cpu(container)?))
        })?;
    regular
        .max(init)
        .checked_add(cpu_milli(spec.overhead.as_ref())?)
}

fn cpu_milli(resources: Option<&BTreeMap<String, Quantity>>) -> Option<u64> {
    let Some(Quantity(value)) = resources.and_then(|r| r.get("cpu")) else {
        return Some(0);
    };
    // k8s-openapi's Quantity is only a string wrapper. Keep the Kubernetes
    // suffix adapter here and delegate decimal syntax/precision to rust_decimal.
    let (number, suffix) = value.split_at(value.find(char::is_alphabetic).unwrap_or(value.len()));
    let (decimal_power, binary_power) = match suffix {
        "" => (0, 0),
        "n" => (-9, 0),
        "u" => (-6, 0),
        "m" => (-3, 0),
        "k" => (3, 0),
        "M" => (6, 0),
        "G" => (9, 0),
        "T" => (12, 0),
        "P" => (15, 0),
        "E" => (18, 0),
        "Ki" => (0, 10),
        "Mi" => (0, 20),
        "Gi" => (0, 30),
        "Ti" => (0, 40),
        "Pi" => (0, 50),
        "Ei" => (0, 60),
        exponent if exponent.starts_with(['e', 'E']) => (exponent[1..].parse().ok()?, 0),
        _ => return None,
    };
    decimal_milli(number, decimal_power, binary_power)
}

fn decimal_milli(number: &str, decimal_power: i32, binary_power: u32) -> Option<u64> {
    let number = number.strip_prefix('+').unwrap_or(number);
    // Decimal also accepts underscores and negatives, which are not entitlements.
    if !number.bytes().all(|b| b.is_ascii_digit() || b == b'.') {
        return None;
    }
    let decimal = Decimal::from_str_exact(number).ok()?.normalize();
    let coefficient = u128::try_from(decimal.mantissa())
        .ok()?
        .checked_mul(1_u128 << binary_power)?;
    let power = decimal_power
        .checked_add(3)?
        .checked_sub(decimal.scale() as i32)?;
    let scale = 10_u128.checked_pow(power.unsigned_abs())?;
    // Integer scaling avoids Decimal multiplication rounding a fractional milli-LU.
    let milli = if power >= 0 {
        coefficient.checked_mul(scale)?
    } else {
        if coefficient % scale != 0 {
            return None;
        }
        coefficient / scale
    };
    milli.try_into().ok()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn pod(cpu: &str) -> Pod {
        serde_json::from_value(json!({
            "metadata": {"uid": "uid-1"},
            "spec": {"containers": [{"name": "app", "resources": {"requests": {"cpu": cpu}}}]}
        }))
        .unwrap()
    }

    fn lu(pod: &Pod) -> Option<u64> {
        PodEntitlement::from_pod(pod, 5_000).map(|entitlement| entitlement.lu_milli)
    }

    #[test]
    fn scheduling_cpu_uses_sum_max_init_and_overhead() {
        let mut pod: Pod = serde_json::from_value(json!({
            "metadata": {"uid": "uid-1"},
            "spec": {
                "containers": [
                    {"name": "a", "resources": {"requests": {"cpu": "250m"}}},
                    {"name": "b", "resources": {"requests": {"cpu": "0.75"}}}
                ],
                "initContainers": [
                    {"name": "i", "resources": {"requests": {"cpu": "2"}}},
                    {"name": "j", "resources": {"requests": {"cpu": "1500m"}}}
                ],
                "overhead": {"cpu": "50m"}
            }
        }))
        .unwrap();
        assert_eq!(lu(&pod), Some(2_050));
        pod.spec.as_mut().unwrap().init_containers = None;
        assert_eq!(lu(&pod), Some(1_050));
    }

    #[test]
    fn label_first_overrides_do_not_fall_back_after_invalid_values() {
        let mut pod = pod("1");
        pod.metadata.annotations = Some(BTreeMap::from([
            ("dblet.dev/logging-unit-request".into(), "9".into()),
            ("dblet.dev/log-volume".into(), "S".into()),
        ]));
        for (value, expected) in [("1.25", Some(1_250)), ("bad", None), ("0", None)] {
            pod.metadata.labels = Some(BTreeMap::from([
                ("dblet.dev/logging-unit-request".into(), value.into()),
                ("dblet.dev/log-volume".into(), "XL".into()),
            ]));
            assert_eq!(lu(&pod), expected);
        }
        pod.metadata.labels = None;
        assert_eq!(lu(&pod), Some(9_000));
        pod.metadata
            .annotations
            .as_mut()
            .unwrap()
            .remove("dblet.dev/logging-unit-request");
        assert_eq!(lu(&pod), Some(500));
        for (class, expected) in [
            ("S", Some(500)),
            ("M", Some(1_000)),
            ("L", Some(2_000)),
            ("XL", Some(4_000)),
            ("bad", None),
        ] {
            pod.metadata.labels = Some(BTreeMap::from([(
                "dblet.dev/log-volume".into(),
                class.into(),
            )]));
            assert_eq!(lu(&pod), expected);
        }
        pod.spec = self::pod("1m").spec;
        pod.metadata.labels = None;
        assert_eq!(lu(&pod), Some(1));
    }

    #[test]
    fn quantities_are_exact_milli_lu_or_unattributed() {
        for (value, expected) in [
            ("1e-3", Some(1)),
            ("1E-3", Some(1)),
            ("1E+0", Some(1_000)),
            ("1000000n", Some(1)),
            ("1000u", Some(1)),
            ("1k", Some(1_000_000)),
            ("1Ki", Some(1_024_000)),
            (".25", Some(250)),
            ("+1.", Some(1_000)),
            ("0", None),
            ("-1", None),
            ("NaN", None),
            ("1_0", None),
            ("1e100", None),
            ("1u", None),
            ("1.0001m", None),
            ("0.00000000000000000000000000001", None),
        ] {
            assert_eq!(lu(&pod(value)), expected, "{value}");
        }
        let mut missing = pod("1");
        missing.metadata.uid = None;
        assert_eq!(lu(&missing), None);
        missing.metadata.uid = Some("uid-1".into());
        missing.spec.as_mut().unwrap().containers[0].resources = None;
        assert_eq!(lu(&missing), None);
    }

    #[test]
    fn fill_rate_is_positive_and_fits_a_one_minute_bucket() {
        let pod = pod("1.25");
        assert_eq!(
            PodEntitlement::from_pod(&pod, 5_000),
            Some(PodEntitlement {
                uid: "uid-1",
                lu_milli: 1_250,
                microtokens_per_minute: 6_250_000_000,
            })
        );
        assert_eq!(PodEntitlement::from_pod(&pod, 0), None);
        for (cpu, expected) in [
            ("3689348814741m", Some(3_689_348_814_741)),
            ("3689348814742m", None),
        ] {
            assert_eq!(lu(&self::pod(cpu)), expected);
        }
    }
}
