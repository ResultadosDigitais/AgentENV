//! The task's decisions: when it starts, in which mode, and against which
//! sandbox list.
//!
//! The sweep itself is tested where it lives (`maintenance::paused_artifacts`);
//! what is proved here is the wiring, which is where a report pass could
//! silently become an enforce one.

use super::*;
use crate::cfg::PausedArtifactReapMode;

#[test]
fn the_off_switch_starts_no_task() {
    assert_eq!(
        reap_mode(PausedArtifactReapMode::Off, Some("http://gw:8080")),
        None
    );
    assert_eq!(reap_mode(PausedArtifactReapMode::Off, None), None);
}

#[test]
fn report_runs_in_report_even_with_a_gateway() {
    assert_eq!(
        reap_mode(PausedArtifactReapMode::Report, Some("http://gw:8080")),
        Some(ReapMode::Report)
    );
}

#[test]
fn enforce_without_a_gateway_downgrades_to_report() {
    assert_eq!(
        reap_mode(PausedArtifactReapMode::Enforce, None),
        Some(ReapMode::Report),
        "a pass that cannot see the other nodes can prove nothing orphaned, so it must not enforce"
    );
}

#[test]
fn enforce_with_a_gateway_runs_in_enforce() {
    assert_eq!(
        reap_mode(PausedArtifactReapMode::Enforce, Some("http://gw:8080")),
        Some(ReapMode::Enforce)
    );
}
