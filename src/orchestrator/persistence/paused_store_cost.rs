//! Storage and operation cost for a paused-sandbox bucket, from measured bytes.
//!
//! The spike behind moving paused sandboxes off node-local SSD has to answer
//! "what does it cost per month", and the answer must be arithmetic over measured
//! numbers rather than an estimate. This module takes the bytes one pause
//! uploads, the paused population and the pause rate, and applies the published
//! GCS price schedule; every price is a named constant, so a price change is a
//! one-line diff and a wrong constant is visible in review.
//!
//! It is a pure function of its inputs and talks to no bucket, so it is unit
//! tested here and the spike harness (`crates/benchmarks/benches/paused_store`)
//! only has to supply measurements — which is what keeps the printed monthly
//! figure and the code path that will gate the adoption decision the same one.

/// GCS pricing, US-CENTRAL1, STANDARD class, from the official price schedule.
/// Prices are per GiB-month and per 1,000 operations.
///
/// Source: <https://cloud.google.com/storage/pricing> (US-CENTRAL1, STANDARD,
/// retrieved 2026-11-05). Egress to a reader in the bucket's own region is not
/// billed, which is what makes resume-from-bucket a storage-only cost for a node
/// in the same region; a cross-region reader would add egress.
pub const STANDARD_STORAGE_USD_PER_GIB_MONTH: f64 = 0.020;
/// Class A operations (writes, list). A pause writes one object per artifact.
pub const CLASS_A_USD_PER_1K: f64 = 0.005;
/// Class B operations (reads). A resume reads one object per artifact.
pub const CLASS_B_USD_PER_1K: f64 = 0.0004;

/// Bytes in a GiB, matching the unit the price schedule uses.
const BYTES_PER_GIB: f64 = 1024.0 * 1024.0 * 1024.0;
/// Average days per month, used to turn a residency in hours into billable time.
const DAYS_PER_MONTH: f64 = 30.4375;
const HOURS_PER_DAY: f64 = 24.0;

/// What one measured pause costs to store and move.
#[derive(Clone, Copy, Debug)]
pub struct PausedStoreCost {
    /// Bytes uploaded by one pause, summed over every artifact object.
    pub bytes_per_pause: u64,
    /// Objects one pause uploads, which is also the resume's object count.
    pub objects_per_pause: u64,
    /// How long a paused sandbox stays paused before it is resumed or deleted.
    pub residency_hours: f64,
    /// How many pauses happen per month. Each is also a resume eventually.
    pub pauses_per_month: u64,
}

/// The monthly cost, split so the storage and operation halves are auditable
/// separately.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MonthlyCost {
    pub storage_usd: f64,
    pub class_a_usd: f64,
    pub class_b_usd: f64,
}

impl MonthlyCost {
    pub fn total_usd(&self) -> f64 {
        self.storage_usd + self.class_a_usd + self.class_b_usd
    }
}

impl PausedStoreCost {
    /// Steady-state cost when `paused_population` sandboxes are paused at once.
    ///
    /// Storage bills the bytes of the whole population; operations bill every
    /// pause as a Class A write and every resume as a Class B read of the same
    /// number of objects.
    pub fn monthly(&self, paused_population: u64) -> MonthlyCost {
        let storage_usd = self.resident_gib(paused_population) * STANDARD_STORAGE_USD_PER_GIB_MONTH;

        let operations = (self.pauses_per_month as f64 * self.objects_per_pause as f64) / 1_000.0;
        MonthlyCost {
            storage_usd,
            class_a_usd: operations * CLASS_A_USD_PER_1K,
            class_b_usd: operations * CLASS_B_USD_PER_1K,
        }
    }

    /// GiB the bucket holds for `paused_population` paused sandboxes, which is
    /// the quantity the storage term is built from.
    pub fn resident_gib(&self, paused_population: u64) -> f64 {
        (self.bytes_per_pause as f64 / BYTES_PER_GIB) * paused_population as f64
    }

    /// Hours in a billing month, so a residency can be stated as a fraction of
    /// the period it was priced over.
    pub fn month_hours() -> f64 {
        DAYS_PER_MONTH * HOURS_PER_DAY
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The arithmetic must be reproducible by hand: 10 GiB for a month at the
    /// published rate is the price schedule's own number.
    #[test]
    fn storage_matches_the_published_rate() {
        let cost = PausedStoreCost {
            bytes_per_pause: 10 * 1024 * 1024 * 1024,
            objects_per_pause: 1,
            residency_hours: 24.0,
            pauses_per_month: 30,
        };

        let monthly = cost.monthly(1);

        assert!((monthly.storage_usd - 0.20).abs() < 1e-9);
        assert_eq!(cost.resident_gib(1), 10.0);
    }

    /// 43 paused at ~24 GiB each is the volume the card states, so the storage
    /// term must reflect it without a second multiplication at the call site.
    #[test]
    fn current_volume_scales_with_population() {
        let cost = PausedStoreCost {
            bytes_per_pause: 24 * 1024 * 1024 * 1024,
            objects_per_pause: 4,
            residency_hours: 48.0,
            pauses_per_month: 100,
        };

        let monthly = cost.monthly(43);

        assert!((cost.resident_gib(43) - 1032.0).abs() < 1e-6);
        assert!((monthly.storage_usd - 20.64).abs() < 1e-9);
    }

    /// Operations are per object, so a pause split into four artifacts costs four
    /// Class A writes and four Class B reads.
    #[test]
    fn operations_count_objects_not_pauses() {
        let cost = PausedStoreCost {
            bytes_per_pause: 1,
            objects_per_pause: 4,
            residency_hours: 1.0,
            pauses_per_month: 10_000,
        };

        let monthly = cost.monthly(1);

        assert!((monthly.class_a_usd - 0.20).abs() < 1e-9);
        assert!((monthly.class_b_usd - 0.016).abs() < 1e-9);
    }

    /// The total is exactly the sum of its parts, so a report never has to
    /// recompute it with a different rounding.
    #[test]
    fn total_is_the_sum_of_the_parts() {
        let monthly = PausedStoreCost {
            bytes_per_pause: 24 * 1024 * 1024 * 1024,
            objects_per_pause: 4,
            residency_hours: 12.0,
            pauses_per_month: 500,
        }
        .monthly(43);

        assert_eq!(
            monthly.total_usd(),
            monthly.storage_usd + monthly.class_a_usd + monthly.class_b_usd
        );
    }

    /// A month of paused time is a plausible month, which is what lets a report
    /// compare population-hours against the billing period.
    #[test]
    fn month_hours_is_a_plausible_month() {
        let hours = PausedStoreCost::month_hours();

        assert!(hours > 720.0 && hours < 745.0, "got {hours}");
    }
}
