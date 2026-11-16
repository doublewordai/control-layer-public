
use dashmap::DashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ConcurrencyAdjustment {
    pub previous_limit: usize,
    pub new_limit: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Generation(u64);

#[derive(Debug)]
struct ModelState {
    limit: usize,
    generation: u64,
    dispatched: usize,
}

impl ModelState {
    fn new(configured: usize) -> Self {
        Self {
            limit: configured.max(1),
            generation: 0,
            dispatched: 0,
        }
    }

    fn open_new_generation(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.dispatched = 0;
    }
}

#[derive(Debug)]
pub(super) struct AdaptiveConcurrencyController {
    states: DashMap<String, ModelState>,
    growth_factor: f64,
    cut_factor: f64,
}

impl AdaptiveConcurrencyController {
    pub(super) fn new(growth_factor: f64, cut_factor: f64) -> Self {
        Self {
            states: DashMap::new(),
            growth_factor: growth_factor.clamp(1.01, 10.0),
            cut_factor: cut_factor.clamp(0.05, 0.99),
        }
    }

    pub(super) fn limit(&self, model: &str, configured: usize) -> usize {
        self.states
            .entry(model.to_owned())
            .or_insert_with(|| ModelState::new(configured))
            .limit
    }

    pub(super) fn stamp(&self, model: &str, configured: usize) -> Generation {
        let mut state = self
            .states
            .entry(model.to_owned())
            .or_insert_with(|| ModelState::new(configured));
        state.dispatched = state.dispatched.saturating_add(1);
        Generation(state.generation)
    }

    pub(super) fn record_overload(
        &self,
        model: &str,
        generation: Generation,
    ) -> Option<ConcurrencyAdjustment> {
        let mut state = self.states.get_mut(model)?;
        if state.generation != generation.0 {
            return None;
        }

        let previous_limit = state.limit;
        let scaled = (previous_limit as f64 * self.cut_factor).floor() as usize;
        let new_limit = scaled.min(previous_limit.saturating_sub(1)).max(1);

        state.limit = new_limit;
        state.open_new_generation();

        (new_limit != previous_limit).then_some(ConcurrencyAdjustment {
            previous_limit,
            new_limit,
        })
    }

    pub(super) fn try_grow(&self, model: &str) -> Option<ConcurrencyAdjustment> {
        let mut state = self.states.get_mut(model)?;

        let previous_limit = state.limit;
        let grown = (previous_limit as f64 * self.growth_factor).ceil() as usize;
        let new_limit = grown.max(previous_limit.saturating_add(1));

        if state.dispatched < new_limit - previous_limit {
            return None;
        }

        state.limit = new_limit;
        state.open_new_generation();

        Some(ConcurrencyAdjustment {
            previous_limit,
            new_limit,
        })
    }
}

pub(super) fn available_capacity_for_model(limit: usize, in_flight: usize) -> usize {
    limit.saturating_sub(in_flight)
}

#[cfg(test)]
mod tests {

    use super::*;

    const GROWTH: f64 = 1.5;
    const CUT: f64 = 0.8;

    fn controller() -> AdaptiveConcurrencyController {
        AdaptiveConcurrencyController::new(GROWTH, CUT)
    }

    #[test]
    fn starts_at_the_configured_limit_and_can_go_above_it() {
        let controller = controller();
        assert_eq!(controller.limit("m", 100), 100);

        assert_eq!(controller.try_grow("m"), None);

        for _ in 0..50 {
            controller.stamp("m", 100);
        }
        assert!(controller.try_grow("m").is_some());
        assert_eq!(controller.limit("m", 100), 150);
    }

    #[test]
    fn recovery_from_a_cut_takes_a_handful_of_steps_at_any_scale() {
        let controller = controller();
        let start = 60_000;

        let generation = controller.stamp("m", start);
        controller.record_overload("m", generation);
        assert!(controller.limit("m", start) < start);

        let mut steps = 0;
        while controller.limit("m", start) < start {
            let limit = controller.limit("m", start);
            for _ in 0..limit {
                controller.stamp("m", start);
            }
            controller.try_grow("m");
            steps += 1;
            assert!(steps <= 5, "took {steps} steps to recover");
        }
    }

    #[test]
    fn a_model_being_rejected_does_not_climb() {
        let controller = controller();
        let mut limit = controller.limit("m", 150);

        for _ in 0..200 {
            let generation = controller.stamp("m", 150);
            for _ in 0..limit {
                controller.stamp("m", 150);
            }
            controller.record_overload("m", generation);
            controller.try_grow("m");

            let next = controller.limit("m", 150);
            assert!(
                next <= limit,
                "limit climbed from {limit} to {next} while everything was being rejected"
            );
            limit = next;
        }

        assert_eq!(limit, 1, "sustained rejection should walk the limit down");
    }

    #[test]
    fn overload_cuts_multiplicatively() {
        let controller = controller();
        let generation = controller.stamp("m", 100);

        assert_eq!(
            controller.record_overload("m", generation),
            Some(ConcurrencyAdjustment {
                previous_limit: 100,
                new_limit: 80,
            })
        );
        assert_eq!(controller.limit("m", 100), 80);
    }

    #[test]
    fn a_burst_of_overloads_costs_exactly_one_cut() {
        let controller = controller();
        let generation = controller.stamp("m", 100);

        assert!(controller.record_overload("m", generation).is_some());
        for _ in 0..50 {
            assert_eq!(controller.record_overload("m", generation), None);
        }
        assert_eq!(controller.limit("m", 100), 80);
    }

    #[test]
    fn sustained_overload_still_walks_the_limit_down() {
        let controller = controller();

        let first = controller.stamp("m", 100);
        assert!(controller.record_overload("m", first).is_some());

        let second = controller.stamp("m", 100);
        assert_eq!(
            controller.record_overload("m", second),
            Some(ConcurrencyAdjustment {
                previous_limit: 80,
                new_limit: 64,
            })
        );
    }

    #[test]
    fn overloads_from_a_stale_generation_are_ignored() {
        let controller = controller();
        let stale = controller.stamp("m", 100);
        assert!(controller.record_overload("m", stale).is_some());
        assert_eq!(controller.limit("m", 100), 80);

        for _ in 0..40 {
            controller.stamp("m", 100);
        }
        assert!(controller.try_grow("m").is_some());
        assert_eq!(controller.record_overload("m", stale), None);
        assert_eq!(controller.limit("m", 100), 120);
    }

    #[test]
    fn small_limits_still_make_progress_downward_and_never_reach_zero() {
        let controller = AdaptiveConcurrencyController::new(GROWTH, 0.99);

        let mut previous = controller.limit("m", 4);
        for _ in 0..10 {
            let generation = controller.stamp("m", 4);
            controller.record_overload("m", generation);
            let current = controller.limit("m", 4);
            assert!(current < previous || current == 1);
            previous = current;
        }
        assert_eq!(controller.limit("m", 4), 1);
    }

    #[test]
    fn models_are_independent() {
        let controller = controller();
        let generation = controller.stamp("a", 100);
        controller.record_overload("a", generation);

        assert_eq!(controller.limit("a", 100), 80);
        assert_eq!(controller.limit("b", 100), 100);
    }
}
