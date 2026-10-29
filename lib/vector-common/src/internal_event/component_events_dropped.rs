use metrics::{Counter, counter};

use super::{Count, InternalEvent, InternalEventHandle, RegisterInternalEvent};
use crate::NamedInternalEvent;

pub const INTENTIONAL: bool = true;
pub const UNINTENTIONAL: bool = false;

#[derive(Debug, NamedInternalEvent)]
pub struct ComponentEventsDropped<'a, const INTENTIONAL: bool> {
    pub count: usize,
    pub reason: &'a str,
}

impl<const INTENTIONAL: bool> InternalEvent for ComponentEventsDropped<'_, INTENTIONAL> {
    fn emit(self) {
        let count = self.count;
        self.register().emit(Count(count));
    }
}

impl<'a, const INTENTIONAL: bool> From<&'a str> for ComponentEventsDropped<'a, INTENTIONAL> {
    fn from(reason: &'a str) -> Self {
        Self { count: 0, reason }
    }
}

impl<'a, const INTENTIONAL: bool> RegisterInternalEvent
    for ComponentEventsDropped<'a, INTENTIONAL>
{
    type Handle = DroppedHandle<'a, INTENTIONAL>;
    fn register(self) -> Self::Handle {
        Self::Handle {
            discarded_events: counter!(
                "component_discarded_events_total",
                "intentional" => if INTENTIONAL { "true" } else { "false" },
            ),
            reason: self.reason,
            error_code: None,
        }
    }
}

/// [`ComponentEventsDropped`] for a drop whose cause has been classified, adding an
/// `error_code` label to `component_discarded_events_total`.
///
/// `error_code` must come from a fixed set (a `match` on an error type), never from error
/// text, so the label stays bounded. Drops that are not classified keep using
/// [`ComponentEventsDropped`] and its unlabeled series.
#[derive(Debug, NamedInternalEvent)]
pub struct ComponentEventsDroppedWithCode<'a, const INTENTIONAL: bool> {
    pub count: usize,
    pub reason: &'a str,
    pub error_code: &'static str,
}

impl<const INTENTIONAL: bool> InternalEvent for ComponentEventsDroppedWithCode<'_, INTENTIONAL> {
    fn emit(self) {
        let count = self.count;
        self.register().emit(Count(count));
    }
}

impl<'a, const INTENTIONAL: bool> RegisterInternalEvent
    for ComponentEventsDroppedWithCode<'a, INTENTIONAL>
{
    type Handle = DroppedHandle<'a, INTENTIONAL>;
    fn register(self) -> Self::Handle {
        Self::Handle {
            discarded_events: counter!(
                "component_discarded_events_total",
                "intentional" => if INTENTIONAL { "true" } else { "false" },
                "error_code" => self.error_code,
            ),
            reason: self.reason,
            error_code: Some(self.error_code),
        }
    }
}

#[derive(Clone)]
pub struct DroppedHandle<'a, const INTENDED: bool> {
    discarded_events: Counter,
    reason: &'a str,
    error_code: Option<&'static str>,
}

impl<const INTENDED: bool> InternalEventHandle for DroppedHandle<'_, INTENDED> {
    type Data = Count;
    fn emit(&self, data: Self::Data) {
        let message = "Events dropped";
        if INTENDED {
            debug!(
                message,
                intentional = INTENDED,
                count = data.0,
                reason = self.reason,
                error_code = self.error_code,
            );
        } else {
            error!(
                message,
                intentional = INTENDED,
                count = data.0,
                reason = self.reason,
                error_code = self.error_code,
            );
        }
        self.discarded_events.increment(data.0 as u64);
    }
}
