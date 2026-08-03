use vector_lib::internal_event::{ComponentEventsDropped, Count, INTENTIONAL, Registered};

vector_lib::registered_event!(
    MetricToLogHydraEventsDropped => {
        events_dropped: Registered<ComponentEventsDropped<'static, INTENTIONAL>>
            = register!(ComponentEventsDropped::<INTENTIONAL>::from(
                "Metric timestamp outside accepted window (−10 min / +5 min)."
            )),
    }

    fn emit(&self, data: Count) {
        self.events_dropped.emit(data);
    }
);
