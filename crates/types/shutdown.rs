/// The indexer service has shut down: its checkpoint sender has been dropped,
/// so no stream built from it can ever advance.
///
/// Returned instead of a bare message so consumers can tell an orderly shutdown
/// apart from a genuine failure by downcasting, rather than matching on text. A
/// consumer that sees this should stop its own task quietly — the shutdown is
/// expected, and logging it as an error turns a clean teardown into noise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ServiceShutDown;

impl core::fmt::Display for ServiceShutDown {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("Checkpoint height channel is closed; the service is shut down")
    }
}

impl core::error::Error for ServiceShutDown {}
