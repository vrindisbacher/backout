use futures_util::FutureExt;
use std::{error::Error, fmt, panic::AssertUnwindSafe, pin::Pin, time::Duration};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type UndoFn<'a, E> = Box<dyn FnMut() -> BoxFuture<'a, Result<(), E>> + Send + 'a>;
/// Error returned by an undo handler
pub type UndoError = Box<dyn Error + Send + Sync>;

/// Failure for a transaction
///
/// A step can fail, and on top of that some undo handlers can fail
///
/// `#[non_exhaustive]` prevents client code from creating these variants.
/// Errors should come from failed steps.
#[derive(Debug)]
pub enum TransactionError<E> {
    /// The step failed and every undo for the previous steps succeeded
    #[non_exhaustive]
    Step { error: E },
    /// The step failed and at least one undo for the previous steps also failed
    #[non_exhaustive]
    Rollback {
        step: E,
        /// Errors from the undos that failed, in the order they ran (i.e., opposite order the
        /// operations were executed)
        undo: Vec<UndoError>,
    },
    /// The transaction was aborted (no rollback ran, so it may be partly applied)
    #[non_exhaustive]
    Aborted {},
}

impl<E: fmt::Display> fmt::Display for TransactionError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionError::Step { error } => write!(f, "step failed: {error}"),
            TransactionError::Rollback { step, undo } => {
                write!(f, "step failed: {step}; {} undo(s) also failed", undo.len())
            }
            TransactionError::Aborted {} => write!(f, "transaction aborted"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> Error for TransactionError<E> {}

fn backoff_duration(num_retries: i32) -> Duration {
    let base: f64 = 2.0;
    Duration::from_millis(((base.powi(num_retries) - 1.) * 100.) as u64)
}

/// Execute `operation` with exponential backoff
///
/// Either the op succeeds or we fail `max_retries + 1` times (retrying `max_retries` times)
/// with exponential backoff before each retry
async fn retry_with_backoff<Op, Fut, R, E>(mut operation: Op, max_retries: u32) -> Result<R, E>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<R, E>>,
{
    let mut num_tries = 0;
    loop {
        // delay
        tokio::time::sleep(backoff_duration(num_tries as i32)).await;

        // call the registered fn - if we succeed then return otherwise retry if we're under
        // max retries
        match operation().await {
            Ok(res) => return Ok(res),
            Err(e) => {
                num_tries += 1;
                if num_tries > max_retries {
                    return Err(e);
                }
            }
        }
    }
}

/// An undo handler
///
/// Registered to undo an operation that succeeds
struct Undo<'env> {
    handler: UndoFn<'env, UndoError>,
    max_retries: u32,
    exhausted: Option<ExhaustedFn<'env>>,
}

/// Hook that runs once every retry of an undo has failed
type ExhaustedFn<'env> = Box<dyn FnOnce() -> BoxFuture<'env, ()> + Send + 'env>;

/// Step: one operation of a transaction, retried with exponential backoff
///
/// Can be unwound by registering an `undo` handler (i.e. compensation for failure)
pub struct Step<'transaction, 'env, Op> {
    operation: Op,
    max_retries: u32,
    transaction: &'transaction mut Transaction<'env>,
}

impl<'transaction, 'env, Op> Step<'transaction, 'env, Op> {
    /// Create a new instance from a closure and max number of retries
    fn new(
        operation: Op,
        max_retries: u32,
        transaction: &'transaction mut Transaction<'env>,
    ) -> Self {
        Self {
            operation,
            max_retries,
            transaction,
        }
    }

    /// Set the number of retries for the operation if it fails initially
    pub fn with_retries(self, num_retries: u32) -> Self {
        Self {
            max_retries: num_retries,
            ..self
        }
    }

    /// Register an undo handler to unwind this op if a later one fails
    ///
    /// The handler receives a clone of the result of the step on every call
    pub fn undo<R, U, Fut>(self, mut on_failure: U) -> StepWithUndo<'transaction, 'env, Op, R>
    where
        R: Clone + Send + 'env,
        U: FnMut(R) -> Fut + Send + 'env,
        Fut: Future<Output = Result<(), UndoError>> + Send + 'env,
    {
        let make: MakeUndo<'env, R> = Box::new(move |result: &R| {
            let result = result.clone();
            // erase the closure's type (and its future's type) so undos have the same type
            let handler: UndoFn<'env, UndoError> =
                Box::new(move || Box::pin(on_failure(result.clone())));
            handler
        });
        StepWithUndo {
            step: self,
            undo: UndoSpec {
                make,
                max_retries: 0,
                exhausted: None,
            },
        }
    }
}

/// Builds the undo handler once the step's result is known
type MakeUndo<'env, R> = Box<dyn FnOnce(&R) -> UndoFn<'env, UndoError> + Send + 'env>;

/// An undo handler waiting for the result of its step
struct UndoSpec<'env, R> {
    make: MakeUndo<'env, R>,
    max_retries: u32,
    exhausted: Option<MakeExhausted<'env, R>>,
}

/// Builds the exhausted hook once the step's result is known
type MakeExhausted<'env, R> = Box<dyn FnOnce(&R) -> ExhaustedFn<'env> + Send + 'env>;

/// StepWithUndo: a `Step` with an undo handler to unwind operations when later steps fail
pub struct StepWithUndo<'transaction, 'env, Op, R> {
    step: Step<'transaction, 'env, Op>,
    undo: UndoSpec<'env, R>,
}

impl<'transaction, 'env, Op, R> StepWithUndo<'transaction, 'env, Op, R> {
    /// Set the number of retries for the operation if it fails initially
    pub fn with_retries(mut self, num_retries: u32) -> Self {
        self.step = self.step.with_retries(num_retries);
        self
    }

    /// Set the number of times the undo handler is retried (with backoff) if it fails
    ///
    /// NOTE: This requires the `undo` handler to be **idempotent**
    pub fn with_undo_retries(mut self, num_retries: u32) -> Self {
        self.undo.max_retries = num_retries;
        self
    }

    /// Register a hook that runs once if the undo still fails after all of its retries
    ///
    /// The hook receives a clone of the result of the step.
    /// Unwinding continues with the remaining undos after it completes.
    /// The hook should not panic.
    pub fn on_undo_exhausted<H, Fut>(mut self, hook: H) -> Self
    where
        R: Clone + Send + 'env,
        H: FnOnce(R) -> Fut + Send + 'env,
        Fut: Future<Output = ()> + Send + 'env,
    {
        let make: MakeExhausted<'env, R> = Box::new(move |result: &R| {
            let result = result.clone();
            let exhausted: ExhaustedFn<'env> = Box::new(move || Box::pin(hook(result)));
            exhausted
        });
        self.undo.exhausted = Some(make);
        self
    }
}

/// Run a step of the transaction with exponential backoff (if any)
///
/// if we succeed, register its undo (if any)
/// if we fail, we unwind the transaction
async fn run_step<'env, Op, Fut, R, E>(
    step: Step<'_, 'env, Op>,
    undo: Option<UndoSpec<'env, R>>,
) -> Result<R, TransactionError<E>>
where
    Op: FnMut() -> Fut,
    Fut: Future<Output = Result<R, E>>,
{
    let Step {
        operation,
        max_retries,
        transaction,
    } = step;

    match retry_with_backoff(operation, max_retries).await {
        Ok(res) => {
            // register the undo handler in the transaction if we're done
            if let Some(spec) = undo {
                transaction.stack.push(Undo {
                    handler: (spec.make)(&res),
                    max_retries: spec.max_retries,
                    exhausted: spec.exhausted.map(|make| make(&res)),
                });
            }
            Ok(res)
        }
        // failed so we must undo previous ops
        Err(e) => Err(match transaction.unwind().await {
            Ok(()) => TransactionError::Step { error: e },
            Err(undo) => TransactionError::Rollback { step: e, undo },
        }),
    }
}

/// Implementation of `IntoFuture` for `Step`
///
/// Used to force execution of the step with `.await`
impl<'transaction, 'env, Op, Fut, R, E> IntoFuture for Step<'transaction, 'env, Op>
where
    Op: FnMut() -> Fut + Send + 'transaction,
    Fut: Future<Output = Result<R, E>> + Send + 'transaction,
    R: Send + 'transaction,
    E: Send + 'transaction,
{
    type Output = Result<R, TransactionError<E>>;
    type IntoFuture = BoxFuture<'transaction, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(run_step(self, None))
    }
}

/// Implementation of `IntoFuture` for `StepWithUndo`
///
/// Used to force execution of the step with `.await`
impl<'transaction, 'env, Op, Fut, R, E> IntoFuture for StepWithUndo<'transaction, 'env, Op, R>
where
    Op: FnMut() -> Fut + Send + 'transaction,
    Fut: Future<Output = Result<R, E>> + Send + 'transaction,
    R: Send + 'transaction,
    E: Send + 'transaction,
{
    type Output = Result<R, TransactionError<E>>;
    type IntoFuture = BoxFuture<'transaction, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(run_step(self.step, Some(self.undo)))
    }
}

/// Transaction: transactions for arbitrary operations
///
/// NOTE: this api is not cancel safe.
///
/// Do not race `steps` of a transaction in a `select!`
/// or wrap them in `timeout`.
///
/// If a `timeout` is needed, put the `timeout` inside the operation
/// closure you register.
///
/// Roll back conditions:
///
/// - Rolled back: every step that completed with an `undo`, if a later step returns Err (or panics, when run with `transaction`).
/// - Not rolled back: if a step fails or panics, it isn't rolled back. **Best practice is to split that step into multiple steps**
/// - No rollback at all: a panic outside of `transaction`, a task abort or runtime shutdown, or a build with panic = "abort".
pub struct Transaction<'env> {
    stack: Vec<Undo<'env>>,
}

impl<'env> Transaction<'env> {
    /// Create a new Transaction
    pub fn new() -> Self {
        Self { stack: Vec::new() }
    }

    /// One step of the transaction
    pub fn step<'transaction, Op, Fut, R, E>(
        &'transaction mut self,
        closure: Op,
    ) -> Step<'transaction, 'env, Op>
    where
        Op: FnMut() -> Fut + Send + 'transaction,
        Fut: Future<Output = Result<R, E>> + Send + 'transaction,
    {
        Step::new(closure, 0, self)
    }

    /// Finish the transaction, dropping `Self` and the stack
    pub fn finish(self) {}

    /// Unwind in reverse order, running every undo (with its retries) and collecting the ones
    /// that still fail
    async fn unwind(&mut self) -> Result<(), Vec<UndoError>> {
        let mut failures = Vec::new();
        while let Some(undo) = self.stack.pop() {
            let Undo {
                handler,
                max_retries,
                exhausted,
            } = undo;
            if let Err(e) = retry_with_backoff(handler, max_retries).await {
                if let Some(exhausted) = exhausted {
                    exhausted().await;
                }
                failures.push(e);
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(failures)
        }
    }
}

/// A trait to require the future for `transaction`'s async closure to be `Send`
#[doc(hidden)]
pub trait TxFn<'a, T, E>:
    FnOnce(&'a mut Transaction<'static>) -> <Self as TxFn<'a, T, E>>::Fut
{
    type Fut: Future<Output = Result<T, TransactionError<E>>> + Send + 'a;
}

impl<'a, F, Fut, T, E> TxFn<'a, T, E> for F
where
    F: FnOnce(&'a mut Transaction<'static>) -> Fut,
    Fut: Future<Output = Result<T, TransactionError<E>>> + Send + 'a,
{
    type Fut = Fut;
}

/// Start a transaction in a tokio task
///
/// Useful for when running transactions within something
/// like axum or another framework that can cancel futures
/// (e.g., when a client disconnects in Axum).
///
/// The transaction here is run with a tokio task which doesn't get
/// dropped when the parent gets dropped.
///
/// If the code panics, completed steps are rolled back, and the panic is resumed.
/// Undo errors during this rollback are ignored.
///
/// Requires the closure's future to be `Send`.
///
/// NOTE: a task abort, runtime shutdown, or panic = "abort" can still cause a skipped `unwind`.
pub async fn transaction<F, T, E>(f: F) -> Result<T, TransactionError<E>>
where
    F: for<'a> TxFn<'a, T, E>
        + AsyncFnOnce(&mut Transaction<'static>) -> Result<T, TransactionError<E>>
        + Send
        + 'static,
    T: Send + 'static,
    E: Send + 'static,
{
    let task = tokio::spawn(async move {
        let mut t = Transaction::new();
        match AssertUnwindSafe(f(&mut t)).catch_unwind().await {
            Ok(Ok(value)) => {
                t.finish();
                Ok(value)
            }
            // The closure didn't panic, but returned an error.
            // That error usually comes from a failed step, which has already unwound,
            // but unwind again in case any undos are left.
            Ok(Err(e)) => {
                let _ = t.unwind().await;
                Err(e)
            }
            Err(panic) => {
                // catch the panic, unwind ops that happened and resume the panic
                //
                // ** note that if a `step` panics, and it is in flight or can have partial updates
                // it will not be rolled back
                let _ = t.unwind().await;
                std::panic::resume_unwind(panic)
            }
        }
    });
    match task.await {
        Ok(result) => result,
        Err(e) if e.is_panic() => std::panic::resume_unwind(e.into_panic()),
        Err(_) => Err(TransactionError::Aborted {}),
    }
}
