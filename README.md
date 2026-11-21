# backout

Run async steps with retries and exponential backoff, undoing completed steps if later steps fail.

Here is a short example:

```rust
use backout::Transaction;

let mut t = Transaction::new();

t.step(|| payments.charge(&order))
    .with_retries(3)
    .undo(|| payments.refund(&order))
    .with_undo_retries(3)
    .await?;

t.step(|| warehouse.reserve(&order))
    .undo(|| warehouse.release(&order))
    .await?;

t.step(|| email.send_receipt(&order)).await?;

t.finish();
```

At a high level, the `undo` handlers registered with `.undo` guarantee that 
if `reserve` fails, `refund` will run and if `send_receipt` fails, `release` will run,
followed by `refund`.

Operations are retried with exponential backoff, and undos may also retry.
It is expected that undo handlers with retries are **idempotent**.

## A note on Web Frameworks

Frameworks like axum drop the handler's future when the client disconnects, which stops a
transaction halfway. `transaction` runs steps in a tokio task instead, allowing it to finish
or roll back properly:

```rust
use backout::transaction;

let receipt = transaction(async move |t| {
    t.step(|| app.payments.charge(&order)).await?;
    t.step(|| app.warehouse.reserve(&order)).await?;
    Ok(Receipt::new(&order))
})
.await?;
```

`transaction` also rolls back steps if the closure panics, and then resumes the panic.

## Undo Handlers

Not all undo handlers run. In particular, the semantics of which handlers run are as follows.

- Steps that completed and have an `undo`, newest first (i.e., reverse execution order).
- If a step fails, its `undo` handler does not run. **If partial updates** are possible, split each `step` into smaller changes that can be rolled back in a complete way.
- If a task aborts, the underlying async runtime shuts down, or the build uses `panic = "abort"`, no steps will be unwound.

