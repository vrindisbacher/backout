# backout

Run async steps with retries and exponential backoff, undoing completed steps if later steps fail.

Here is an example. Starting a job means writing a record, launching a server, and then marking
the job as running. If any step fails, the steps that already happened are undone, newest first:

```rust
use backout::Transaction;

async fn start_job(db: &Database, cloud: &Cloud, job_id: &str) -> Result<(), Error> {
    let mut t = Transaction::new();

    // Step 1: write the job record. Retry up to 3 times if it fails.
    // If a later step fails, mark the job as failed (also retried up to 3 times).
    t.step(|| db.insert_job(job_id))
        .with_retries(3)
        .undo(|()| db.mark_failed(job_id))
        .with_undo_retries(3)
        .await?;

    // Step 2: launch a server. The step returns a `Server`.
    let server = t
        .step(|| cloud.launch_server(job_id))
        .with_retries(1)
        // The undo handler receives a clone of the step's result (so it must be `Clone`),
        // which is how it knows which server to terminate.
        .undo(|server: Server| async move { cloud.terminate_server(&server).await })
        .with_undo_retries(5)
        // If terminating still fails after all 5 retries, this runs once.
        // Use it to log or alert: the server is still running and costing money.
        .on_undo_exhausted(|server: Server| async move {
            eprintln!("{} is still running and needs manual cleanup", server.id);
        })
        .await?;

    // Step 3: the last step has no undo. If it fails, steps 1 and 2 are undone.
    t.step(|| db.mark_running(job_id, &server))
        .with_retries(3)
        .await?;

    // Everything succeeded, so there is nothing left to undo.
    t.finish();
    Ok(())
}
```

If the database goes down before step 3 succeeds, the undo handlers run in reverse order and
`start_job` returns the error from step 3:

```text
db:    insert job 43
cloud: launched srv-43
cloud: terminated srv-43
db:    mark job 43 failed
```

Operations are retried with exponential backoff, and undos may also retry.
It is expected that undo handlers with retries are **idempotent**.

<details>
<summary>The types used above</summary>

```rust
use std::error::Error as StdError;

type Error = Box<dyn StdError + Send + Sync>;

#[derive(Clone)]
struct Server {
    id: String,
}

struct Database {
    healthy: bool,
}

impl Database {
    async fn insert_job(&self, job_id: &str) -> Result<(), Error> {
        println!("db:    insert job {job_id}");
        Ok(())
    }

    async fn mark_failed(&self, job_id: &str) -> Result<(), Error> {
        println!("db:    mark job {job_id} failed");
        Ok(())
    }

    async fn mark_running(&self, job_id: &str, server: &Server) -> Result<(), Error> {
        if !self.healthy {
            return Err("database unavailable".into());
        }
        println!("db:    job {job_id} running on {}", server.id);
        Ok(())
    }
}

struct Cloud;

impl Cloud {
    async fn launch_server(&self, job_id: &str) -> Result<Server, Error> {
        let server = Server {
            id: format!("srv-{job_id}"),
        };
        println!("cloud: launched {}", server.id);
        Ok(server)
    }

    async fn terminate_server(&self, server: &Server) -> Result<(), Error> {
        println!("cloud: terminated {}", server.id);
        Ok(())
    }
}
```

</details>

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

