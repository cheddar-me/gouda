Gouda is an ActiveJob adapter used at Cheddar. It requires PostgreSQL and a recent version of Rails.

> [!CAUTION]
> At the moment Gouda is only used internally at Cheddar. Any support to external parties is on best-effort
> basis. While we are happy to see issues and pull requests, we can't guarantee that those will be addressed
> quickly. The library does receive rapid updates which may break your application if you come to depend on
> the library. That is to be expected.

## Installation

```
$ bundle add gouda
$ bundle install
$ bin/rails g gouda:install
```

Gouda is a lightweight alternative to [good_job](https://github.com/bensheldon/good_job) and [solid_queue.](https://github.com/rails/solid_queue/) - while
more similar to the latter. It has been created prior to solid_queue and is smaller. It was designed to enable job processing using `SELECT ... FOR UPDATE SKIP LOCKED`
on Postgres so that we could use pg_bouncer in our system setup. We have also observed that `SKIP LOCKED` causes less load on our database than advisory locking,
especially as queue depths would grow. 


## Key concepts in Gouda: Workload

Gouda is built around the concept of a **Workload.** A workload is not the same as an ActiveJob. A workload is a single execution of a task - the task may be an entire ActiveJob, or a retry of an ActiveJob, or a part of a sequence of ActiveJobs initiated using [job-iteration](https://github.com/shopify/job-iteration)

You can easily have multiple `Workloads` stored in your queue which reference the same job. However, when you are using Gouda it is important to always keep the distinction between the two in mind.

When an ActiveJob gets first initialised, it receives a randomly-generated ActiveJob ID, which is normally a UUID. This UUID will be reused when a job gets retried, or when job-iteration is in use - but it will exist across multiple Gouda workloads.

A `Workload` can only be in one of the three states: `enqueued`, `executing` and `finished`. It does not matter whether the workload has raised an exception, or was manually canceled before it started performing, or succeeded - its terminal state is always going to be `finished`, regardless. This is done on purpose: Gouda uses a number of partial indexes in Postgres which allows it to maintain uniqueness, but only among jobs which are either waiting to start or already running. Additionally, _only the transitions between those states_ are guarded by `BEGIN...COMMIT` and it is the selection on those states that is supplemented by `SELECT ... FOR UPDATE SKIP LOCKED`. The only time locks are placed on a particular `gouda_workloads` row is when this update is about to take place (`SELECT` then `UPDATE`). This makes Gouda a good fit for use with pg_bouncer in transaction mode.

Understanding workload identity is key for making good use of Gouda. For example, an ActiveJob that gets retried can take the following shape in Gouda:

```
 ____________________________         _______________________________________________
| ActiveJob(id="0abc-...34") | ----> |  Workload(id="f67b-...123",state="finished")  |
 ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾        ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
 ____________________________         _______________________________________________
| ActiveJob(id="0abc-...34") | ----> |  Workload(id="5e52-...456",state="finished")  |
 ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾        ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
 ____________________________         _______________________________________________
| ActiveJob(id="0abc-...34") | ----> |  Workload(id="8a41-...789",state="enqueued")  |
 ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾        ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
```

This would happen if, for example, the ActiveJob raises an exception inside `perform` and is configured to `retry_on` after this exception. Same for job-iteration:

```
 _______________________________________         _______________________________________________
| ActiveJob(id="0abc-...34",cursor=nil) | ----> |  Workload(id="f67b-...123",state="finished")  |
 ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾        ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
 _______________________________________         _______________________________________________
| ActiveJob(id="0abc-...34",cursor=123) | ----> |  Workload(id="5e52-...456",state="finished")  |
 ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾        ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
 _______________________________________         _______________________________________________
| ActiveJob(id="0abc-...34",cursor=456) | ----> |  Workload(id="8a41-...789",state="executing") |
 ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾        ‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾‾
```

A key thing to remember when reading the Gouda source code is that **workloads and jobs are not the same thing.** A single job **may span multiple workloads.**

## Key concepts in Gouda: concurrency keys

Gouda has a few indexes on the `gouda_workloads` table which will:

* Forbid inserting another `enqueued` workload with the same `enqueue_concurrency_key` value. Uniqueness is on that column only.
* Forbid a workload from transition into `executing` when another workload with the same `execution_concurrency_key` is already running.

These are compatible with good_job concurrency keys, with one major distinction: we use unique indices and not counters, so these keys can be used
to **prevent concurrent executions** but not to **limit the load on the system**, and the limit of 1 is always enforced.

## Key concepts in Gouda: `executing_on`

A `Workload` is executing on a particular `executing_on` entity - usually a worker thread or fiber. That entity gets a pseudorandom ID. The `executing_on` value can be used to see, for example, whether a particular worker thread has hung. If multiple jobs have a far-behind `updated_at` and are all `executing`, this likely means that the worker executing them has died or has hung.

The `executing_on` field now clearly indicates the execution context:
- Thread-based execution: `hostname-pid-uuid-thread-abc123` 
- Hybrid execution (threads + fibers): `hostname-pid-uuid-thread-abc123-fiber-def456`

You can programmatically check the execution context:

```ruby
workload = Gouda::Workload.last
workload.executed_on_thread?  # => true/false
workload.uses_async_execution?  # => true/false  
workload.execution_context    # => :thread, :fiber, or :unknown
```

## Usage tips: bulkify your enqueues

When possible, Gouda uses `enqueue_all` to `INSERT` as many jobs at once as possible. With modern servers this allows for very rapid insertion of very large
batches of jobs. It is supplemented by a module which will make all `perform_later` calls buffered and submitted to the queue in bulk:

```ruby
Gouda.in_bulk do
  User.joined_recently.find_each do |user|
    WelcomeMailer.with(user:).welcome_email.deliver_later
  end
end
```

If there are multiple ActiveJob adapters configured and you bulk-enqueue a job which uses an adapter different than Gouda, `in_bulk` will try to use `enqueue_all` on that
adapter as well.

## Usage tips: co-commit

Gouda is designed to `COMMIT` the workload together with your business data. It does not need `after_commit` unless you so choose. In fact,
the main advantage of DB-based job queues such as Gouda is that you can always rely on the fact that the workload will be enqueued only
once the data it needs to operate on is already available for reading. This is guaranteed to work:

```ruby
User.transaction do
  freshly_joined_user = User.create!(user_params)
  WelcomeMailer.with(user: freshly_joined_user).welcome_email.deliver_later
end
```

## Hybrid Execution: Threads + Fibers

Gouda supports two execution modes: traditional thread-based execution and hybrid execution that combines multiple threads with multiple fibers per thread. The hybrid mode provides non-blocking IO capabilities while still utilizing multiple CPU cores.

### Execution Modes

**Thread-based execution (default):**
- Multiple worker threads
- One job per thread at a time
- Simple and reliable
- Good for CPU-bound work

**Hybrid execution (threads + fibers):**
- Multiple worker threads, each containing multiple fibers
- Much higher concurrency (threads × fibers per thread)
- Non-blocking IO within each thread
- Best of both worlds: CPU parallelism + IO concurrency

### Requirements

- Ruby 3.1+ with Fiber.scheduler support
- `async` gem dependency (~> 2.25, automatically included)
- **Rails isolation level set to `:fiber`** (critical for hybrid mode)
- Async-compatible gems for full benefit

### Critical Configuration for Hybrid Mode

**⚠️ IMPORTANT**: When using hybrid execution **with PostgreSQL**, you **must** configure Rails to use fiber-based isolation:

```ruby
# config/application.rb
class Application < Rails::Application
  # This is REQUIRED for Gouda hybrid mode with PostgreSQL
  config.active_support.isolation_level = :fiber
end
```

**Why this matters for PostgreSQL:**
- Prevents segmentation faults with Ruby 3.4+ and PostgreSQL adapter
- Ensures ActiveRecord connection pools work correctly with fibers and PostgreSQL connections
- Required for proper fiber-based concurrency with PostgreSQL's connection handling

**Note**: This configuration is specifically required when using PostgreSQL. Other database adapters may not have the same requirement, but setting it to `:fiber` is generally safe and recommended when using Gouda's hybrid mode.

Gouda will automatically detect if this setting is missing when using PostgreSQL and warn you during startup.

### Configuration

To enable hybrid execution (threads + fibers):

```ruby
Gouda.configure do |config|
  # Enable hybrid scheduler (threads + fibers)
  config.use_fiber_scheduler = true
  
  # Number of worker threads (should match your CPU cores)
  config.worker_thread_count = 4
  
  # Number of fibers per thread (can be much higher)
  config.fibers_per_thread = 10  # This means 10 fibers PER thread
  
  # Total concurrency = worker_thread_count × fibers_per_thread
  # In this example: 4 threads × 10 fibers = 40 concurrent jobs
end
```

**Traditional thread-based execution (default):**

```ruby
Gouda.configure do |config|
  # Thread-based execution (default)
  config.use_fiber_scheduler = false
  
  # Number of worker threads
  config.worker_thread_count = 4  # Total concurrency = 4
end
```

### Benefits

- **Non-blocking IO**: Database queries, HTTP requests, and file operations don't block other jobs
- **CPU parallelism**: Multiple threads utilize multiple CPU cores
- **Higher concurrency**: Can handle many more concurrent jobs with less memory overhead than pure threading
- **Better resource utilization**: Cooperative scheduling reduces context switching overhead
- **Backward compatibility**: Thread-based mode remains the default and continues to work

### When to use hybrid vs thread execution

**Use hybrid execution (threads + fibers) for:**
- IO-bound jobs (HTTP requests, database queries, file processing)
- High-concurrency scenarios
- Jobs that spend time waiting for external resources
- Mixed workloads with both CPU and IO requirements

**Use thread-based execution for:**
- CPU-intensive jobs
- Jobs that use gems without async support
- Simpler deployment scenarios
- When you want predictable, simple execution

You can even run both modes simultaneously with different queue constraints to optimize for different job types.

## Web UI

At the moment the Gouda UI is proprietary, so this gem only provides a "headless" implementation. We expect this to change in the future.