Gouda is an ActiveJob adapter used at Cheddar. It requires PostgreSQL and a recent version of Rails.

⚠️ At the moment Gouda is only used internally at Cheddar. We do not provide support for it, nor do we accept
issues or feature requests. This is likely to change in the future.

## Installation

```
$ bundle add gouda
$ bundle install
$ bin/rails g gouda:install
```

Gouda is build as a lightweight alternative to [good_job](https://github.com/bensheldon/good_job) and has been created before [solid_queue.](https://github.com/rails/solid_queue/)
It is _smaller_ than solid_queue though.

It was designed to enable job processing using `SELECT ... FOR UPDATE SKIP LOCKED` on Postgres so that we could use pg_bouncer in our system setup. 


## Key concepts in Gouda: Workload

Gouda is built around the concept of a **Workload.** A workload is not the same as an ActiveJob. A workload is a single execution of a task - the task may be an entire ActiveJob, or a retry of an ActiveJob, or a part of a sequence of ActiveJobs initiated using [job-iteration](https://github.com/shopify/job-iteration)

You can easily have multiple `Workloads` stored in your queue which reference the same job. However, when you are using Gouda it is important to always keep the distinction between the two in mind.

When an ActiveJob gets first initialised, it receives a randomly-generated ActiveJob ID, which is normally a UUID. This UUID will be reused when a job gets retried, or when job-iteration is in use - but it will exist across multiple Gouda workloads.

A `Workload` can only be in one of the three states: `enqueued`, `executing` and `finished`. It does not matter whether the workload has raised an exception, or was manually canceled before it started performing, or succeeded - its terminal state is always going to be `finished`, regardless. This is done on purpose: Gouda uses a number of partial indexes in Postgres which allows it to maintain uniqueness, but only among jobs which are either waiting to start or already running. Additionally, _only the transitions between those states_ are guarded by `BEGIN...COMMIT` and it is the selection on those states that is supplemented by `SELECT ... FOR UPDATE SKIP LOCKED`. The only time locks are places on a particular `gouda_workloads` row is when this update is about to take place (`SELECT` then `UPDATE`). This makes Gouda a good fit for use with pg_bouncer in transaction mode.

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

A `Workload` is executing on a particular `executing_on` entity - usually a worker thread. That entity gets a pseudorandom ID . The `executing_on` value can be used to see, for example, whether a particular worker thread has hung. If multiple jobs have a far-behind `updated_at` and are all `executing`, this likely means that the worker has crashed or hung. The value can also be used to build a table of currently running workers.

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

If multiple ActiveJob adapters and you bulkify a job which uses an adapter different than Gouda, `in_bulk` will try to use `enqueue_all` on that
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

## Web UI

At the moment the Gouda UI is proprietary, so this gem only provides a "headless" implementation. We expect this to change in the future.

