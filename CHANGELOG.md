## [0.1.15]

- Enable Rails 8 dependency versions
- Add CI tests for Rails 7 and Rails 8 going forward, using Appraisal
- Silence SQL when upserting Scheduler workloads, as it is spammy

## [0.1.14]

- Relax `fugit` dependency to allow updates

## [0.1.13]

- Ensure we won't execute workloads which were scheduled but are no longer present in the cron table entries.

## [0.1.12]

- When doing polling, suppress DEBUG-level messages. This will stop Gouda spamming the logs with SQL in dev/test environments.

## [0.1.11]

- Fix: make sure the Gouda logger config does not get used during Rails initialization

## [0.1.10]

- Fix: remove logger overrides that Gouda should install, as this causes problems for Rails apps hosting Gouda

## [0.1.9]

- Fix: cleanup_preserved_jobs_before in Gouda::Workload.prune now points to Gouda.config

## [0.1.8]

- Move some missed instrumentations to Gouda.instrument

## [0.1.7]

- Separate all instrumentation to use ActiveSupport::Notification

## [0.1.6]

- Fix: don't upsert workloads twice when starting Gouda.
- Add back in Appsignal calls

## [0.1.5]

- Update documentation
- Don't pass on scheduler keys to retries

## [0.1.4]

- Rescue NoDatabaseError at scheduler update.
- Include tests in gem, for sake of easier debugging.
- Reduce logging in local test runs.
- Bump local ruby version to 3.3.3

## [0.1.3]

- Allow the Rails app to boot even if there is no database yet

## [0.1.2]

- Updated readme and method renaming in Scheduler

## [0.1.1]

- Fix support for older ruby versions until 2.7

## [0.1.0]

- Initial release

