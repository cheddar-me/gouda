## [Unreleased]

## [0.1.0] - 2024-06-10

- Initial release

## [0.1.1] - 2024-06-10

- Fix support for older ruby versions until 2.7

## [0.1.2] - 2024-06-11

- Updated readme and method renaming in Scheduler

## [0.1.3] - 2024-06-11

- Allow the Rails app to boot even if there is no database yet

## [0.1.4] - 2024-06-14

- Rescue NoDatabaseError at scheduler update.
- Include tests in gem, for sake of easier debugging.
- Reduce logging in local test runs.
- Bump local ruby version to 3.3.3

## [0.1.5] - 2024-06-18

- Update documentation
- Don't pass on scheduler keys to retries

## [0.1.6] - 2024-06-18

- Fix: don't upsert workloads twice when starting Gouda.
- Add back in Appsignal calls

## [0.1.7] - 2024-06-21

- Separate all instrumentation to use ActiveSupport::Notification

## [0.1.8] - 2024-06-21

- Move some missed instrumentations to Gouda.instrument

## [0.1.9] - 2024-06-26

- Fix: cleanup_preserved_jobs_before in Gouda::Workload.prune now points to Gouda.config

## [0.1.10] - 2024-07-03

- Fix: remove logger overrides that Gouda should install, as this causes problems for Rails apps hosting Gouda