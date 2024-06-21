## [Unreleased]

## [0.1.0] - 2023-06-10

- Initial release

## [0.1.1] - 2023-06-10

- Fix support for older ruby versions until 2.7

## [0.1.2] - 2023-06-11

- Updated readme and method renaming in Scheduler

## [0.1.3] - 2023-06-11

- Allow the Rails app to boot even if there is no database yet

## [0.1.4] - 2023-06-14

- Rescue NoDatabaseError at scheduler update.
- Include tests in gem, for sake of easier debugging.
- Reduce logging in local test runs.
- Bump local ruby version to 3.3.3

## [0.1.5] - 2023-06-18

- Update documentation
- Don't pass on scheduler keys to retries

## [0.1.6] - 2023-06-18

- Fix: don't upsert workloads twice when starting Gouda.
- Add back in Appsignal calls

## [0.1.7] - 2023-06-21

- Separate all instrumentation to use ActiveSupport::Notification

## [0.1.8] - 2023-06-21

- Move some missed instrumentations to Gouda.instrument
