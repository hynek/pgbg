# Changelog

This file records notable changes to *pgbg*.

The format is based on [Keep a Changelog](https://keepachangelog.com/).
This project uses [Calendar Versioning](https://calver.org/).

The first version number is the release year.
The second number starts at 1 each year and increases with each release.
The third number identifies emergency releases from older branches.

> [!IMPORTANT]
> This package is in beta.
> The code is production-grade, but APIs can change before the first stable release.

<!-- changelog follows -->


## [Unreleased](https://github.com/hynek/pgbg/compare/26.2.0...HEAD)


## [26.2.0](https://github.com/hynek/pgbg/compare/26.1.0...26.2.0) - 2026-09-06

### Removed

- The supervision layer moved to the new [*bgt*](https://github.com/hynek/bgt) package, which *pgbg* now depends on.
  `pgbg.Supervisor`, `pgbg.Service`, `pgbg.SupervisedService`, `pgbg.IntervalOnlyWakeup`, `pgbg.as_work_factory`, `pgbg.exceptions.SuppressedCrashError`, and the `pgbg.typing.Loop`, `pgbg.typing.Wakeup`, `pgbg.typing.DoWork`, and `pgbg.typing.WorkFactory` protocols are gone.
  Import them from `bgt` instead; their behavior is unchanged.


### Added

- `pgbg.sqlalchemy.init_db()` that takes a SQLAlchemy Engine or Connection.
  [#1](https://github.com/hynek/pgbg/pull/1)


### Changed

- The `service.notified` log event is now called `service.woken`.
  It fires on any wakeup, not only on notifications.

- The restart counter is now called `bgt_supervisor_restarts_total`, because *bgt*'s supervisor drives every loop.
  The metrics of elected services keep their `pgbg_` prefix.
  Supervision log events now go to the `bgt` logger; everything else stays on `pgbg`.


## [26.1.0](https://github.com/hynek/pgbg/tree/26.1.0) - 2026-09-02

### Added

- Initial public release.
