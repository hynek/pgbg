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


## [Unreleased](https://github.com/hynek/pgbg/compare/26.1.0...HEAD)

### Added

- `pgbg.sqlalchemy.init_db()` that takes a SQLAlchemy Engine or Connection.
  [#1](https://github.com/hynek/pgbg/pull/1)


### Changed

- The `service.notified` log event is now called `service.woken`.
  It fires on any wakeup, not only on notifications.


## [26.1.0](https://github.com/hynek/pgbg/tree/26.1.0) - 2026-09-02

### Added

- Initial public release.
