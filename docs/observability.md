# Observability

Structured log events use [*structlog*](https://www.structlog.org).
Dispatcher and elected service events go to the `pgbg` logger.

For supervision and plain services, see [*bgt*'s observability documentation](https://bgt.hynek.me/stable/observability/).


## Prometheus metrics

The following [Prometheus](https://prometheus.io) metrics cover *pgbg*'s dispatcher and elected services.

`pgbg_dispatcher_last_cycle_timestamp_seconds{name}`
:   Timestamp of the dispatcher's last healthy loop cycle.
    It only advances on healthy loop cycles, so staleness is the alert signal:
    `time() - pgbg_dispatcher_last_cycle_timestamp_seconds > k * interval` means the dispatch loop is down and not recovering.
    Give *k* enough slack for the up to 30 seconds of restart backoff.

    The series exists at 0 from the start of the dispatch loop, so the alert also catches a dispatcher that never got going.

`pgbg_service_last_work_unit_timestamp_seconds{name}`
:   Timestamp of the elected service's last completed work unit.
    Alert on the fleet-wide maximum – `max by (name)` – because healthy followers legitimately sit idle at 0.

    The series exists at 0 from the start of a service's first loop run, without clobbering an earlier stamp.
    Therefore, a staleness alert never sits in no-data for a service that cannot complete a work unit.

`pgbg_service_lease_overruns_total{name}`
:   Work units that ended after their lease lapsed or was lost.
    An overrun means the lease was not kept alive while the work unit ran (renewals failed, the whole process stalled, or the lease was taken away) so another leader can exist.

    See the [overlap caveat](leader-election.md) for what that implies for your work.

`pgbg_service_lease_failures_total{name}`
:   Lease operations (elections and renewals) that failed and were downgraded to a warning.

    On a *follower*, a sustained rate means that it cannot win an election even though the fleet may look healthy: ergo a latent loss of failover capacity.

    On the *leader*, it means that it cannot renew and is about to lose its leadership.

    The leadership metric below tells you which of the two you are looking at.

`pgbg_service_leadership_confirmed_timestamp_seconds{name}`
:   Timestamp of this process's last confirmed leadership for the service; 0 on followers.

    It is restamped on every confirmed election and renewal, and reset to 0 on loss and resignation,
    so the fleet-wide maximum is always the current leader's last confirmation.
    A fleet-wide maximum older than the service's `lease_ttl` means that no process can confirm leadership.


### Alerting on leadership

Here's an example Prometheus alert that makes sure there is always a leader.

Followers export the leadership metric as 0, so the fleet-wide maximum is the current leader's last confirmation, and a stale maximum means that no process can confirm leadership.

```yaml
groups:
  - name: pgbg
    rules:
      - alert: PgbgServiceWithoutLeader
        expr: >-
          time()
          - max by (name)
            (pgbg_service_leadership_confirmed_timestamp_seconds)
          > 60
        for: 1m
        annotations:
          summary: >-
            No process has confirmed leadership for service
            {{ $labels.name }} in over a minute.
```

Keep the threshold above the service's `lease_ttl`, because a leader whose renewals fail keeps its lease for up to one TTL after its last confirmation.
The rule can only fire while at least one worker is scraped, so alert on the absence of your workers separately.
