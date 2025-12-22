# Developer Manual

This is the detailed reference for engineers. It complements the high-level README.

## Domain Flows (Narrative)
1. Client selects services.
2. Masters filtered by matching skills.
3. Gap search computes valid start times respecting working hours, breaks, existing bookings, per-master durations.
4. Client selects slot; logical hold created.
5. Policies evaluated; payment flow starts if required; hold timer begins.
6. Payment success -> confirm; failure/timeout -> release.
7. Reminders scheduled in UTC; rendered in local time.
8. Master marks outcome; analytics update.

## Advisory Locking
- Acquire `pg_advisory_xact_lock(master_id, time_bucket)` before booking write.
- Holds for transaction lifetime; prevents double booking; auto-released on commit/rollback.

## Gap Search (Sketch)
- Inputs: working hours, breaks, existing bookings, requested services, durations, per-master speed.
- Build occupied intervals; derive free windows; subtract total duration; apply lead-time and cutoff policies.

## Payment Flow
- Issue Telegram invoice with provider payload.
- Start hold timer; worker monitors expiration.
- On success: confirm booking; on failure/timeout: release hold and notify.

## Policy Enforcement
- Cancellation/reschedule lock windows; optional fees.
- Lead time and maximum future window (e.g., 60 days) enforced in services.

## Navigation Stack
- Push on entering view (id, payload, cursor/filter state).
- Pop on Back; re-render prior view with stored payload.
- Prevents ad-hoc jumps; keeps UX deterministic.

## FSM Design
- Explicit states and events; guards validate transitions.
- Side effects live in services; FSM definitions stay pure.

## Error Handling
- Fail fast on config; user-facing errors are actionable.
- Correlation IDs in logs; PII minimized.

## Logging Fields
- `event`, `user_id`, `role`, `booking_id`, `master_id`, `service_ids`, `state`, `flow`, `correlation_id`, `latency_ms`.

## Background Jobs
- `reminder_send`: pre-visit reminders.
- `hold_cleanup`: release expired holds.
- `payment_reconcile`: verify payment state if needed.
- `digest_admin`: periodic summary (load, no-shows).

## Performance
- Index bookings on `(master_id, start_at, status)`; consider partial index on `status=confirmed`.
- Paginate admin lists; avoid unbounded scans.
- Cache static catalogs cautiously; prioritize correctness for bookings.

## Extending Services
- Add rules in services; avoid logic in handlers.
- Introduce value objects for new concepts (Deposit, Voucher).
- Cover invariants with unit tests; add integration tests for new flows.

## Adding Payment Providers
- Implement adapter: `create_invoice`, `verify_payment`, optional `refund`.
- Map provider states to internal Payment states; reuse hold/confirm pipeline.

## Adding Policies (Example: Buffer Time)
- Add buffer duration to policy config.
- Update gap search to include buffer before/after bookings.
- Test for overlap prevention and correct availability rendering.

## Timezone Handling
- Store `start_at_utc`; render with business/user TZ.
- Convert user selections to UTC before persistence.
- Reminders rely on stored UTC to avoid drift.

## Deployment Playbook
- Build image; push to registry.
- `docker compose pull && docker compose up -d` on server.
- Run Alembic migrations before/during rollout.
- Monitor logs/workers for 10–15 minutes post-deploy.

## Backup and Restore
- Nightly `pg_dump` to secure storage.
- Test restores periodically.
- Keep migration history for replay into fresh DB.

## Smoke Test (Local)
- Start stack.
- Seed services, master working hours.
- Book composite services; attempt concurrent booking to verify lock.
- Test payment success/failure (sandbox).
- Verify reminders scheduled and sent.
- Use Back navigation across multiple levels; confirm state restores.

## Coding Style
- Small, composable service functions.
- Dependency injection over globals.
- Guard clauses for invalid commands.

## Edge Cases
- Concurrent slot selection: advisory lock ensures single winner.
- Language change mid-flow: navigation re-renders in new language.
- Service disabled mid-flow: validation blocks and suggests alternatives.
- Late payment webhook: reconciler adjusts final state safely.

## Workers Reliability
- Idempotent actions; keyed by booking/payment ids.
- Safe to run multiple replicas; use backoff on transient failures.

## No-Shows
- Master marks no-show; status updates; metric increments.
- Optional fee or deposit requirement for flagged customers.

## Composite Services
- Aggregate durations; require matching skills; use combined duration in availability.
- Store pricing snapshot at booking time.

## Money Handling
- Store amount + currency; avoid float.
- Snapshot prices at booking for historical accuracy.

## Data Integrity
- Locks + constraints prevent overlap; foreign keys enforce relations.
- Status transitions validated; no skipping states.

## Pagination and Filtering
- Offset/limit with ordering by start time.
- Filters: master, service, status, date range.

## Admin Notifications
- On create, cancel, payment failure, no-show; throttle to reduce noise.

## Reminders
- Configurable lead times (e.g., 24h, 3h).
- Respect business TZ; store in UTC.
- Late-schedule handling: send immediately or skip per policy.

## Maintenance Windows
- Maintenance flag blocks new bookings while allowing reads; bot communicates status.

## Webhook vs Long Polling
- Webhook for low latency with HTTPS; long polling for dev/simpler hosts.

## User-Facing Errors
- Slot taken: propose nearest alternatives.
- Payment failed: prompt retry with context.
- Policy block: explain reason and next steps.

## Exports and Safety
- CSV exports filtered by date/status/master.
- Sanitize personal notes before sharing.
- Store exports securely and clean up after delivery.

## BI Metrics Definitions
- LTV: sum of realized revenue per customer.
- Retention: share of customers with repeat bookings in period.
- No-show rate: no-show/canceled near visit over confirmed.
- Revenue pipeline: confirmed+paid vs confirmed+unpaid.

## New Location (Future-friendly)
- Separate policy set per location (timezone, currency).
- Tag masters/services by location; gap search respects context.

## Quality Gates
- Lint/type checks pass.
- Unit + integration tests updated.
- Migration present and reversible when schema changes.

## Observability Playbook
- Ship structured logs to central stack.
- Dashboards: bookings created/failed, payment failures, reminder sends.
- Alerts on spikes in payment failure or no-show rate.

## Operations Playbook
- On-call watches worker health and DB metrics.
- Incident: trace via `correlation_id`, check locks/payments.
- Maintenance: announce, enable flag, run migrations, disable flag.

## Security Playbook
- Keep tokens/keys out of logs.
- Restrict DB network access.
- Use separate DB users for app vs migrations if desired.
- Review dependencies regularly.

## Scaling Playbook
- Add bot replicas; centralize state store (DB/cache).
- Scale DB vertically, then read replicas if needed.
- Tune pool sizes; watch lock wait times during bursts.

## Troubleshooting Checklist
- Booking not confirmed: payment status + hold worker.
- Double reminder: check idempotency keys.
- Wrong time: verify `BUSINESS_TZ` and rendering.
- Admin blocked: verify role in services.
