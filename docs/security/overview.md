# Orbit Scheduler: security

Scheduled callbacks run with the permissions and process identity of the application. This package
does not authenticate job definitions, authorize callback actions, isolate tenants, or restrict
which resources a callback can access.

## Callback and schedule control

Treat callback registration as application configuration. Do not allow an untrusted request to
register arbitrary callbacks or choose sensitive schedules without authorization and explicit
bounds. Keep callbacks narrowly scoped and use provider identities with least privilege.

The scheduler is process-local. A job starts in every worker that constructs and starts the
scheduler. A multi-worker deployment can therefore multiply side effects; do not rely on this
package for singleton work or security-sensitive one-time actions.

## Failure data and logs

`JobFailure` intentionally excludes exception messages and tracebacks, which may contain
credentials, request bodies, or customer data. Exception class labels are sanitized before
reporting. Job names are restricted to safe ASCII characters. Error handlers should preserve the
same data-minimization policy and avoid attaching sensitive values to metrics labels.

## Shutdown and resource handling

Shutdown eventually cancels active callbacks. Cancellation is cooperative, and Python code can
suppress it; this package cannot forcibly terminate a task. Ensure callbacks close owned resources
in `finally` blocks and do not assume a `CLOSED` scheduler means a cancellation-resistant callback
has already stopped.

## Scope

Follow the repository [security policy](../../SECURITY.md) for private vulnerability reporting.
This guidance does not audit callback code, application authorization, or deployment permissions.
