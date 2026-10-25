# Changelog

## 0.1.1 — 2026-10-25

- Use the normal idle deadline when a valid response omits power status.
- Increase reconnect backoff until a valid response arrives, even if TCP connects.
- Keep confirmed-off sessions quiet and retain the one post-shutdown listener rebind.

## 0.1.0 — 2026-10-25

- Recover power-state updates after external/manual power changes.
- Read complete frames and reconnect with a fresh state request.
- Stop recurring state requests once off is confirmed.
- Preserve Temescal 0.5 commands, keyword arguments and constants.
- Stop the listener on close and reject commands after shutdown.
