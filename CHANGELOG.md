# Changelog

## 0.1.0 — unreleased

- Recover power-state updates after external/manual power changes.
- Read complete frames and reconnect with a fresh state request.
- Stop recurring state requests once off is confirmed.
- Preserve Temescal 0.5 commands, keyword arguments and constants.
- Stop the listener on close and reject commands after shutdown.
