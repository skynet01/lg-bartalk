# BarTalk

Talk to LG Wi-Fi soundbars without losing track of their power state.

BarTalk replaces the archived [Temescal 0.5](https://github.com/google/python-temescal)
client. It comes from a Home Assistant custom fix for an LG S95Q that could leave
HA showing on after the soundbar was switched off manually. It runs inside your
Python application, not as a separate service.

## Install

Python 3.10 or newer is required. Until a PyPI release is published:

```sh
python -m pip install git+https://github.com/skynet01/lg-bartalk.git
```

## Use

```python
from lg_bartalk import LGSoundbarClient

speaker = LGSoundbarClient("192.0.2.1", callback=print)
try:
    speaker.get_info()
    speaker.set_volume(17)
finally:
    speaker.close()
```

Pass a callback to receive decoded responses on the client's listener thread.
Always call `close()` when finished. Commands are synchronous; async applications
should run construction, commands and cleanup in an executor.

## Switching from Temescal

Change the import; keep your existing calls:

```python
import lg_bartalk as temescal

speaker = temescal.temescal("192.0.2.1", port=9741, callback=print, logger=None)
speaker.set_func(temescal.E_ARC)
speaker.set_eq(eq=temescal.CINEMA)
speaker.close()
```

The Temescal 0.5 constructor, public methods, keyword arguments, tables and
numeric constants are retained and tested against the original library.
`encrypt_packet()` still returns a complete wire frame. The old package is only
used by compatibility tests; it is not a runtime dependency.

Unlike Temescal's silent send failures, failed commands raise `OSError` after
one reconnect attempt, and malformed payloads raise `ValueError` when decrypted
directly. Only one listener can run at a time. Direct manipulation
of Temescal's socket/thread internals is not a supported migration path.

## When does it check?

- Normally it waits for soundbar notifications instead of polling.
- A new connection has five seconds to supply its first power response.
- While power is reported on, 30 seconds without data triggers a reconnect and state request.
- After a reported shutdown, five idle seconds trigger one reconnect to restore the listener.
- Once off is confirmed on that connection, recurring state requests stop.
- Broken connections reconnect with a bounded retry delay and request current power state.

It reads complete TCP frames, so split reads cannot silently discard an update.
Idle receive deadlines while off send no traffic; socket deadlines also bound commands.
No HA polling automation is needed. Device support follows Temescal's protocol;
the original fix was exercised on an S95Q, not every LG model.
LG's fixed protocol key is not network authentication; use this on a trusted LAN.

## Development

```sh
python -m pip install -e '.[test]'
python -m unittest discover -s tests -v
```

## License

Apache-2.0. Protocol constants and command payloads retain Google's original
attribution in [NOTICE](NOTICE).
