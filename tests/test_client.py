"""Tests for the local LG soundbar protocol client."""

import json
import socket
import struct
import unittest
from collections import deque
from threading import Event, Lock, Thread
from unittest.mock import patch

from lg_bartalk import LGSoundbarClient


class FakeSocket:
    """Socket double that can fragment reads and block while idle."""

    def __init__(
        self,
        reads: list[bytes | BaseException] | None = None,
        *,
        timeout_when_idle: bool = False,
    ) -> None:
        self._reads = deque(reads or [])
        self._lock = Lock()
        self._wake = Event()
        self._timeout: float | None = None
        self._timeout_when_idle = timeout_when_idle
        self.recv_entered = Event()
        self.idle_waiting = Event()
        self.sent: list[bytes] = []
        self.closed = False

    def recv(self, size: int) -> bytes:
        """Return at most size bytes from the scripted stream."""
        self.recv_entered.set()
        while True:
            with self._lock:
                if self._reads:
                    item = self._reads.popleft()
                    if isinstance(item, BaseException):
                        raise item
                    if len(item) > size:
                        self._reads.appendleft(item[size:])
                        return item[:size]
                    return item
                if self.closed:
                    return b""
                if self._timeout is not None and self._timeout_when_idle:
                    self._timeout_when_idle = False
                    raise TimeoutError
            self.idle_waiting.set()
            self._wake.wait(1)
            self._wake.clear()

    def sendall(self, data: bytes) -> None:
        """Record a complete outgoing packet."""
        if self.closed:
            raise ConnectionResetError
        self.sent.append(data)

    def settimeout(self, timeout: float | None) -> None:
        """Accept blocking-mode changes made by the client."""
        self._timeout = timeout

    def gettimeout(self) -> float | None:
        """Expose the active socket deadline."""
        return self._timeout

    def shutdown(self, _how: int) -> None:
        """Wake a blocked reader."""
        self.closed = True
        self._wake.set()

    def close(self) -> None:
        """Close the socket."""
        self.closed = True
        self._wake.set()


class SocketFactory:
    """Return scripted sockets in connection order."""

    def __init__(self, *sockets: FakeSocket) -> None:
        self._sockets = deque(sockets)

    def __call__(self, _address: tuple[str, int], timeout: float) -> FakeSocket:
        """Return the next socket."""
        del timeout
        return self._sockets.popleft()


class TimeoutFailureSocket(FakeSocket):
    """Simulate a connection closing while its read timeout is updated."""

    def settimeout(self, timeout: float | None) -> None:
        if timeout == 30:
            raise ConnectionResetError
        super().settimeout(timeout)


class LGSoundbarClientTest(unittest.TestCase):
    """Verify framing, reconnect behavior, and command compatibility."""

    def test_response_without_power_status_uses_normal_idle_deadline(self) -> None:
        """Models without power status must not retain the initial short deadline."""
        for response in (
            {"msg": "SPK_LIST_VIEW_INFO", "data": {"i_vol": 10}},
            {"msg": "PRODUCT_INFO", "data": {"s_model_name": "test"}},
        ):
            with self.subTest(message=response["msg"]):
                connection = FakeSocket([LGSoundbarClient.encode_packet(response)])
                delivered = Event()
                with patch(
                    "lg_bartalk.socket.create_connection", return_value=connection
                ):
                    client = LGSoundbarClient(
                        "192.0.2.1",
                        callback=lambda _, delivered=delivered: delivered.set(),
                    )
                    try:
                        self.assertTrue(delivered.wait(1))
                        self.assertEqual(connection.gettimeout(), 30)
                        self.assertEqual(connection.sent, [])
                    finally:
                        client.close()

    def test_unusable_sessions_increase_recovery_backoff(self) -> None:
        """Accepting TCP alone must not reset retry delays for unhealthy peers."""
        for failure in (TimeoutError(), b"\x10\x00\x00\x00\x00"):
            with self.subTest(failure=repr(failure)):
                connections = [FakeSocket([failure]) for _ in range(8)]
                delays = []
                with patch(
                    "lg_bartalk.socket.create_connection", SocketFactory(*connections)
                ):
                    client = LGSoundbarClient("192.0.2.1")

                    def wait(delay, delays=delays, client=client):
                        delays.append(delay)
                        if len(delays) == 7:
                            client._stop.set()
                            return True
                        return False

                    try:
                        with patch.object(client._stop, "wait", side_effect=wait):
                            client.listen()
                        self.assertEqual(delays, [1, 2, 4, 8, 16, 30, 30])
                    finally:
                        client.close()

    def test_recovery_backoff_resets_after_valid_response(self) -> None:
        """A recovered protocol session restores immediate first reconnects."""
        on = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": True}}
        connections = [
            FakeSocket([TimeoutError()]),
            FakeSocket([TimeoutError()]),
            FakeSocket([TimeoutError()]),
            FakeSocket([LGSoundbarClient.encode_packet(on), TimeoutError()]),
            FakeSocket([TimeoutError()]),
        ]
        delays = []
        with patch("lg_bartalk.socket.create_connection", SocketFactory(*connections)):
            client = LGSoundbarClient("192.0.2.1")

            def wait(delay):
                delays.append(delay)
                if len(delays) == 3:
                    client._stop.set()
                    return True
                return False

            try:
                with patch.object(client._stop, "wait", side_effect=wait):
                    client.listen()
                self.assertEqual(delays, [1, 2, 1])
            finally:
                client.close()

    def test_confirmed_off_does_not_reconnect_on_idle_timeout(self) -> None:
        """Off-state idle deadlines bound sends without querying or reconnecting."""
        off = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": False}}
        connection = FakeSocket(
            [LGSoundbarClient.encode_packet(off)], timeout_when_idle=True
        )
        delivered = Event()
        with patch(
            "lg_bartalk.socket.create_connection", return_value=connection
        ) as connect:
            client = LGSoundbarClient("192.0.2.1", callback=lambda _: delivered.set())
            self.addCleanup(client.close)
            self.assertTrue(delivered.wait(1))
            self.assertTrue(connection.idle_waiting.wait(1))
            self.assertEqual(connection.gettimeout(), 30)
            self.assertEqual(connection.sent, [])
            self.assertEqual(connect.call_count, 1)
            client.close()

    def test_bad_frames_recover_and_do_not_reach_callback(self) -> None:
        """EOF, invalid lengths, JSON, padding and data shapes cannot kill recovery."""
        off = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": False}}
        invalid = [
            b"",
            b"\x10\x00\x00",
            b"\x10" + struct.pack(">I", 0),
            b"\x10" + struct.pack(">I", 17),
            b"\x10" + struct.pack(">I", 1024 * 1024 + 16),
            b"\x10" + struct.pack(">I", 16) + b"\x00" * 16,
            bytes(LGSoundbarClient.encrypt_packet("not json")),
            bytes(LGSoundbarClient.encrypt_packet("[]")),
            bytes(
                LGSoundbarClient.encrypt_packet(
                    '{"data":' + "[" * 2000 + "]" * 2000 + "}"
                )
            ),
            LGSoundbarClient.encode_packet({"msg": "SPK_LIST_VIEW_INFO", "data": 1}),
        ]
        for frame in invalid:
            with self.subTest(frame=frame[:5]):
                stale = FakeSocket([frame, b""])
                fresh = FakeSocket([LGSoundbarClient.encode_packet(off)])
                responses = []
                delivered = Event()

                def callback(response, responses=responses, delivered=delivered):
                    responses.append(response)
                    delivered.set()

                with patch(
                    "lg_bartalk.socket.create_connection", SocketFactory(stale, fresh)
                ):
                    client = LGSoundbarClient("192.0.2.1", callback=callback)
                    try:
                        self.assertTrue(delivered.wait(1))
                        self.assertEqual(responses, [off])
                        self.assertTrue(stale.closed)
                    finally:
                        client.close()

    def test_callback_exception_does_not_stop_notifications(self) -> None:
        """An application callback failure does not discard later notifications."""
        off = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": False}}
        on = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": True}}
        connection = FakeSocket(
            [LGSoundbarClient.encode_packet(off) + LGSoundbarClient.encode_packet(on)]
        )
        delivered = Event()

        def callback(response):
            if response == off:
                raise RuntimeError("application failed")
            delivered.set()

        with (
            patch("lg_bartalk.socket.create_connection", SocketFactory(connection)),
            self.assertLogs("lg_bartalk", level="ERROR"),
        ):
            client = LGSoundbarClient("192.0.2.1", callback=callback)
            try:
                self.assertTrue(delivered.wait(1))
            finally:
                client.close()

    def test_real_socket_split_frame_and_eof(self) -> None:
        """Real socket reads deliver a split frame and recover after peer closure."""
        reader, writer = socket.socketpair()
        self.addCleanup(writer.close)
        off = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": False}}
        frame = LGSoundbarClient.encode_packet(off)
        fresh = FakeSocket([frame])
        delivered = Event()
        responses = []

        def callback(response):
            responses.append(response)
            if len(responses) == 2:
                delivered.set()

        with patch("lg_bartalk.socket.create_connection", side_effect=[reader, fresh]):
            client = LGSoundbarClient("192.0.2.1", callback=callback)
            try:
                for byte in frame:
                    writer.sendall(bytes([byte]))
                writer.close()
                self.assertTrue(delivered.wait(1))
                self.assertEqual(responses, [off, off])
                self.assertEqual(len(fresh.sent), 1)
            finally:
                client.close()

    def test_partial_frame_timeout_while_off_restores_framing(self) -> None:
        """Only a timeout between frames can be ignored while off."""
        off = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": False}}
        on = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": True}}
        frame = LGSoundbarClient.encode_packet(off)
        for partial in (frame[:3], frame[:-1]):
            with self.subTest(length=len(partial)):
                stale = FakeSocket(
                    [frame, partial, TimeoutError(), LGSoundbarClient.encode_packet(on)]
                )
                fresh = FakeSocket([frame])
                responses = []
                delivered = Event()

                def callback(response, responses=responses, delivered=delivered):
                    responses.append(response)
                    if len(responses) == 2:
                        delivered.set()

                with patch(
                    "lg_bartalk.socket.create_connection", SocketFactory(stale, fresh)
                ):
                    client = LGSoundbarClient("192.0.2.1", callback=callback)
                    try:
                        self.assertTrue(delivered.wait(1))
                        self.assertEqual(responses, [off, off])
                        self.assertTrue(stale.closed)
                    finally:
                        client.close()

    def test_command_retries_once_then_reports_failure(self) -> None:
        """A command reconnects once and never silently succeeds after two failures."""
        stale = FakeSocket()
        fresh = FakeSocket()
        with patch(
            "lg_bartalk.socket.create_connection",
            side_effect=SocketFactory(stale, fresh),
        ) as connect:
            client = LGSoundbarClient("192.0.2.1")
            try:
                stale.close()
                fresh.close()
                with self.assertRaises(OSError):
                    client.set_volume(17)
                self.assertEqual(connect.call_count, 2)
            finally:
                client.close()

    def test_cleanup_aborts_reconnect_backoff(self) -> None:
        """Closing during failed reconnect attempts stops retries and the thread."""
        connection = FakeSocket()
        failed = Event()

        def connect(_address, timeout):
            failed.set()
            raise ConnectionRefusedError

        with patch("lg_bartalk.socket.create_connection", return_value=connection):
            client = LGSoundbarClient("192.0.2.1", callback=lambda _: None)
            self.addCleanup(client.close)
            self.assertTrue(connection.recv_entered.wait(1))
        with patch("lg_bartalk.socket.create_connection", side_effect=connect) as retry:
            connection.close()
            self.assertTrue(failed.wait(1))
            client.close()
            self.assertFalse(client._thread.is_alive())
            self.assertEqual(retry.call_count, 1)

    def test_failed_recovery_does_not_close_a_command_replacement(self) -> None:
        """Failure cleanup cannot close a socket installed by another caller."""
        original, failing, healthy = FakeSocket(), FakeSocket(), FakeSocket()
        failing.close()
        triggered = False
        lock = Lock()

        class InterleavingLock:
            def __enter__(self):
                lock.acquire()

            def __exit__(self, *_args):
                nonlocal triggered
                lock.release()
                if not triggered:
                    # Place a command exactly at the first recovery-lock release.
                    triggered = True
                    client.set_volume(17)
                    client._stop.set()

        with patch(
            "lg_bartalk.socket.create_connection",
            SocketFactory(original, failing, healthy),
        ):
            client = LGSoundbarClient("192.0.2.1")
            client._connection_lock = InterleavingLock()
            try:
                client._recover_connection(original)
                self.assertTrue(triggered)
                self.assertFalse(healthy.closed)
                self.assertEqual(
                    json.loads(client.decrypt_packet(healthy.sent[0][5:]))["data"],
                    {"i_vol": 17},
                )
            finally:
                client.close()

    def test_callback_can_close_the_client(self) -> None:
        """Closing from a callback must not join its own listener or deadlock."""
        connection = FakeSocket()
        closed = Event()
        responses = []

        def callback(response):
            responses.append(response)
            client.close()
            closed.set()

        with patch("lg_bartalk.socket.create_connection", return_value=connection):
            client = LGSoundbarClient("192.0.2.1", callback=callback)
            self.addCleanup(client.close)
            off = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": False}}
            with connection._lock:
                connection._reads.append(LGSoundbarClient.encode_packet(off))
            connection._wake.set()
            self.assertTrue(closed.wait(1))
            client.close()
            self.assertEqual(responses, [off])
            self.assertFalse(client._thread.is_alive())

    def test_manual_listener_is_owned_and_rejects_a_second_reader(self) -> None:
        """Legacy manual listening remains closeable without duplicate readers."""
        connection = FakeSocket()
        errors = []
        finished = Event()

        def duplicate_listener():
            try:
                client.listen()
            except RuntimeError as err:
                errors.append(err)
            finally:
                finished.set()

        with patch("lg_bartalk.socket.create_connection", return_value=connection):
            client = LGSoundbarClient("192.0.2.1")
            first = Thread(target=client.listen)
            second = Thread(target=duplicate_listener)
            try:
                first.start()
                self.assertTrue(connection.recv_entered.wait(1))
                second.start()
                self.assertTrue(finished.wait(1))
                self.assertEqual(len(errors), 1)
            finally:
                client.close()
                first.join(1)
                if second.ident is not None:
                    second.join(1)
            self.assertFalse(first.is_alive())
            self.assertFalse(second.is_alive())

    def test_concurrent_commands_keep_complete_frames(self) -> None:
        """Commands from separate callers are serialized as complete wire frames."""
        reader, writer = socket.socketpair()
        self.addCleanup(reader.close)
        with patch("lg_bartalk.socket.create_connection", return_value=writer):
            client = LGSoundbarClient("192.0.2.1")
            self.addCleanup(client.close)
            threads = [
                Thread(target=client.set_volume, args=(volume,)) for volume in range(10)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(1)
                self.assertFalse(thread.is_alive())
            reader.settimeout(1)
            volumes = [client._read_response(reader)["data"]["i_vol"] for _ in threads]
            self.assertEqual(sorted(volumes), list(range(10)))

    def test_timeout_update_failure_recovers_the_listener(self) -> None:
        """A socket closing during power handling must not kill the listener."""
        on = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": True}}
        off = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": False}}
        stale = TimeoutFailureSocket([LGSoundbarClient.encode_packet(on)])
        fresh = FakeSocket([LGSoundbarClient.encode_packet(off)])
        delivered = Event()
        with (
            patch("threading.excepthook"),
            patch("lg_bartalk.socket.create_connection", SocketFactory(stale, fresh)),
        ):
            client = LGSoundbarClient(
                "192.0.2.1", callback=lambda response: delivered.set()
            )
            self.addCleanup(client.close)
            self.assertTrue(delivered.wait(1), "listener died during timeout update")
            client.close()

    def test_silent_fresh_connection_recovers(self) -> None:
        """A fresh connection that never reports state cannot block forever."""
        off = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": False}}
        silent = FakeSocket(timeout_when_idle=True)
        fresh = FakeSocket([LGSoundbarClient.encode_packet(off)])
        delivered = Event()
        with patch("lg_bartalk.socket.create_connection", SocketFactory(silent, fresh)):
            client = LGSoundbarClient(
                "192.0.2.1", callback=lambda response: delivered.set()
            )
            self.addCleanup(client.close)
            self.assertTrue(delivered.wait(1), "fresh session never recovered")
            client.close()

    def test_fragmented_notification_is_delivered(self) -> None:
        """A TCP frame split across recv calls is still decoded once."""
        expected = {
            "cmd": "noti",
            "msg": "SPK_LIST_VIEW_INFO",
            "data": {"b_powerstatus": False},
        }
        frame = LGSoundbarClient.encode_packet(expected)
        socket = FakeSocket(
            [
                frame[:1],
                frame[1:3],
                frame[3:5],
                *(bytes([byte]) for byte in frame[5:]),
            ]
        )
        received: list[dict[str, object]] = []
        delivered = Event()

        def callback(response: dict[str, object]) -> None:
            received.append(response)
            delivered.set()

        with patch("lg_bartalk.socket.create_connection", SocketFactory(socket)):
            client = LGSoundbarClient("192.0.2.1", callback=callback)
            self.assertTrue(delivered.wait(1), "notification was not delivered")
            client.close()

        self.assertEqual(received, [expected])

    def test_connection_reset_reconnects_and_resynchronizes_power(self) -> None:
        """A reset triggers one state request and consumes the new connection."""
        expected = {
            "cmd": "noti",
            "msg": "SPK_LIST_VIEW_INFO",
            "data": {"b_powerstatus": False},
        }
        disconnected = FakeSocket([ConnectionResetError()])
        reconnected = FakeSocket([LGSoundbarClient.encode_packet(expected)])
        received: list[dict[str, object]] = []
        delivered = Event()

        def callback(response: dict[str, object]) -> None:
            received.append(response)
            delivered.set()

        factory = SocketFactory(disconnected, reconnected)
        with patch("lg_bartalk.socket.create_connection", factory):
            client = LGSoundbarClient("192.0.2.1", callback=callback)
            self.assertTrue(delivered.wait(1), "reconnected state was not delivered")
            client.close()

        self.assertEqual(received, [expected])
        self.assertEqual(
            [
                json.loads(LGSoundbarClient.decrypt_packet(packet[5:]))
                for packet in reconnected.sent
            ],
            [{"cmd": "get", "msg": "SPK_LIST_VIEW_INFO"}],
        )

    def test_idle_on_connection_reconnects_and_resynchronizes_power(self) -> None:
        """An idle on-state session is replaced before its state becomes stale."""
        on_response = {
            "cmd": "noti",
            "msg": "SPK_LIST_VIEW_INFO",
            "data": {"b_powerstatus": True},
        }
        off_response = {
            "cmd": "get",
            "msg": "SPK_LIST_VIEW_INFO",
            "data": {"b_powerstatus": False},
        }
        stale = FakeSocket(
            [LGSoundbarClient.encode_packet(on_response)], timeout_when_idle=True
        )
        reconnected = FakeSocket([LGSoundbarClient.encode_packet(off_response)])
        received: list[dict[str, object]] = []
        off_delivered = Event()

        def callback(response: dict[str, object]) -> None:
            received.append(response)
            if (response.get("data") or {}).get("b_powerstatus") is False:
                off_delivered.set()

        with patch(
            "lg_bartalk.socket.create_connection", SocketFactory(stale, reconnected)
        ):
            client = LGSoundbarClient("192.0.2.1", callback=callback)
            self.assertTrue(off_delivered.wait(1), "off state was not resynchronized")
            client.close()

        self.assertEqual(received, [on_response, off_response])
        self.assertEqual(
            [
                json.loads(LGSoundbarClient.decrypt_packet(packet[5:]))
                for packet in reconnected.sent
            ],
            [{"cmd": "get", "msg": "SPK_LIST_VIEW_INFO"}],
        )

    def test_power_off_notification_rebinds_listener_once(self) -> None:
        """A reported shutdown gets one fresh listener without off-state polling."""
        on_response = {
            "cmd": "noti",
            "msg": "SPK_LIST_VIEW_INFO",
            "data": {"b_powerstatus": True},
        }
        off_notification = {
            "cmd": "noti",
            "msg": "SPK_LIST_VIEW_INFO",
            "data": {"b_powerstatus": False},
        }
        off_resync = {
            "cmd": "get",
            "msg": "SPK_LIST_VIEW_INFO",
            "data": {"b_powerstatus": False},
        }
        metadata = {"msg": "PRODUCT_INFO", "data": {"s_model_name": "test"}}
        shutting_down = FakeSocket(
            [
                LGSoundbarClient.encode_packet(on_response),
                LGSoundbarClient.encode_packet(off_notification),
                LGSoundbarClient.encode_packet(metadata),
                LGSoundbarClient.encode_packet(off_notification),
            ],
            timeout_when_idle=True,
        )
        reconnected = FakeSocket([LGSoundbarClient.encode_packet(off_resync)])
        received: list[dict[str, object]] = []
        rebound = Event()

        def callback(response: dict[str, object]) -> None:
            received.append(response)
            if len(received) == 5:
                rebound.set()

        with patch(
            "lg_bartalk.socket.create_connection",
            SocketFactory(shutting_down, reconnected),
        ):
            client = LGSoundbarClient("192.0.2.1", callback=callback)
            self.addCleanup(client.close)
            self.assertTrue(rebound.wait(1), "listener was not rebound after shutdown")
            self.assertEqual(shutting_down.gettimeout(), 5)
            self.assertEqual(len(reconnected.sent), 1)
            client.close()

        self.assertEqual(
            received,
            [on_response, off_notification, metadata, off_notification, off_resync],
        )
        self.assertEqual(
            json.loads(LGSoundbarClient.decrypt_packet(reconnected.sent[0][5:])),
            {"cmd": "get", "msg": "SPK_LIST_VIEW_INFO"},
        )

    def test_closed_client_cannot_reopen_on_a_command(self) -> None:
        """A late command after unload cannot restart the connection."""
        connection = FakeSocket()
        with patch(
            "lg_bartalk.socket.create_connection",
            SocketFactory(connection, FakeSocket()),
        ):
            client = LGSoundbarClient("192.0.2.1")
            client.close()
            try:
                with self.assertRaises(OSError):
                    client.get_info()
            finally:
                client.close()

    def test_listener_waits_for_initial_notifications(self) -> None:
        """A new listener waits with a deadline, without an unsolicited command."""
        socket = FakeSocket()
        with patch("lg_bartalk.socket.create_connection", SocketFactory(socket)):
            client = LGSoundbarClient("192.0.2.1", callback=lambda _response: None)
            self.assertTrue(socket.recv_entered.wait(1), "listener did not start")
            self.assertEqual(socket.gettimeout(), 5)
            self.assertEqual(socket.sent, [])
            client.close()

    def test_commands_retain_the_temescal_wire_contract(self) -> None:
        """Commands use the JSON payload expected by existing soundbars."""
        socket = FakeSocket()
        with patch("lg_bartalk.socket.create_connection", SocketFactory(socket)):
            client = LGSoundbarClient("192.0.2.1")
            client.set_volume(17)
            client.close()

        self.assertEqual(
            json.loads(LGSoundbarClient.decrypt_packet(socket.sent[0][5:])),
            {
                "cmd": "set",
                "data": {"i_vol": 17},
                "msg": "SPK_LIST_VIEW_INFO",
            },
        )


if __name__ == "__main__":
    unittest.main()
