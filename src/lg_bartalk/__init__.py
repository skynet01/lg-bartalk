"""Local client for the LG soundbar protocol.

The wire-format constants and command payloads originate from python-temescal,
Copyright 2018 Google LLC, licensed under the Apache License, Version 2.0.
This implementation replaces its archived connection and framing code.
"""

import json
import logging
import socket
import struct
from collections.abc import Callable
from contextlib import suppress
from threading import Event, Lock, Thread, current_thread
from typing import Any

from Crypto.Cipher import AES

_LOGGER = logging.getLogger(__name__)

_IV = b"'%^Ur7gy$~t+f)%@"
_KEY = b"T^&*J%^7tr~4^%^&I(o%^!jIJ__+a0 k"
_MARKER = 0x10
_MAX_PAYLOAD_SIZE = 1024 * 1024
_CONNECT_TIMEOUT = 10
_ON_IDLE_TIMEOUT = 30
_OFF_SETTLE_TIMEOUT = 5
_RECONNECT_DELAY = 1
_MAX_RECONNECT_DELAY = 30

equalisers = [
    "Standard",
    "Bass",
    "Flat",
    "Boost",
    "Treble and Bass",
    "User",
    "Music",
    "Cinema",
    "Night",
    "News",
    "Voice",
    "ia_sound",
    "Adaptive Sound Control",
    "Movie",
    "Bass Blast",
    "Dolby Atmos",
    "DTS Virtual X",
    "Bass Boost Plus",
    "DTS X",
    "AI Sound Pro",
    "Clear Voice",
    "Sports",
    "Game",
]

functions = [
    "Wi-Fi",
    "Bluetooth",
    "Portable",
    "Aux",
    "Optical",
    "CP",
    "HDMI",
    "ARC",
    "Spotify",
    "Optical2",
    "HDMI2",
    "HDMI3",
    "LG TV",
    "Mic",
    "Chromecast",
    "Optical/HDMI ARC",
    "LG Optical",
    "FM",
    "USB",
    "USB2",
    "E-ARC",
]


STANDARD = 0
BASS = 1
FLAT = 2
BOOST = 3
TREBLE_BASS = 4
USER_EQ = 5
MUSIC = 6
CINEMA = 7
NIGHT = 8
NEWS = 9
VOICE = 10
IA_SOUND = 11
ASC = 12
MOVIE = 13
BASS_BLAST = 14
DOLBY_ATMOS = 15
DTS_VIRTUAL_X = 16
BASS_BOOST_PLUS = 17
DTS_X = 18
AI_SOUND_PRO = 19
CLEAR_VOICE = 20
SPORTS = 21
GAME = 22
WIFI = 0
BLUETOOTH = 1
PORTABLE = 2
AUX = 3
OPTICAL = 4
CP = 5
HDMI = 6
ARC = 7
SPOTIFY = 8
OPTICAL_2 = 9
HDMI_2 = 10
HDMI_3 = 11
LG_TV = 12
MIC = 13
C4A = 14
OPTICAL_HDMIARC = 15
LG_OPTICAL = 16
FM = 17
USB = 18
USB_2 = 19
E_ARC = 20
stream_types = ["Unknown0", "Google Cast", "Unknown2", "Unknown3", "Spotify Connect"]
GOOGLE_CAST = 1
SPOTIFY_CONNECT = 4
play_control_states = ["Playing", "Stopped/Paused"]
PLAYING = 0
PAUSED = 1


class LGSoundbarClient:
    """Communicate with an LG soundbar."""

    def __init__(
        self,
        address: str,
        port: int = 9741,
        callback: Callable[[dict[str, Any]], None] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        """Initialize and connect the client."""
        self._address = address
        self._port = port
        self._callback = callback
        self.logger = logger if logger is not None else _LOGGER
        self._connection_lock = Lock()
        self._stop = Event()
        self._reconnect_delay = 0
        self._power_on: bool | None = None
        self._socket: socket.socket | None = None
        self._thread: Thread | None = None

        self.connect()

        if callback is not None:
            self._thread = Thread(
                target=self.listen,
                name=f"lg-soundbar-{address}",
                daemon=True,
            )
            self._thread.start()

    def connect(self) -> None:
        """Connect or replace the current connection, as in Temescal."""
        with self._connection_lock:
            if self._stop.is_set():
                raise OSError("LG soundbar client is closed")
            self._close_socket(self._socket)
            self._socket = None
            self._power_on = None
            self._connect_locked()

    @classmethod
    def encode_packet(cls, data: dict[str, Any]) -> bytes:
        """Encode a protocol packet."""
        return bytes(cls.encrypt_packet(json.dumps(data)))

    @classmethod
    def encrypt_packet(cls, data: str) -> bytearray:
        """Return a complete wire frame, matching Temescal's public method."""
        encrypted = cls._encrypt_payload(data)
        return bytearray([_MARKER]) + struct.pack(">I", len(encrypted)) + encrypted

    @classmethod
    def _encrypt_payload(cls, data: str) -> bytes:
        """Encrypt a JSON payload using the soundbar's fixed protocol key."""
        plaintext = data.encode()
        padding_length = AES.block_size - len(plaintext) % AES.block_size
        plaintext += bytes([padding_length]) * padding_length
        return AES.new(_KEY, AES.MODE_CBC, _IV).encrypt(plaintext)

    @classmethod
    def decrypt_packet(cls, data: bytes) -> str:
        """Decrypt an encrypted payload."""
        if not data or len(data) % AES.block_size:
            raise ValueError("Invalid encrypted LG soundbar payload length")
        plaintext = AES.new(_KEY, AES.MODE_CBC, _IV).decrypt(data)
        padding_length = plaintext[-1]
        if (
            padding_length < 1
            or padding_length > AES.block_size
            or plaintext[-padding_length:] != bytes([padding_length]) * padding_length
        ):
            raise ValueError("Invalid LG soundbar payload padding")
        return plaintext[:-padding_length].decode()

    def _connect_locked(self) -> None:
        """Open a connection with a deadline for its first state response."""
        connection = socket.create_connection(
            (self._address, self._port), timeout=_CONNECT_TIMEOUT
        )
        try:
            connection.settimeout(_OFF_SETTLE_TIMEOUT)
        except OSError:
            self._close_socket(connection)
            raise
        self._socket = connection

    @staticmethod
    def _close_socket(connection: socket.socket | None) -> None:
        """Close a socket without masking the original failure."""
        if connection is None:
            return
        with suppress(OSError):
            connection.shutdown(socket.SHUT_RDWR)
        with suppress(OSError):
            connection.close()

    @staticmethod
    def _recv_exact(connection: socket.socket, size: int) -> bytes:
        """Read exactly size bytes or report a closed connection."""
        chunks = bytearray()
        while len(chunks) < size:
            chunk = connection.recv(size - len(chunks))
            if not chunk:
                raise EOFError("LG soundbar closed the connection")
            chunks.extend(chunk)
        return bytes(chunks)

    def _read_response(self, connection: socket.socket) -> dict[str, Any]:
        """Read and decode one complete response frame."""
        while self._recv_exact(connection, 1)[0] != _MARKER:
            pass
        try:
            length = struct.unpack(">I", self._recv_exact(connection, 4))[0]
            if length == 0 or length > _MAX_PAYLOAD_SIZE or length % AES.block_size:
                raise ValueError(f"Invalid LG soundbar payload length: {length}")
            payload = self._recv_exact(connection, length)
        except TimeoutError as err:
            # A timeout midway through a frame loses framing; it is not idle.
            raise ValueError("Incomplete LG soundbar frame") from err
        response = json.loads(self.decrypt_packet(payload))
        if not isinstance(response, dict):
            raise TypeError("LG soundbar response is not an object")
        return response

    def listen(self) -> None:
        """Deliver notifications and recover the connection after disconnects."""
        with self._connection_lock:
            if self._stop.is_set():
                return
            if (
                self._thread is not None
                and self._thread is not current_thread()
                and self._thread.is_alive()
            ):
                raise RuntimeError("LG soundbar listener is already running")
            self._thread = current_thread()
        while not self._stop.is_set():
            connection = self._socket
            if connection is None:
                self._recover_connection(None)
                continue
            try:
                response = self._read_response(connection)
                with self._connection_lock:
                    if self._stop.is_set() or self._socket is not connection:
                        continue
                    data = response.get("data") or {}
                    if not isinstance(data, dict):
                        raise TypeError("LG soundbar response data is not an object")
                    if (
                        response.get("msg") == "SPK_LIST_VIEW_INFO"
                        and "b_powerstatus" in data
                    ):
                        power_on = bool(data["b_powerstatus"])
                        # Rebind once after shutdown, then wait for notifications.
                        # A finite socket deadline also bounds synchronous sends.
                        timeout = _ON_IDLE_TIMEOUT
                        if not power_on and self._power_on:
                            timeout = _OFF_SETTLE_TIMEOUT
                        if power_on or self._power_on is not False:
                            connection.settimeout(timeout)
                        self._power_on = power_on
            except TimeoutError:
                with self._connection_lock:
                    if (
                        self._socket is connection
                        and self._power_on is False
                        and connection.gettimeout() == _ON_IDLE_TIMEOUT
                    ):
                        # An off-state read deadline is not a state poll.
                        continue
                if not self._stop.is_set():
                    self._recover_connection(connection)
                continue
            except (EOFError, OSError):
                if not self._stop.is_set():
                    self._recover_connection(connection)
                continue
            except (RecursionError, TypeError, UnicodeError, ValueError):
                self.logger.warning(
                    "Invalid LG soundbar response; reconnecting to restore framing"
                )
                self._recover_connection(connection)
                continue

            self._reconnect_delay = 0

            try:
                if self._callback is not None:
                    self._callback(response)
            except (
                Exception
            ):  # Home Assistant callback errors must not kill the listener
                self.logger.exception("Error handling an LG soundbar response")

    def _recover_connection(self, failed_connection: socket.socket | None) -> None:
        """Reconnect after a socket failure and request one state resync."""
        if self._reconnect_delay and self._stop.wait(self._reconnect_delay):
            return
        while not self._stop.is_set():
            with self._connection_lock:
                if self._stop.is_set():
                    return
                try:
                    if self._socket is failed_connection:
                        self._close_socket(self._socket)
                        self._socket = None
                        self._power_on = None
                    if self._socket is None:
                        self._connect_locked()
                    assert self._socket is not None
                    self._socket.sendall(
                        self.encode_packet({"cmd": "get", "msg": "SPK_LIST_VIEW_INFO"})
                    )
                except OSError:
                    self.logger.debug(
                        "LG soundbar reconnect failed; retrying", exc_info=True
                    )
                    self._close_socket(self._socket)
                    self._socket = None
                    self._power_on = None
                else:
                    self._reconnect_delay = _RECONNECT_DELAY
                    return
            delay = self._reconnect_delay or _RECONNECT_DELAY
            self._reconnect_delay = min(delay * 2, _MAX_RECONNECT_DELAY)
            self._stop.wait(delay)

    def send_packet(self, data: dict[str, Any]) -> None:
        """Send one command, reconnecting once if the current socket failed."""
        packet = self.encode_packet(data)
        with self._connection_lock:
            if self._stop.is_set():
                raise OSError("LG soundbar client is closed")
            if self._socket is None:
                self._connect_locked()
            assert self._socket is not None
            try:
                self._socket.sendall(packet)
            except OSError:
                self._close_socket(self._socket)
                self._socket = None
                self._power_on = None
                self._connect_locked()
                assert self._socket is not None
                self._socket.sendall(packet)

    def get_eq(self) -> None:
        """Request equalizer state."""
        self.send_packet({"cmd": "get", "msg": "EQ_VIEW_INFO"})

    def set_eq(self, eq: int) -> None:
        """Set the equalizer."""
        self.send_packet(
            {"cmd": "set", "data": {"i_curr_eq": eq}, "msg": "EQ_VIEW_INFO"}
        )

    def get_info(self) -> None:
        """Request speaker state, including power."""
        self.send_packet({"cmd": "get", "msg": "SPK_LIST_VIEW_INFO"})

    def get_play(self) -> None:
        """Request playback state."""
        self.send_packet({"cmd": "get", "msg": "PLAY_INFO"})

    def get_func(self) -> None:
        """Request input state."""
        self.send_packet({"cmd": "get", "msg": "FUNC_VIEW_INFO"})

    def set_func(self, value: int) -> None:
        """Set the input."""
        self.send_packet(
            {"cmd": "set", "data": {"i_curr_func": value}, "msg": "FUNC_VIEW_INFO"}
        )

    def get_settings(self) -> None:
        """Request soundbar settings."""
        self.send_packet({"cmd": "get", "msg": "SETTING_VIEW_INFO"})

    def get_product_info(self) -> None:
        """Request product information."""
        self.send_packet({"cmd": "get", "msg": "PRODUCT_INFO"})

    def get_mac_info(self) -> None:
        """Request MAC and UUID information."""
        self.send_packet({"cmd": "get", "msg": "MAC_INFO_DEV"})

    def set_volume(self, value: int) -> None:
        """Set the speaker volume."""
        self.send_packet(
            {"cmd": "set", "data": {"i_vol": value}, "msg": "SPK_LIST_VIEW_INFO"}
        )

    def set_mute(self, enable: bool) -> None:
        """Set mute state."""
        self.send_packet(
            {"cmd": "set", "data": {"b_mute": enable}, "msg": "SPK_LIST_VIEW_INFO"}
        )

    def get_c4a_info(self) -> None:
        """Request c4a info."""
        data = {"cmd": "get", "msg": "C4A_SETTING_INFO"}
        self.send_packet(data)

    def get_radio_info(self) -> None:
        """Request radio info."""
        data = {"cmd": "get", "msg": "RADIO_VIEW_INFO"}
        self.send_packet(data)

    def get_ap_info(self) -> None:
        """Request ap info."""
        data = {"cmd": "get", "msg": "SHARE_AP_INFO"}
        self.send_packet(data)

    def get_update_info(self) -> None:
        """Request update info."""
        data = {"cmd": "get", "msg": "UPDATE_VIEW_INFO"}
        self.send_packet(data)

    def get_build_info(self) -> None:
        """Request build info."""
        data = {"cmd": "get", "msg": "BUILD_INFO_DEV"}
        self.send_packet(data)

    def get_option_info(self) -> None:
        """Request option info."""
        data = {"cmd": "get", "msg": "OPTION_INFO_DEV"}
        self.send_packet(data)

    def get_mem_mon_info(self) -> None:
        """Request mem mon info."""
        data = {"cmd": "get", "msg": "MEM_MON_DEV"}
        self.send_packet(data)

    def get_test_info(self) -> None:
        """Request test info."""
        data = {"cmd": "get", "msg": "TEST_DEV"}
        self.send_packet(data)

    def test_tone(self) -> None:
        """Set test tone."""
        data = {"cmd": "set", "msg": "TEST_TONE_REQ"}
        self.send_packet(data)

    def set_night_mode(self, enable: bool) -> None:
        """Set night mode."""
        data = {
            "cmd": "set",
            "data": {"b_night_mode": enable},
            "msg": "SETTING_VIEW_INFO",
        }
        self.send_packet(data)

    def set_avc(self, enable: bool) -> None:
        """Set avc."""
        data = {
            "cmd": "set",
            "data": {"b_auto_vol": enable},
            "msg": "SETTING_VIEW_INFO",
        }
        self.send_packet(data)

    def set_drc(self, enable: bool) -> None:
        """Set drc."""
        data = {"cmd": "set", "data": {"b_drc": enable}, "msg": "SETTING_VIEW_INFO"}
        self.send_packet(data)

    def set_neuralx(self, enable: bool) -> None:
        """Set neuralx."""
        data = {"cmd": "set", "data": {"b_neuralx": enable}, "msg": "SETTING_VIEW_INFO"}
        self.send_packet(data)

    def set_av_sync(self, value: int) -> None:
        """Set av sync."""
        data = {"cmd": "set", "data": {"i_av_sync": value}, "msg": "SETTING_VIEW_INFO"}
        self.send_packet(data)

    def set_woofer_level(self, value: int) -> None:
        """Set woofer level."""
        data = {
            "cmd": "set",
            "data": {"i_woofer_level": value},
            "msg": "SETTING_VIEW_INFO",
        }
        self.send_packet(data)

    def set_top_level(self, value: int) -> None:
        """Set top speaker level."""
        self.send_packet(
            {"cmd": "set", "data": {"i_top_level": value}, "msg": "SETTING_VIEW_INFO"}
        )

    def set_center_level(self, value: int) -> None:
        """Set center speaker level."""
        self.send_packet(
            {
                "cmd": "set",
                "data": {"i_center_level": value},
                "msg": "SETTING_VIEW_INFO",
            }
        )

    def set_bt_restrict(self, enable: bool) -> None:
        """Limit Bluetooth connections."""
        self.send_packet(
            {
                "cmd": "set",
                "data": {"b_conn_bt_limit": enable},
                "msg": "SETTING_VIEW_INFO",
            }
        )

    def set_bt_standby(self, enable: bool) -> None:
        """Set Bluetooth standby."""
        self.send_packet(
            {"cmd": "set", "data": {"b_bt_standby": enable}, "msg": "SETTING_VIEW_INFO"}
        )

    def set_factory(self) -> None:
        """Request a factory reset."""
        self.send_packet({"cmd": "set", "msg": "FACTORY_SET_REQ"})

    def set_rear_control(self, enable: bool) -> None:
        """Set rear control."""
        data = {"cmd": "set", "data": {"b_rear": enable}, "msg": "SETTING_VIEW_INFO"}
        self.send_packet(data)

    def set_rear_level(self, value: int) -> None:
        """Set rear level."""
        data = {
            "cmd": "set",
            "data": {"i_rear_level": value},
            "msg": "SETTING_VIEW_INFO",
        }
        self.send_packet(data)

    def set_tv_remote(self, enable: bool) -> None:
        """Set tv remote."""
        data = {
            "cmd": "set",
            "data": {"b_tv_remote": enable},
            "msg": "SETTING_VIEW_INFO",
        }
        self.send_packet(data)

    def set_auto_power(self, enable: bool) -> None:
        """Set auto power."""
        data = {
            "cmd": "set",
            "data": {"b_auto_power": enable},
            "msg": "SETTING_VIEW_INFO",
        }
        self.send_packet(data)

    def set_auto_display(self, enable: bool) -> None:
        """Set auto display."""
        data = {
            "cmd": "set",
            "data": {"b_auto_display": enable},
            "msg": "SETTING_VIEW_INFO",
        }
        self.send_packet(data)

    def set_sleep_time(self, value: int) -> None:
        """Set sleep time."""
        data = {
            "cmd": "set",
            "data": {"i_sleep_time": value},
            "msg": "SETTING_VIEW_INFO",
        }
        self.send_packet(data)

    def set_name(self, name: str) -> None:
        """Set name."""
        data = {"cmd": "set", "data": {"s_user_name": name}, "msg": "SETTING_VIEW_INFO"}
        self.send_packet(data)

    def close(self) -> None:
        """Stop the listener and close the connection."""
        self._stop.set()
        with self._connection_lock:
            self._close_socket(self._socket)
            self._socket = None
        if (
            self._thread is not None
            and self._thread is not current_thread()
            and self._thread.is_alive()
        ):
            self._thread.join(timeout=2)


temescal = LGSoundbarClient
