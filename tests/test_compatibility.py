"""Check the public Temescal 0.5 contract against the original library."""

import inspect
import json
import logging
import unittest
from threading import Event
from unittest.mock import Mock, patch

import temescal as legacy
from test_client import FakeSocket, SocketFactory

import lg_bartalk as bartalk


class CompatibilityTest(unittest.TestCase):
    """Existing callers need only change the import."""

    def test_legacy_class_name(self) -> None:
        """The old constructor name still provides the same client."""
        self.assertIs(getattr(bartalk, "temescal", None), bartalk.LGSoundbarClient)

    def test_all_public_legacy_methods_exist(self) -> None:
        """Keep the whole public method surface, including manual reconnect/listen."""
        methods = {
            name
            for name, value in vars(legacy.temescal).items()
            if callable(value) and not name.startswith("_")
        }
        self.assertFalse(methods - set(dir(bartalk.LGSoundbarClient)))

    def test_manual_connect_replaces_and_closes_the_old_socket(self) -> None:
        """Legacy manual reconnect remains available without leaking a socket."""
        original, fresh = FakeSocket(), FakeSocket()
        with patch(
            "lg_bartalk.socket.create_connection", SocketFactory(original, fresh)
        ):
            client = bartalk.temescal("192.0.2.1")
            try:
                client.connect()
                client.get_info()
                self.assertTrue(original.closed)
                self.assertEqual(len(fresh.sent), 1)
                client.close()
                with self.assertRaises(OSError):
                    client.connect()
            finally:
                client.close()

    def test_constructor_accepts_logger(self) -> None:
        """Existing four-argument calls still connect successfully."""
        self.assertIn("logger", inspect.signature(bartalk.LGSoundbarClient).parameters)
        connection = FakeSocket()
        with patch("lg_bartalk.socket.create_connection", SocketFactory(connection)):
            client = bartalk.LGSoundbarClient("192.0.2.1", 9741, None, None)
            client.close()
        self.assertTrue(connection.closed)

    def test_public_tables_and_constants(self) -> None:
        """Old numeric input/mode constants retain their wire meanings."""
        names = ["equalisers", "functions", "stream_types", "play_control_states"]
        names.extend(name for name in vars(legacy) if name.isupper())
        for name in names:
            with self.subTest(name=name):
                self.assertEqual(getattr(bartalk, name, None), getattr(legacy, name))

    def test_custom_logger_receives_callback_errors(self) -> None:
        """The retained logger parameter is used, not merely accepted."""
        logger = Mock(spec=logging.Logger)
        logged = Event()
        logger.exception.side_effect = lambda *_: logged.set()
        off = {"msg": "SPK_LIST_VIEW_INFO", "data": {"b_powerstatus": False}}
        connection = FakeSocket([bartalk.LGSoundbarClient.encode_packet(off)])

        def callback(_response):
            raise RuntimeError("application failed")

        with patch("lg_bartalk.socket.create_connection", SocketFactory(connection)):
            client = bartalk.temescal("192.0.2.1", callback=callback, logger=logger)
            try:
                self.assertTrue(logged.wait(1))
            finally:
                client.close()

    def test_encrypt_packet_returns_a_complete_frame(self) -> None:
        """Legacy callers can still send encrypt_packet's result directly."""
        payload = json.dumps({"cmd": "get", "msg": "SPK_LIST_VIEW_INFO"})
        with patch.object(legacy.temescal, "connect"):
            old = legacy.temescal("192.0.2.1")
        expected = old.encrypt_packet(payload)
        self.assertIsInstance(
            bartalk.LGSoundbarClient.encrypt_packet(payload), bytearray
        )
        self.assertEqual(bartalk.LGSoundbarClient.encrypt_packet(payload), expected)

    def test_all_legacy_commands(self) -> None:
        """Every public command sends the same payload as Temescal 0.5."""
        commands = [
            ("get_eq", ()),
            ("set_eq", (1,)),
            ("get_info", ()),
            ("get_play", ()),
            ("get_func", ()),
            ("get_settings", ()),
            ("get_product_info", ()),
            ("get_c4a_info", ()),
            ("get_radio_info", ()),
            ("get_ap_info", ()),
            ("get_update_info", ()),
            ("get_build_info", ()),
            ("get_option_info", ()),
            ("get_mac_info", ()),
            ("get_mem_mon_info", ()),
            ("get_test_info", ()),
            ("test_tone", ()),
            ("set_night_mode", (True,)),
            ("set_avc", (True,)),
            ("set_drc", (True,)),
            ("set_neuralx", (True,)),
            ("set_av_sync", (7,)),
            ("set_woofer_level", (2,)),
            ("set_top_level", (2,)),
            ("set_center_level", (2,)),
            ("set_bt_restrict", (True,)),
            ("set_bt_standby", (True,)),
            ("set_factory", ()),
            ("set_rear_control", (True,)),
            ("set_rear_level", (2,)),
            ("set_tv_remote", (True,)),
            ("set_auto_power", (True,)),
            ("set_auto_display", (True,)),
            ("set_sleep_time", (30,)),
            ("set_func", (1,)),
            ("set_volume", (17,)),
            ("set_mute", (True,)),
            ("set_name", ("Living room",)),
        ]
        self.assertEqual(
            {name for name, _args in commands},
            {
                name
                for name, value in vars(legacy.temescal).items()
                if callable(value)
                and (name.startswith(("get_", "set_")) or name == "test_tone")
            },
        )
        with patch.object(legacy.temescal, "connect"):
            old = legacy.temescal("192.0.2.1")
        connection = FakeSocket()
        with patch("lg_bartalk.socket.create_connection", SocketFactory(connection)):
            client = bartalk.LGSoundbarClient("192.0.2.1")
            try:
                for name, args in commands:
                    with self.subTest(command=name):
                        method = getattr(client, name, None)
                        self.assertTrue(callable(method), f"Missing command: {name}")
                        expected = []
                        with patch.object(old, "send_packet", expected.append):
                            getattr(old, name)(*args)
                        method(*args)
                        actual = json.loads(
                            client.decrypt_packet(connection.sent[-1][5:])
                        )
                        self.assertEqual(actual, expected[0])
                        if args:
                            parameter = next(
                                iter(inspect.signature(getattr(old, name)).parameters)
                            )
                            method(**{parameter: args[0]})
                            actual = json.loads(
                                client.decrypt_packet(connection.sent[-1][5:])
                            )
                            self.assertEqual(actual, expected[0])
            finally:
                client.close()
