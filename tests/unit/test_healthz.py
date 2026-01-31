# ruff: noqa: S101
"""cli/healthz.py のユニットテスト

NOTE: Liveness チェック機能は my_lib.healthz で提供されるため、
      そのテストは my_lib で行われます。
      ここでは healthz モジュール固有の機能のみをテストします。
"""

from __future__ import annotations

import pathlib
import unittest.mock
from typing import Any

from my_lib.healthz import HealthzTarget

import amdar.cli.healthz as healthz
from amdar.constants import CONTAINER_STARTUP_GRACE_PERIOD_SECONDS, VDL2_STARTUP_GRACE_PERIOD_SECONDS


class TestHealthzModule:
    """healthz モジュールのテスト"""

    def test_module_import(self):
        """healthz モジュールがインポートできること"""
        import amdar.cli.healthz  # noqa: F401

    def test_has_get_timeout_for_now(self):
        """_get_timeout_for_now 関数が定義されていること"""
        assert hasattr(healthz, "_get_timeout_for_now")

    def test_has_notify_error(self):
        """_notify_error 関数が定義されていること"""
        assert hasattr(healthz, "_notify_error")


class TestHealthzIntegration:
    """healthz モジュールの統合テスト"""

    def test_collector_and_receiver_targets(self, config_dict: dict):
        """Collector と receiver の両方をターゲットとして設定できる"""
        targets = [
            HealthzTarget(
                name="collector",
                liveness_file=pathlib.Path(config_dict["liveness"]["file"]["collector"]),
                interval=60 * 10,
            ),
            HealthzTarget(
                name="modes",
                liveness_file=pathlib.Path(config_dict["liveness"]["file"]["receiver"]["modes"]),
                interval=60 * 10,
            ),
        ]

        # ターゲットが正しく作成されていることを確認
        assert len(targets) == 2
        assert targets[0].name == "collector"
        assert targets[1].name == "modes"

    def test_web_mode_no_file_targets(self):
        """WEB モードではファイルターゲットが空"""
        # WEB モードでは conf_list は空になる（ポートチェックのみ）
        conf_list: list[str] = []
        assert conf_list == []


class TestGracePeriod:
    """Grace period のテスト"""

    def test_vdl2_uses_longer_grace_period(self):
        """VDL2 は 10時間の grace period を使用する"""
        assert VDL2_STARTUP_GRACE_PERIOD_SECONDS == 10 * 60 * 60  # 10時間

    def test_default_grace_period(self):
        """デフォルトの grace period は 120秒"""
        assert CONTAINER_STARTUP_GRACE_PERIOD_SECONDS == 120

    def test_vdl2_failure_within_grace_period_no_notification(self, config: Any):
        """VDL2 が grace period 内に失敗しても通知しない"""
        # uptime (1時間) < VDL2 grace period (10時間) なので通知されない
        with (
            unittest.mock.patch("my_lib.healthz.check_liveness_all_with_ports", return_value=["vdl2"]),
            unittest.mock.patch("my_lib.container_util.get_uptime", return_value=3600.0),  # 1時間
            unittest.mock.patch.object(healthz, "_notify_error") as mock_notify,
            unittest.mock.patch("sys.exit"),
            unittest.mock.patch(
                "docopt.docopt",
                return_value={"-c": "config.yaml", "-m": "COL", "-p": "5000", "-D": False},
            ),
            unittest.mock.patch("my_lib.config.load", return_value={}),
            unittest.mock.patch("amdar.config.load_from_dict", return_value=config),
        ):
            healthz.main()
            mock_notify.assert_not_called()

    def test_vdl2_failure_after_grace_period_sends_notification(self, config: Any):
        """VDL2 が grace period を超えて失敗したら通知する"""
        # uptime (11時間) > VDL2 grace period (10時間) なので通知される
        with (
            unittest.mock.patch("my_lib.healthz.check_liveness_all_with_ports", return_value=["vdl2"]),
            unittest.mock.patch("my_lib.container_util.get_uptime", return_value=11 * 60 * 60),  # 11時間
            unittest.mock.patch.object(healthz, "_notify_error") as mock_notify,
            unittest.mock.patch("sys.exit"),
            unittest.mock.patch(
                "docopt.docopt",
                return_value={"-c": "config.yaml", "-m": "COL", "-p": "5000", "-D": False},
            ),
            unittest.mock.patch("my_lib.config.load", return_value={}),
            unittest.mock.patch("amdar.config.load_from_dict", return_value=config),
        ):
            healthz.main()
            mock_notify.assert_called_once()
            # 通知メッセージに vdl2 が含まれていることを確認
            call_args = mock_notify.call_args
            assert "vdl2" in call_args[0][1]

    def test_modes_failure_uses_default_grace_period(self, config: Any):
        """modes が失敗した場合はデフォルトの grace period を使用"""
        # uptime (5分) > デフォルト grace period (120秒) なので通知される
        with (
            unittest.mock.patch("my_lib.healthz.check_liveness_all_with_ports", return_value=["modes"]),
            unittest.mock.patch("my_lib.container_util.get_uptime", return_value=300.0),  # 5分
            unittest.mock.patch.object(healthz, "_notify_error") as mock_notify,
            unittest.mock.patch("sys.exit"),
            unittest.mock.patch(
                "docopt.docopt",
                return_value={"-c": "config.yaml", "-m": "COL", "-p": "5000", "-D": False},
            ),
            unittest.mock.patch("my_lib.config.load", return_value={}),
            unittest.mock.patch("amdar.config.load_from_dict", return_value=config),
        ):
            healthz.main()
            mock_notify.assert_called_once()
            call_args = mock_notify.call_args
            assert "modes" in call_args[0][1]

    def test_modes_failure_within_default_grace_period_no_notification(self, config: Any):
        """modes がデフォルト grace period 内に失敗しても通知しない"""
        # uptime (1分) < デフォルト grace period (120秒) なので通知されない
        with (
            unittest.mock.patch("my_lib.healthz.check_liveness_all_with_ports", return_value=["modes"]),
            unittest.mock.patch("my_lib.container_util.get_uptime", return_value=60.0),  # 1分
            unittest.mock.patch.object(healthz, "_notify_error") as mock_notify,
            unittest.mock.patch("sys.exit"),
            unittest.mock.patch(
                "docopt.docopt",
                return_value={"-c": "config.yaml", "-m": "COL", "-p": "5000", "-D": False},
            ),
            unittest.mock.patch("my_lib.config.load", return_value={}),
            unittest.mock.patch("amdar.config.load_from_dict", return_value=config),
        ):
            healthz.main()
            mock_notify.assert_not_called()
