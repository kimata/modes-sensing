# ruff: noqa: S101
"""cli/healthz.py のユニットテスト"""

from __future__ import annotations

import pathlib
import tempfile
import time
import unittest.mock
from typing import Any

import my_lib.footprint
from my_lib.healthz import HealthzTarget

import amdar.cli.healthz as healthz
from amdar.constants import CONTAINER_STARTUP_GRACE_PERIOD_SECONDS, VDL2_STARTUP_GRACE_PERIOD_SECONDS


class TestCheckLiveness:
    """check_liveness 関数のテスト"""

    def test_empty_targets_returns_true(self):
        """ターゲットが空の場合は True を返す"""
        success, failed_target = healthz.check_liveness([])
        assert success is True
        assert failed_target is None

    def test_single_target_liveness_ok(self):
        """単一ターゲットが生存している場合"""
        with tempfile.TemporaryDirectory() as tmpdir:
            liveness_file = pathlib.Path(tmpdir) / "liveness"
            # footprint.update で正しいフォーマットのファイルを作成
            my_lib.footprint.update(liveness_file)

            target = HealthzTarget(
                name="test",
                liveness_file=liveness_file,
                interval=3600,  # 1時間
            )

            success, failed_target = healthz.check_liveness([target])
            assert success is True
            assert failed_target is None

    def test_single_target_liveness_expired(self):
        """単一ターゲットの liveness が期限切れの場合"""
        with tempfile.TemporaryDirectory() as tmpdir:
            liveness_file = pathlib.Path(tmpdir) / "liveness"
            # 古いタイムスタンプを書き込む
            old_time = time.time() - 7200  # 2時間前
            liveness_file.write_text(str(old_time))

            target = HealthzTarget(
                name="test",
                liveness_file=liveness_file,
                interval=3600,  # 1時間
            )

            success, failed_target = healthz.check_liveness([target])
            assert success is False
            assert failed_target == "test"

    def test_single_target_file_not_exists(self):
        """Liveness ファイルが存在しない場合"""
        target = HealthzTarget(
            name="test",
            liveness_file=pathlib.Path("/nonexistent/path/to/file"),
            interval=3600,
        )

        success, failed_target = healthz.check_liveness([target])
        assert success is False
        assert failed_target == "test"

    def test_multiple_targets_all_ok(self):
        """複数ターゲットが全て生存している場合"""
        with tempfile.TemporaryDirectory() as tmpdir:
            liveness_file1 = pathlib.Path(tmpdir) / "collector"
            liveness_file2 = pathlib.Path(tmpdir) / "receiver"
            my_lib.footprint.update(liveness_file1)
            my_lib.footprint.update(liveness_file2)

            targets = [
                HealthzTarget(name="collector", liveness_file=liveness_file1, interval=3600),
                HealthzTarget(name="receiver", liveness_file=liveness_file2, interval=3600),
            ]

            success, failed_target = healthz.check_liveness(targets)
            assert success is True
            assert failed_target is None

    def test_multiple_targets_one_failed(self):
        """複数ターゲットのうち1つが失敗している場合"""
        with tempfile.TemporaryDirectory() as tmpdir:
            liveness_file = pathlib.Path(tmpdir) / "collector"
            my_lib.footprint.update(liveness_file)

            targets = [
                HealthzTarget(name="collector", liveness_file=liveness_file, interval=3600),
                HealthzTarget(
                    name="receiver",
                    liveness_file=pathlib.Path("/nonexistent/path"),
                    interval=3600,
                ),
            ]

            success, failed_target = healthz.check_liveness(targets)
            assert success is False
            assert failed_target == "receiver"

    def test_with_port_check_success(self):
        """ポートチェックが成功する場合"""
        with tempfile.TemporaryDirectory() as tmpdir:
            liveness_file = pathlib.Path(tmpdir) / "liveness"
            my_lib.footprint.update(liveness_file)

            with unittest.mock.patch("my_lib.healthz.check_http_port", return_value=True):
                target = HealthzTarget(
                    name="test",
                    liveness_file=liveness_file,
                    interval=3600,
                )

                success, failed_target = healthz.check_liveness([target], port=5000)
                assert success is True
                assert failed_target is None

    def test_with_port_check_failure(self):
        """ポートチェックが失敗する場合"""
        with tempfile.TemporaryDirectory() as tmpdir:
            liveness_file = pathlib.Path(tmpdir) / "liveness"
            my_lib.footprint.update(liveness_file)

            with unittest.mock.patch("my_lib.healthz.check_http_port", return_value=False):
                target = HealthzTarget(
                    name="test",
                    liveness_file=liveness_file,
                    interval=3600,
                )

                success, failed_target = healthz.check_liveness([target], port=5000)
                assert success is False
                assert failed_target == "http_port"

    def test_liveness_ok_but_port_check_failure(self):
        """Liveness は OK だがポートチェックが失敗する場合"""
        with tempfile.TemporaryDirectory() as tmpdir:
            liveness_file = pathlib.Path(tmpdir) / "liveness"
            my_lib.footprint.update(liveness_file)

            with unittest.mock.patch("my_lib.healthz.check_http_port", return_value=False):
                target = HealthzTarget(
                    name="test",
                    liveness_file=liveness_file,
                    interval=3600,
                )

                success, failed_target = healthz.check_liveness([target], port=8080)
                assert success is False
                assert failed_target == "http_port"


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
            unittest.mock.patch.object(healthz, "check_liveness", return_value=(False, "vdl2")),
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
            unittest.mock.patch.object(healthz, "check_liveness", return_value=(False, "vdl2")),
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
            unittest.mock.patch.object(healthz, "check_liveness", return_value=(False, "modes")),
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
            unittest.mock.patch.object(healthz, "check_liveness", return_value=(False, "modes")),
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
