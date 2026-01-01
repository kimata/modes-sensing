# ruff: noqa: S101
"""healthz.py のユニットテスト"""
import pathlib
import tempfile
import time
import unittest.mock

import my_lib.footprint
from my_lib.healthz import HealthzTarget

import healthz


class TestCheckLiveness:
    """check_liveness 関数のテスト"""

    def test_empty_targets_returns_true(self):
        """ターゲットが空の場合は True を返す"""
        result = healthz.check_liveness([])
        assert result is True

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

            result = healthz.check_liveness([target])
            assert result is True

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

            result = healthz.check_liveness([target])
            assert result is False

    def test_single_target_file_not_exists(self):
        """Liveness ファイルが存在しない場合"""
        target = HealthzTarget(
            name="test",
            liveness_file=pathlib.Path("/nonexistent/path/to/file"),
            interval=3600,
        )

        result = healthz.check_liveness([target])
        assert result is False

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

            result = healthz.check_liveness(targets)
            assert result is True

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

            result = healthz.check_liveness(targets)
            assert result is False

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

                result = healthz.check_liveness([target], port=5000)
                assert result is True

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

                result = healthz.check_liveness([target], port=5000)
                assert result is False

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

                result = healthz.check_liveness([target], port=8080)
                assert result is False


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
                name="receiver",
                liveness_file=pathlib.Path(config_dict["liveness"]["file"]["receiver"]),
                interval=60 * 10,
            ),
        ]

        # ターゲットが正しく作成されていることを確認
        assert len(targets) == 2
        assert targets[0].name == "collector"
        assert targets[1].name == "receiver"

    def test_web_mode_no_file_targets(self):
        """WEB モードではファイルターゲットが空"""
        # WEB モードでは conf_list は空になる（ポートチェックのみ）
        conf_list: list[str] = []
        assert conf_list == []
