#!/usr/bin/env python3
# ruff: noqa: S101
"""_receive_lines のテスト

リモートホストが接続を閉じた場合（recv() が b"" を返す場合）に
無限ループせず正常に終了することを検証する。
"""

from __future__ import annotations

from unittest.mock import MagicMock

import amdar.sources.modes.receiver as modes_receiver


class TestReceiveLinesConnectionClose:
    """リモート切断時の _receive_lines のテスト"""

    def test_recv_returns_empty_bytes_on_connection_close(self) -> None:
        """recv() が b"" を返した場合（リモート切断）、ジェネレータが終了すること"""
        mock_sock = MagicMock()
        # リモートが接続を閉じると recv() は b"" を返す
        mock_sock.recv.return_value = b""

        lines = list(modes_receiver._receive_lines(mock_sock))

        assert lines == []
        # recv が1回だけ呼ばれ、無限ループしないことを確認
        mock_sock.recv.assert_called_once()

    def test_recv_returns_data_then_close(self) -> None:
        """データ受信後にリモートが切断した場合、受信済みデータを返してから終了すること"""
        mock_sock = MagicMock()
        mock_sock.recv.side_effect = [
            b"*8D861F3C99458E8DE804161B720E;\n",
            b"",  # 切断
        ]

        lines = list(modes_receiver._receive_lines(mock_sock))

        assert lines == ["*8D861F3C99458E8DE804161B720E;"]
        assert mock_sock.recv.call_count == 2

    def test_recv_returns_partial_then_complete_then_close(self) -> None:
        """部分データ→完全データ→切断の場合、正しくラインを組み立てて終了すること"""
        mock_sock = MagicMock()
        mock_sock.recv.side_effect = [
            b"*8D861F3C",  # 部分データ（改行なし）
            b"99458E8DE804;\n",  # 残り＋改行
            b"",  # 切断
        ]

        lines = list(modes_receiver._receive_lines(mock_sock))

        assert lines == ["*8D861F3C99458E8DE804;"]
        assert mock_sock.recv.call_count == 3
