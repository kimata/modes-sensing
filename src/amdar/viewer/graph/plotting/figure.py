"""matplotlib Figure/Axes 生成と画像変換のヘルパ。"""

from __future__ import annotations

import io

import matplotlib.pyplot
import my_lib.pil_util
import PIL.Image

import amdar.config
from amdar.constants import GRAPH_IMAGE_DPI
from amdar.viewer.graph.plotting.styles import ERROR_SIZE, to_panel_font_config


def create_figure(figsize=(12, 8)):
    """余白を最適化した 2D Figure/Axes を返す。"""
    fig, ax = matplotlib.pyplot.subplots(figsize=figsize)
    fig.subplots_adjust(
        left=0.08,
        bottom=0.08,
        right=0.94,  # カラーバーの目盛テキスト用スペース
        top=0.90,
    )
    return fig, ax


def create_3d_figure(figsize=(12, 8)):
    """余白を最適化した 3D Figure/Axes を返す。"""
    fig = matplotlib.pyplot.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection="3d")
    fig.subplots_adjust(
        left=0.02,
        bottom=0.05,
        right=0.94,
        top=0.91,
    )
    return fig, ax


def setup_3d_layout(ax) -> None:
    """3D プロットの視点とプロット位置を調整する。"""
    ax.view_init(elev=25, azim=35)
    ax.set_position([0.02, 0.05, 0.86, 0.88])


def convert_figure_to_image(fig) -> PIL.Image.Image:
    """Figure を PIL.Image に変換し、Figure はクローズする。"""
    buf = io.BytesIO()
    # pyplot.savefig はカレント figure に依存するため、対象 figure の savefig を直接使う
    fig.savefig(buf, format="png", dpi=GRAPH_IMAGE_DPI, facecolor="white", transparent=False)

    buf.seek(0)
    img = PIL.Image.open(buf).copy()
    buf.close()

    # メモリ解放: Figure を即座にクローズ
    matplotlib.pyplot.clf()
    matplotlib.pyplot.close(fig)
    matplotlib.pyplot.close("all")

    return img


def create_no_data_image(
    config: amdar.config.Config,
    size: tuple[int, int],
    text: str = "データがありません",
) -> PIL.Image.Image:
    """データがない場合の画像を生成する。"""
    img = PIL.Image.new("RGB", size, color="white")

    # フォントサイズを DPI に合わせて調整
    font_size = int(ERROR_SIZE * GRAPH_IMAGE_DPI / 72)

    font_config = to_panel_font_config(config.font)
    font = my_lib.pil_util.get_font(font_config, "jp_bold", font_size)

    pos = (size[0] // 2, size[1] // 2)

    my_lib.pil_util.draw_text(img, text, pos, font, align="center", color="#666")

    return img
