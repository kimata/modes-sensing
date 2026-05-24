"""グラフ種別定義の集約。

GRAPH_DEF_MAP は HTTP 層・キャッシュ層・worker 層から参照されるため、
プロット関数群を一箇所に束ねる役割を持つ。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import PIL.Image

from amdar.constants import GraphName
from amdar.viewer.graph.plotting.contour import plot_contour_2d, plot_contour_3d
from amdar.viewer.graph.plotting.density import plot_density, plot_heatmap
from amdar.viewer.graph.plotting.scatter import plot_scatter_2d, plot_scatter_3d
from amdar.viewer.graph.plotting.temperature import plot_temperature
from amdar.viewer.graph.plotting.wind import plot_wind_direction


@dataclass(frozen=True)
class GraphDefinition:
    """グラフ定義。"""

    func: Callable[..., tuple[PIL.Image.Image, float]]
    size: tuple[int, int]
    file: str


GRAPH_DEF_MAP: dict[GraphName, GraphDefinition] = {
    "scatter_2d": GraphDefinition(func=plot_scatter_2d, size=(2400, 1600), file="scatter_2d.png"),
    "scatter_3d": GraphDefinition(func=plot_scatter_3d, size=(2800, 2800), file="scatter_3d.png"),
    "contour_2d": GraphDefinition(func=plot_contour_2d, size=(2400, 1600), file="contour_2d.png"),
    "contour_3d": GraphDefinition(func=plot_contour_3d, size=(2800, 2800), file="contour_3d.png"),
    "density": GraphDefinition(func=plot_density, size=(2400, 1600), file="density.png"),
    "heatmap": GraphDefinition(func=plot_heatmap, size=(2400, 1600), file="heatmap.png"),
    "temperature": GraphDefinition(func=plot_temperature, size=(2400, 1600), file="temperature.png"),
    "wind_direction": GraphDefinition(func=plot_wind_direction, size=(2400, 1600), file="wind_direction.png"),
}
