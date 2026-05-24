"""グラフ生成ドメイン。

このパッケージはグラフ生成のオーケストレーションを担う。
HTTP / pregeneration などの呼び出し側は :mod:`amdar.viewer.graph.service` の
``graph_service`` のみを介してグラフを生成する。matplotlib は必ず
サブプロセス内 (:mod:`amdar.viewer.graph.worker`) で実行される。
"""
