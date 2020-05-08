from typing import *
from dataclasses import asdict
from PluginEngine import quadtree
from backend.task_scheduler_service.api_launcher import run_app, Rect


def prepare_rect_1() -> Dict[str, float]:
    """
    See test_area_1.png
    """

    cell = quadtree.make_cell(11, False, 1291, 689)
    bl0 = quadtree.GeoVector()
    bl1 = quadtree.GeoVector()

    cell.get_bound_box(bl0, bl1)

    return asdict(Rect(bl0.lon, bl1.lon, bl0.lat, bl1.lat))


if __name__ == '__main__':
    run_app(rect=prepare_rect_1())