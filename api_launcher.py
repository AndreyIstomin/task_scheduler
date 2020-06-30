import requests
import sys
import json
from typing import *
from copy import copy
from jsonschema import validate, ValidationError
from multiprocessing import Process
from PyQt5 import QtWidgets
from PyQt5.QtCore import QSettings
from PluginEngine import quadtree
from LandscapeEditor.backend.schemas import LON_LAT_RECT_PROPERTY, CELL_LIST_ARGS_PROPERTY
from LandscapeEditor.backend.config import SERVICE_CONFIG
from backend.task_scheduler_service.scenario_provider import ScenarioProvider
from backend.task_scheduler_service.consumers import *

def log_help():

    print("""
API launcher v1.0.0
    first: URL, example: 'http://127.0.0.1:8181
    second: USERNAME
    """)


class Rect:
    def __init__(self, lon_min: float, lon_max: float, lat_min: float, lat_max: float):
        self.lon_min = lon_min
        self.lon_max = lon_max
        self.lat_min = lat_min
        self.lat_max = lat_max

    def to_dict(self):
        return {'lon_min': self.lon_min, 'lon_max': self.lon_max,
                'lat_min': self.lat_min, 'lat_max': self.lat_max}


class Core:

    @staticmethod
    def subdivide_rect(init_rect: Dict[str, float], div_pow: int, payload_arr: List[Any]):

        if div_pow == 0:
            payload_arr[0]['rect'] = init_rect

        rect = Rect(**init_rect)
        result = []
        step_lon = 1.0 / (2 ** div_pow) * (rect.lon_max - rect.lon_min)
        step_lat = 1.0 / (2 ** div_pow) * (rect.lat_max - rect.lat_min)
        lat_min = rect.lat_min
        pos = 0
        for i in range(2 ** div_pow):
            lon_min = rect.lon_min
            for j in range(2 ** div_pow):
                payload_arr[pos]['rect'] = Rect(lon_min, lon_min + step_lon, lat_min, lat_min + step_lat).to_dict()
                lon_min += step_lon
                pos += 1
            lat_min += step_lat
        return result

    @staticmethod
    def subdivide_cells(init_cells: List[int], div_pow: int, payload_arr: List[Any]):
        if div_pow == 0:
            payload_arr[0]['cells'] = init_cells
        raise NotImplementedError('div pow > 0')

    @staticmethod
    def request(url_: str, payload: Any):
        try:
            response = requests.post(url_, json=payload, timeout=1.0)
            print(response.status_code, response.text)
        except Exception as err:
            print('Failed to send HTTP request: {}'.format(str(err)))

    @staticmethod
    def n_requests(n_req: int, div_pow: int, url_: str, payload: Any):

        assert not (div_pow > 0 and 'rect' in payload and 'cells' in payload)

        payload_arr = [copy(payload) for i in range(4**div_pow)]
        if 'rect' in payload:
            Core.subdivide_rect(payload['rect'], div_pow, payload_arr)
        if 'cells' in payload:
            Core.subdivide_cells(payload['cells'], div_pow, payload_arr)

        for i in range(n_req):
            for j in range(4**div_pow):
                p = Process(target=Core.request, args=(url_, payload_arr[j]))
                p.start()


class APILauncherApp(QtWidgets.QDialog):

    _settings_file = '.config'

    def __init__(self, url, username,
                 rect: Optional[Dict[str, float]] = None, cells: Optional[List[quadtree.QCell]] = None):
        super().__init__()
        self._url = url + SERVICE_CONFIG['task_scheduler_service']['run_task_url']
        self._username = username
        self._external_input = bool(rect or cells)
        self._rect = rect
        self._cells = cells

        h = QtWidgets.QVBoxLayout(self)

        self.buttons = []

        scenario_provider = ScenarioProvider()
        scenario_provider.load()

        def create_callback(task_id_: str):

            def callback():
                return self._request(task_id_)

            return callback

        for task_id, scenario in scenario_provider._scenarios.items():
            new_btn = QtWidgets.QPushButton(f'&{scenario.name()}')
            new_btn.clicked.connect(create_callback(str(task_id)))

            self.buttons.append(new_btn)


        self._n_req = QtWidgets.QSpinBox(self)
        self._n_req.setMinimum(1)
        self._n_req.setMaximum(10)
        self._n_division = QtWidgets.QSpinBox(self)
        self._n_division.setMinimum(0)
        self._n_division.setMaximum(2)
        self._rect_in = QtWidgets.QTextEdit(self)
        self._rect_in.setToolTip(
            'JSON, example: {"lat_min": 27.01, "lat_max": 27.015, "lon_min": 53.017, "lon_max": 53.018}')
        self._rect_in.setEnabled(not self._external_input)
        self._cells_in = QtWidgets.QTextEdit(self)
        self._cells_in.setToolTip(
            'JSON, example: [[11, false, 980, 978], [11, false, 980, 979]]')
        self._cells_in.setEnabled(not self._external_input)
        settings = QSettings(self._settings_file)
        if self._external_input:
            if self._rect:
                validate(rect, LON_LAT_RECT_PROPERTY)
                self._rect_in.setText(json.dumps(self._rect))

            if self._cells:
                self._cells_in.setText(json.dumps([[c.get_level(), c.get_south(), c.get_i(), c.get_j()] for
                                                  c in self._cells]))
        else:
            self._rect_in.setText(settings.value('cd_api_launcher/rect_input', type=str))
            self._cells_in.setText(settings.value('cd_api_launcher/cells_input', type=str))

        self._n_req.setValue(1)
        self._n_division.setValue(0)

        h.addWidget(QtWidgets.QLabel('N parallel requests'))
        h.addWidget(self._n_req)
        h.addWidget(QtWidgets.QLabel('Input subdivision power of two'))
        h.addWidget(self._n_division)
        h.addWidget(QtWidgets.QLabel('Rect'))
        h.addWidget(self._rect_in)
        h.addWidget(QtWidgets.QLabel('Cells'))
        h.addWidget(self._cells_in)

        for item in self.buttons:
            h.addWidget(item)

        self.setLayout(h)

    def closeEvent(self, QCloseEvent):
        if self._external_input:
            return
        settings = QSettings(self._settings_file)
        settings.setValue('cd_api_launcher/rect_input', self._rect_in.toPlainText())
        settings.setValue('cd_api_launcher/cells_input', self._cells_in.toPlainText())

    def _get_rect(self, text: str) -> Union[Dict[str, float], None]:
        try:
            data = json.loads(text)
            validate(data, LON_LAT_RECT_PROPERTY)
            return data
        except json.JSONDecodeError as err:
            self._show_msg(f'Invalid JSON: {err}')
            return None
        except ValidationError as err:
            self._show_msg(f'Incorrect JSON format: {err}')
            return None

    def _get_cells(self, text: str) -> Union[List[quadtree.QCell], None]:
        try:
            data = json.loads(text)
            validate(data, CELL_LIST_ARGS_PROPERTY)
            return [quadtree.make_cell(*item) for item in data]
        except json.JSONDecodeError as err:
            self._show_msg(f'Invalid JSON: {err}')
            return None
        except (ValidationError, ValueError, TypeError) as err:
            self._show_msg(f'Incorrect JSON format: {err}')
            return None

    @staticmethod
    def _show_msg(msg: str):
        msg_box = QtWidgets.QMessageBox()
        msg_box.setText(msg)
        msg_box.exec()

    def _request(self, task_id_: str):

        payload = {'username': self._username,
                   'task_id': task_id_}

        if not self._external_input:
            self._rect = None
            text = self._rect_in.toPlainText()
            if text:
                self._rect = self._get_rect(text)
                if not self._rect:
                    return

            self._cells = None
            text = self._cells_in.toPlainText()
            if text:
                self._cells = self._get_cells(text)
                if not self._cells:
                    return
        div_pow = self._n_division.value()
        if div_pow > 0 and self._cells:
            self._show_msg('Incorrect condition for subdivision power > 1')
            return

        if self._rect:
            payload['rect'] = self._rect

        if self._cells:
            payload['cells'] = [cell.get_raw_index() for cell in self._cells]

        Core.n_requests(self._n_req.value(), div_pow, self._url, payload)


def generate_cells_example():

    cells = [
        quadtree.make_cell(12, False, 2585, 1378),
        quadtree.make_cell(12, False, 2584, 1378),
        quadtree.make_cell(12, False, 2585, 1379),
        quadtree.make_cell(12, False, 2584, 1379)
    ]

    for i in range(len(cells)):
        cells.extend(quadtree.get_child_list(cells[i]))

    print('[{items}]'.format(items=', '.join(f'[{cell.get_level()}, {cell.get_south()}, {cell.get_i()}, {cell.get_j()}]'
                                             for cell in cells)))


def run_app(rect: Optional[Dict[str, float]] = None, cells: Optional[List[quadtree.QCell]] = None):

    try:

        url = sys.argv[1]
        name = sys.argv[2]
        app = QtWidgets.QApplication(sys.argv)
        dlg = APILauncherApp(url, name, rect, cells)
        dlg.setWindowTitle('API launcher')
        dlg.resize(240, 320)

        dlg.show()

        sys.exit(dlg.exec_())

    except (IndexError, ValueError):

        log_help()
        exit(1)


if __name__ == '__main__':

    run_app()




