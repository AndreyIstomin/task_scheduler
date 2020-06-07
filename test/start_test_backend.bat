path=%path%;C:\Python37;C:\Python37\Scripts
set PYTHONPATH=D:\projects\georef_generation;C:\Python37\lib\site-packages
set LANDSCAPE_EDITOR_BACKEND=true
set NOTIFICATION_SERVICE_IP=localhost
set NOTIFICATION_SERVICE_PORT=40003
set GENERATOR_SERVICE_IP=localhost
set GENERATOR_SERVICE_PORT=81
set GENERATOR_USE_THREADS=1
set DB_LISTENER_SERVICE_IP=localhost
set DB_LISTENER_SERVICE_PORT=40001
set DB_LISTENER_SERVICE_USER=exporter
set DB_LISTENER_SERVICE_PASSWORD='123456'
set SCENARIO_DB=test_config.xml
set SERVICE_AUTO_RESTART=0
set LANDSCAPE_EDITOR_LOCAL_RESOURCE_DIR=D:\v21_resource
set DB_HOST='geoserver'
set OSM_DB_HOST='geoserver'
set ROAD_GENERATOR_OUTPUT_PATH=D:\tmp\road_generator
set ELEVATION_PATH=\\gs\world\v21data3\height
set LOG_DB=D:\tmp\event_log.db
set LANDSCAPE_EDITOR_LOG_LEVEL=2
set TASK_SCHEDULER_START_TIMEOUT=3600
set TASK_SCHEDULER_CLOSE_TIMEOUT=1
set TASK_SCHEDULER_TERMINATE_TIMEOUT=60

start "Task scheduler" cmd /c "pipenv run python ..\app.py"
start "RPC server 1" cmd /c "pipenv run python ..\rpc_server.py -c timeout_error 1 crash 1 invalid_response 1"
start "RPC server 2" cmd /c "pipenv run python ..\rpc_server.py -c timeout_error 1 crash 1"
start "API launcher" cmd /c "pipenv run python ..\api_launcher.py http://127.0.0.1:8181 user1"
