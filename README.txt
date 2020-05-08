Настройка RabbitMQ Windows 10

1. Добавить в PATH C:\Program Files\RabbitMQ Server\rabbitmq_server-<x>.<y>.<z>\sbin
2. Скопировать C:\Windows\System32\config\systemprofile\.erland.cookie -> %userprofile%
3. Запустить cmd
4. Выполнить rabbitmq-plugins enable rabbitmq_management
5. Выполнить rabbitmqctl stop_app
6. Выполнить rabbitmqctl reset
7. Выполнить rabbitmqctl start_app

Сброс RabbitMQ сервера

1. Запустить cmd
2. Выполнить rabbitmqctl stop_app
3. Выполнить rabbitmqctl reset
4. Выполнить rabbitmqctl start_app

Добавление администратора с именем user1 и паролем 123456:

1. Запустить cmd
2. Выполнить rabbitmqctl add_user user1 123456
3. Выполнить rabbitmqctl set_user_tags user1 administrator
4. Выполнить rabbitmqctl set_permissions -p / user1 ".*" ".*" ".*"

Включение консоли RabbitMQ:

1. Перейти в браузере на http://127.0.0.1:15672


