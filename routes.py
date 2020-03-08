from backend.task_scheduler_service.task_viewer.views import ChatList, WebSocket


routes = [
    # API routes

    # Task viewer routes
    ('GET', '/',        ChatList,  'main'),
    ('GET', '/ws',      WebSocket, 'chat'),
]