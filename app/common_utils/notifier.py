from fastapi.websockets import WebSocket, WebSocketDisconnect

from common_utils.singleton import Singleton


class Notifier(metaclass=Singleton):
    def __init__(self):
        self.users: dict[int, list[WebSocket]] = {}

    def add_user(self, hierarchy_id: int, websocket: WebSocket):
        if hierarchy_id not in self.users:
            self.users[hierarchy_id] = []
        self.users[hierarchy_id].append(websocket)

    def remove_user(self, hierarchy_id: int, websocket: WebSocket):
        try:
            self.users[hierarchy_id].remove(websocket)
            if len(self.users[hierarchy_id]) == 0:
                del self.users[hierarchy_id]
        except ValueError:
            pass

    async def notify(self, hierarchy_id: int, message):
        if hierarchy_id not in self.users:
            print("this hierarchy has no listeners")
            return
        if len(message) == 0:
            return
        for user in self.users[hierarchy_id][:]:
            try:
                await user.send_json(message)
            except WebSocketDisconnect:
                self.remove_user(hierarchy_id, user)
