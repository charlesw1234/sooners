from fastapi import WebSocket
from pydantic import BaseModel as BaseSchema
from ..endpoint import EPVersion, BaseEndpoint

class UserSchema(BaseSchema):
    name: str
    team: str
class TeamSchema(BaseSchema):
    name: str

class Tester(BaseEndpoint):
    path = 'core_tester'
    async def post(self, user: UserSchema) -> TeamSchema:
        return TeamSchema(name = user.team)
    async def websocket(self, websocket: WebSocket):
        await websocket.accept()
        while True:
            request = await websocket.receive_text()
            await websocket.send_text(request.upper())
