"""服务入口：uvicorn main:app。"""

from app.factory import create_app

app = create_app()
