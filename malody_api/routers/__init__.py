# malody_api/routers/__init__.py (更新)
from .players import router as players_router
from .charts import router as charts_router
from .analytics import router as analytics_router
from .system import router as system_router
from .query import router as query_router  # 新增

__all__ = [
    "players_router", 
    "charts_router", 
    "analytics_router", 
    "system_router",
    "query_router"  # 新增
]