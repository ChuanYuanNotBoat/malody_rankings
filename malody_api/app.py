# malody_api/app.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi

# 修复导入路径 - 改为绝对导入
from malody_api.routers.players import router as players_router
from malody_api.routers.charts import router as charts_router
from malody_api.routers.analytics import router as analytics_router
from malody_api.routers.system import router as system_router
from malody_api.routers.query import router as query_router

# 创建FastAPI应用
app = FastAPI(
    title="Malody数据API",
    description="""
    Malody玩家和谱面数据查询API - 提供完整的排行榜、统计和分析功能
    
    ## 新增功能
    
    - **高级查询**: 支持灵活的数据库查询，包含完整的过滤、排序、分组功能
    - **安全查询**: 内置SQL注入防护，确保查询安全
    - **数据库统计**: 提供数据库结构和统计信息
    - **预定义查询**: 提供常用查询示例
    
    ## 安全说明
    
    所有查询都经过严格的安全验证：
    - 表名和列名白名单验证
    - SQL注入防护
    - 查询结果数量限制
    - 参数化查询执行
    """,
    version="1.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# CORS配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(players_router)
app.include_router(charts_router)
app.include_router(analytics_router)
app.include_router(system_router)
app.include_router(query_router)

# 全局异常处理
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    from datetime import datetime
    return JSONResponse(
        status_code=500,
        content={
            "success": False,
            "error": f"服务器内部错误: {str(exc)}",
            "timestamp": datetime.now().isoformat()
        }
    )

# 根路由
@app.get("/", include_in_schema=False)
async def root():
    return {
        "message": "Malody数据API服务运行中",
        "version": "1.1.0",
        "documentation": "/docs",
        "endpoints": {
            "players": "/players/",
            "charts": "/charts/", 
            "analytics": "/analytics/",
            "system": "/system/",
            "query": "/query/"
        }
    }

# 健康检查
@app.get("/health", include_in_schema=False)
async def health():
    return {"status": "healthy"}

# 自定义OpenAPI文档
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    
    openapi_schema = get_openapi(
        title="Malody数据API",
        version="1.1.0",
        description="""
        ## Malody数据API
        
        提供完整的Malody游戏数据查询和分析功能：
        
        ### 功能特性
        
        - **玩家数据**: 查询玩家排名、详细信息、历史记录
        - **谱面数据**: 获取谱面统计、热门谱面、创作者信息  
        - **数据分析**: 趋势分析、模式比较、数据统计
        - **高级查询**: 灵活的自定义查询功能
        - **灵活筛选**: 支持模式、难度、时间范围、状态等多种筛选条件
        
        ### 数据来源
        
        数据来自Malody游戏服务器，通过爬虫定期更新。
        
        ### 使用说明
        
        所有API均返回统一格式的JSON响应：
        ```json
        {
            "success": true,
            "data": {...},
            "message": "操作成功",
            "error": null,
            "timestamp": "2024-01-01T00:00:00Z"
        }
        ```
        """,
        routes=app.routes,
    )
    
    # 添加服务器信息
    openapi_schema["servers"] = [
        {
            "url": "http://localhost:8000",
            "description": "开发服务器"
        },
        {
            "url": "https://api.yourdomain.com",
            "description": "生产服务器" 
        }
    ]
    
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0", 
        port=8000,
        reload=True,
        log_level="info"
    )