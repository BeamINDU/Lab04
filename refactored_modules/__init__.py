from .tenant_config import TenantConfigManager, TenantConfig
from .database_handler import EnhancedDatabaseHandler, DatabaseHandler


__version__ = "2.1.0"
__author__ = "Siamtemp Development Team"
__description__ = "Enhanced multi-tenant PostgreSQL + Ollama AI Agent with clean architecture"


__all__ = [
    'TenantConfigManager',
    'TenantConfig', 
    'EnhancedDatabaseHandler',
    'DatabaseHandler',
    'EnhancedAIService', 
    'AIService',
    'EnhancedPostgresOllamaAgent'
]