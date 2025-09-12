# agents/__init__.py
"""Siamtemp AI Chatbot Agents Package."""

from .core.orchestrator import ImprovedDualModelDynamicAISystem
from .nlp.intent_detector import ImprovedIntentDetector
from .nlp.prompt_manager import PromptManager
from .sql.validator import SQLValidator
from .data.cleaner import DataCleaningEngine
from .storage.database import SimplifiedDatabaseHandler
from .storage.memory import ConversationMemory
from .clients.ollama import SimplifiedOllamaClient

# Backward compatibility aliases
DualModelDynamicAISystem = ImprovedDualModelDynamicAISystem
UnifiedEnhancedPostgresOllamaAgent = ImprovedDualModelDynamicAISystem
EnhancedUnifiedPostgresOllamaAgent = ImprovedDualModelDynamicAISystem

__all__ = [
    # New names
    'ImprovedDualModelDynamicAISystem',
    'ImprovedIntentDetector',
    'PromptManager',
    'SQLValidator',
    'DataCleaningEngine',
    'SimplifiedDatabaseHandler',
    'ConversationMemory',
    'SimplifiedOllamaClient',
    
    # Old names for compatibility
    'DualModelDynamicAISystem',
    'UnifiedEnhancedPostgresOllamaAgent',
    'EnhancedUnifiedPostgresOllamaAgent',
]