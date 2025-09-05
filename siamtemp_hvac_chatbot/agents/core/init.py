"""Core orchestration and system management."""

from .orchestrator import ImprovedDualModelDynamicAISystem

# Aliases for backward compatibility
DualModelDynamicAISystem = ImprovedDualModelDynamicAISystem
UnifiedEnhancedPostgresOllamaAgent = ImprovedDualModelDynamicAISystem
EnhancedUnifiedPostgresOllamaAgent = ImprovedDualModelDynamicAISystem

__all__ = [
    'ImprovedDualModelDynamicAISystem',
    'DualModelDynamicAISystem',
    'UnifiedEnhancedPostgresOllamaAgent',
    'EnhancedUnifiedPostgresOllamaAgent',
]