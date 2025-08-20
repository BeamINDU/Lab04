import asyncio
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
import logging

from .prompt_manager import PromptManager
from .database_manager import DatabaseManager
from .ai_service import AIService
from .schema_analyzer import SchemaAnalyzer
from .query_validator import QueryValidator
from .response_processor import ResponseProcessor

logger = logging.getLogger(__name__)

class AgentOrchestrator:
    """🎭 Main orchestration logic for multi-tenant system"""
    
    def __init__(self, tenant_configs: Dict[str, Any]):
        self.tenant_configs = tenant_configs
        
        # Initialize core components
        self.prompt_manager = PromptManager(tenant_configs)
        self.database_manager = DatabaseManager(tenant_configs)
        self.ai_service = AIService()
        self.schema_analyzer = SchemaAnalyzer()
        self.query_validator = QueryValidator()
        self.response_processor = ResponseProcessor()
        
        # System statistics
        self.system_stats = {
            'total_queries': 0,
            'successful_queries': 0,
            'failed_queries': 0,
            'avg_response_time': 0,
            'company_breakdown': {}
        }
        
        logger.info("🎭 AgentOrchestrator initialized with modular architecture")
    
    async def process_query(self, question: str, tenant_id: str) -> Dict[str, Any]:
        """🎯 Main entry point for processing queries"""
        
        start_time = datetime.now()
        
        try:
            # 1. Validate tenant
            if tenant_id not in self.tenant_configs:
                raise ValueError(f"Unknown tenant: {tenant_id}")
            
            # 2. Get schema information
            schema_info = await self.schema_analyzer.get_schema_info(tenant_id)
            
            # 3. Generate company-specific SQL prompt
            sql_prompt = self.prompt_manager.generate_sql_prompt(
                tenant_id, question, schema_info
            )
            
            # 4. Call AI service to generate SQL
            sql_query = await self.ai_service.generate_sql(
                sql_prompt, tenant_id, self.tenant_configs[tenant_id]
            )
            
            # 5. Validate generated SQL
            validation_result = self.query_validator.validate_sql(sql_query, tenant_id)
            if not validation_result['is_valid']:
                raise ValueError(f"Invalid SQL: {validation_result['error']}")
            
            # 6. Execute SQL query
            results = await self.database_manager.execute_query(sql_query, tenant_id)
            
            # 7. Format company-specific response
            formatted_response = self.prompt_manager.format_response(
                tenant_id, question, results, {
                    'sql_query': sql_query,
                    'execution_time': (datetime.now() - start_time).total_seconds(),
                    'results_count': len(results)
                }
            )
            
            # 8. Update statistics
            processing_time = (datetime.now() - start_time).total_seconds()
            self._update_statistics(tenant_id, True, processing_time)
            
            return {
                'success': True,
                'answer': formatted_response,
                'sql_query': sql_query,
                'results_count': len(results),
                'processing_time': processing_time,
                'tenant_id': tenant_id,
                'system_version': 'modular_v1.0',
                'prompt_type': self.prompt_manager.get_prompt(tenant_id).__class__.__name__
            }
            
        except Exception as e:
            # Handle errors gracefully
            processing_time = (datetime.now() - start_time).total_seconds()
            self._update_statistics(tenant_id, False, processing_time)
            
            logger.error(f"❌ Query processing failed for {tenant_id}: {e}")
            
            return {
                'success': False,
                'answer': f"เกิดข้อผิดพลาด: {str(e)}",
                'error': str(e),
                'processing_time': processing_time,
                'tenant_id': tenant_id,
                'system_version': 'modular_v1.0_error'
            }
    
    def _update_statistics(self, tenant_id: str, success: bool, processing_time: float):
        """📊 Update system statistics"""
        
        self.system_stats['total_queries'] += 1
        
        if success:
            self.system_stats['successful_queries'] += 1
        else:
            self.system_stats['failed_queries'] += 1
        
        # Update average response time
        total_time = (self.system_stats['avg_response_time'] * 
                     (self.system_stats['total_queries'] - 1) + processing_time)
        self.system_stats['avg_response_time'] = total_time / self.system_stats['total_queries']
        
        # Update company breakdown
        if tenant_id not in self.system_stats['company_breakdown']:
            self.system_stats['company_breakdown'][tenant_id] = {
                'queries': 0, 'successes': 0, 'failures': 0
            }
        
        company_stats = self.system_stats['company_breakdown'][tenant_id]
        company_stats['queries'] += 1
        
        if success:
            company_stats['successes'] += 1
        else:
            company_stats['failures'] += 1
    
    def get_system_statistics(self) -> Dict[str, Any]:
        """📊 Get comprehensive system statistics"""
        
        success_rate = 0
        if self.system_stats['total_queries'] > 0:
            success_rate = (self.system_stats['successful_queries'] / 
                          self.system_stats['total_queries']) * 100
        
        # Get prompt manager statistics
        prompt_stats = self.prompt_manager.get_all_statistics()
        
        return {
            'system_overview': {
                'total_queries': self.system_stats['total_queries'],
                'success_rate': round(success_rate, 2),
                'avg_response_time': round(self.system_stats['avg_response_time'], 3),
                'active_companies': len(prompt_stats['active_prompts'])
            },
            'company_breakdown': self.system_stats['company_breakdown'],
            'prompt_statistics': prompt_stats,
            'system_health': 'healthy' if success_rate > 80 else 'needs_attention',
            'last_updated': datetime.now().isoformat()
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """🩺 System health check"""
        
        health_status = {
            'overall_status': 'healthy',
            'components': {},
            'issues': []
        }
        
        # Check prompt manager
        try:
            prompt_stats = self.prompt_manager.get_all_statistics()
            health_status['components']['prompt_manager'] = {
                'status': 'healthy',
                'companies_loaded': prompt_stats['total_companies']
            }
        except Exception as e:
            health_status['components']['prompt_manager'] = {'status': 'error', 'error': str(e)}
            health_status['issues'].append(f"Prompt Manager: {str(e)}")
        
        # Check database manager
        try:
            db_status = await self.database_manager.health_check()
            health_status['components']['database_manager'] = db_status
            
            if not db_status.get('all_databases_connected', False):
                health_status['issues'].append("Some databases not connected")
        except Exception as e:
            health_status['components']['database_manager'] = {'status': 'error', 'error': str(e)}
            health_status['issues'].append(f"Database Manager: {str(e)}")
        
        # Check AI service
        try:
            ai_status = await self.ai_service.health_check()
            health_status['components']['ai_service'] = ai_status
            
            if ai_status.get('status') != 'healthy':
                health_status['issues'].append("AI Service not responding")
        except Exception as e:
            health_status['components']['ai_service'] = {'status': 'error', 'error': str(e)}
            health_status['issues'].append(f"AI Service: {str(e)}")
        
        # Overall status determination
        if len(health_status['issues']) > 0:
            health_status['overall_status'] = 'degraded' if len(health_status['issues']) < 3 else 'unhealthy'
        
        return health_status