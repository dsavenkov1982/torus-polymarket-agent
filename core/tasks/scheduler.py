# core/tasks/scheduler.py
from celery import Celery
from celery.schedules import crontab
from loguru import logger
from settings import settings
import sys

# Configure logging
logger.remove()
logger.add(
    "../logs/core.log",
    rotation="500 MB",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)
logger.add(
    sys.stdout,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
    level="DEBUG"
)

# Load indexing interval from settings
INDEXER_INTERVAL_MINUTES = settings.INDEXER_INTERVAL_MINUTES

# Ensure interval is valid
if INDEXER_INTERVAL_MINUTES <= 0 or INDEXER_INTERVAL_MINUTES > 60:
    logger.error("Invalid INDEXER_INTERVAL_MINUTES. Must be between 1 and 60.")
    sys.exit(1)

# Celery application instance
scheduler_app = Celery(
    'core',
    broker=settings.REDIS_URL
)

# Import tasks to register them with Celery
from core.tasks.blockchain_indexer import run_polymarket_indexer, enrich_market_metadata, database_maintenance

# Define the beat schedule
scheduler_app.conf.beat_schedule = {
    # Main blockchain indexing task - runs every few minutes
    'index-polymarket-blockchain': {
        'task': 'blockchain_indexer.run_polymarket_indexer',
        'schedule': crontab(minute=f'*/{INDEXER_INTERVAL_MINUTES}'),
    },

    # Metadata enrichment - runs every hour
    'enrich-market-metadata': {
        'task': 'blockchain_indexer.enrich_market_metadata',
        'schedule': crontab(minute=30),  # Every hour at :30
    },

    # Database maintenance - runs daily at 2 AM
    'database-maintenance': {
        'task': 'blockchain_indexer.database_maintenance',
        'schedule': crontab(hour=2, minute=0),  # Daily at 2:00 AM
    },
}

# Celery configuration
scheduler_app.conf.timezone = 'UTC'
scheduler_app.conf.broker_connection_retry_on_startup = True
scheduler_app.conf.worker_proc_alive_timeout = 300  # 5 minutes for blockchain calls
scheduler_app.conf.task_soft_time_limit = 600  # 10 minutes soft limit
scheduler_app.conf.task_time_limit = 900  # 15 minutes hard limit
scheduler_app.conf.worker_prefetch_multiplier = 1  # Process one task at a time
scheduler_app.conf.task_acks_late = True
scheduler_app.conf.worker_disable_rate_limits = True

# Task routing (optional - for scaling)
scheduler_app.conf.task_routes = {
    'blockchain_indexer.run_polymarket_indexer': {'queue': 'indexer'},
    'blockchain_indexer.enrich_market_metadata': {'queue': 'metadata'},
    'blockchain_indexer.database_maintenance': {'queue': 'maintenance'},
}

# Trigger immediate execution if the environment variable is set
if settings.TRIGGER_IMMEDIATE:
    logger.info("Triggering immediate execution of blockchain indexer task.")
    try:
        scheduler_app.send_task('blockchain_indexer.run_polymarket_indexer')
        logger.info("Immediate indexer task triggered successfully")
    except Exception as e:
        logger.error(f"Failed to trigger immediate task: {e}")

# Allow manual execution outside Docker
if __name__ == '__main__':
    logger.info("Starting Celery scheduler for Polymarket indexer")
    logger.info(f"Indexing interval: every {INDEXER_INTERVAL_MINUTES} minutes")
    logger.info(f"Redis broker: {settings.REDIS_URL}")

    # Start Celery with beat scheduler
    scheduler_app.start(argv=[
        'worker',
        '-B',  # Enable beat scheduler
        '--loglevel=info',
        '--concurrency=2',  # Low concurrency for blockchain indexing
        '--queues=indexer,metadata,maintenance'
    ])