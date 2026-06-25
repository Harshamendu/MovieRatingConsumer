import logging
import logging.handlers

def setup_logging(level=logging.INFO):
    """Sets up centralized logging configuration for the entire application."""
    logger = logging.getLogger()
    logger.setLevel(level)
    
    # Prevent multiple handlers if called multiple times
    if logger.hasHandlers():
        logger.handlers.clear()

    # 1. Stream Handler (Console output)
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # 2. File Handler (Rolling file for structured logs)
    # Logs everything to a file, rotating daily
    file_handler = logging.handlers.TimedRotatingFileHandler(
        'app.log', 
        when='midnight',
        interval=1,
        backupCount=7 # Keep 7 days of logs
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    logging.info(f"Logging system initialized successfully at level: {logging.getLevelName(level)}")
    return logger