import logging

def setup_logging():
    logger = logging.getLogger()
    # If the root logger already has handlers, skip reconfiguration
    if logger.hasHandlers():
        return
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # File handler writes to application.log
    file_handler = logging.FileHandler("application.log")
    file_handler.setFormatter(formatter)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)