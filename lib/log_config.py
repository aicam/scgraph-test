import logging

def setup_logging(log_file: str = "app.log"):
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,  # Set the logging level
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file),  # Write logs to a file
            logging.StreamHandler()  # Also print logs to the console
        ]
    )
