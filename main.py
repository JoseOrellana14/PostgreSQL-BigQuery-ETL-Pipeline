import logging
from app import create_app
from app.etl import run_etl
from flask import request as flask_request
from logging.config import setup_logging

# Configure global logging
setup_logging()

logger = logging.getLogger(__name__)

# Create the Flask application
app = create_app()

def invoke_etl():
    """Invoke the ETL process and handle responses."""
    try:
        # Run the ETL process
        run_etl()
        return "ETL process completed successfully", 200
    except Exception:
        logger.exception("Error during ETL process")
        return "ETL process failed", 500
    
# For running locally with Flask
@app.route('/', methods=['POST', 'GET'])
def main_function_local():
    return invoke_etl()

# For running on Google Cloud Functions
def main_function(request):
    return invoke_etl()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=True)