import json
import logging
from mox.mox_extractor import lambda_mox


# Setup logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    function_name = event.get("function")  # Get the function name from the event
    logger.info(f"Invoking function: {function_name}")

    handlers = {
        "lambda_mox": lambda_mox

    }
    print(function_name)
    handler = handlers.get("lambda_mox")

    print(event)
    if handler:
        return handler(event, context)
    else:
        error_message = f"Invalid function name specified: {function_name}"
        logger.error(error_message)
        raise ValueError(error_message)
