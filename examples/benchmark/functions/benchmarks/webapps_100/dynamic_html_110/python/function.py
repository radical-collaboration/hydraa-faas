from datetime import datetime
from time import time, sleep
import os
import sys
import traceback

SCRIPT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__)))


def handler(event, context):
    """
    Lambda handler function that sleeps for 10 seconds and returns execution timing.
    Returns execution timing information for benchmarking.

    Args:
        event: The event data passed to the function
        context: AWS Lambda context object (contains request ID, remaining time, etc.)
    """
    # Record actual function start time
    function_start_time = time()

    try:
        print(f"[FUNCTION] Handler started at {function_start_time}")
        print(f"[FUNCTION] Event received: {event}")
        print(f"[FUNCTION] Context: {context}")
        print(f"[FUNCTION] Request ID: {context.aws_request_id if context else 'No context'}")

        # Function logic - simplified without jinja2
        name = event.get('username', 'anonymous')
        size = event.get('random_len', 100)
        cur_time = datetime.now()

        print(f"[FUNCTION] Processing request for user: {name}")
        print(f"[FUNCTION] Current time: {cur_time}")
        print(f"[FUNCTION] Requested size: {size}")

        # Simple HTML generation without templates
        html = f"""
        <html>
        <head>
            <title>Dynamic HTML Test</title>
        </head>
        <body>
            <h1>Hello {name}!</h1>
            <p>Generated at: {cur_time}</p>
            <p>This is a simple benchmark function.</p>
            <p>Random size requested: {size}</p>
            <p>Request ID: {context.aws_request_id if context else 'No context'}</p>
        </body>
        </html>
        """

        print(f"[FUNCTION] HTML generated, length: {len(html)} characters")
        print(f"[FUNCTION] Starting 10-second sleep...")

        # Sleep for 10 seconds
        sleep(10)

        print(f"[FUNCTION] Sleep completed")

        # Record actual function end time
        function_end_time = time()
        execution_time_ms = (function_end_time - function_start_time) * 1000

        print(f"[FUNCTION] Handler completed at {function_end_time}")
        print(f"[FUNCTION] Total execution time: {execution_time_ms:.2f}ms")

        return {
            'result': html,
            'execution_time_ms': execution_time_ms,
            'function_start_time': function_start_time,
            'function_end_time': function_end_time,
            'success': True,
            'html_length': len(html)
        }

    except Exception as e:
        # Record error time
        function_end_time = time()
        execution_time_ms = (function_end_time - function_start_time) * 1000

        error_details = {
            'errorMessage': str(e),
            'errorType': type(e).__name__,
            'stackTrace': traceback.format_exc().split('\n'),
            'execution_time_ms': execution_time_ms,
            'function_start_time': function_start_time,
            'function_end_time': function_end_time,
            'success': False
        }

        print(f"[FUNCTION] ERROR: {error_details}")

        # Return error details for debugging
        return error_details