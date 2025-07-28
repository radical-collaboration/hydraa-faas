"""
Nuclio Gateway Function Code
API Gateway that routes requests to Nuclio functions
"""

GATEWAY_CODE = '''
import json
import urllib.request
import urllib.error

def handler(context, event):
    """API Gateway that routes to Nuclio functions"""

    path = event.path.strip('/')
    if not path:
        return {"error": "No function specified", "usage": "/api/gateway/<function-name>"}

    function_name = path.split('/')[0]
    target_url = f"http://nuclio-{function_name}.nuclio.svc.cluster.local:8080"

    try:
        headers = {}
        if hasattr(event, 'headers') and event.headers:
            for key, value in event.headers.items():
                if key.lower() in ['content-type', 'authorization']:
                    headers[key] = value

        body = None
        if event.method in ["POST", "PUT", "PATCH"]:
            if hasattr(event, 'body') and event.body is not None:
                if isinstance(event.body, dict):
                    body = json.dumps(event.body).encode('utf-8')
                    if 'content-type' not in [k.lower() for k in headers.keys()]:
                        headers['Content-Type'] = 'application/json'
                elif isinstance(event.body, bytes):
                    body = event.body
                elif isinstance(event.body, str):
                    body = event.body.encode('utf-8')

        req = urllib.request.Request(target_url, data=body, headers=headers, method=event.method)
        response = urllib.request.urlopen(req, timeout=30)
        response_data = response.read()

        try:
            return json.loads(response_data.decode('utf-8'))
        except:
            return response_data.decode('utf-8')

    except Exception as e:
        return {"error": "Gateway error", "details": str(e), "function": function_name}
'''