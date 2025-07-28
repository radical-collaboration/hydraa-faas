import json
import os
import base64
from model_utils import ImageModel
from preprocessing import preprocess_image


def classify(context, event):
    """
    nuclio handler for image classification
    """
    try:
        # get configuration from environment
        model_type = os.environ.get('MODEL_TYPE', 'mobilenet')
        confidence_threshold = float(os.environ.get('CONFIDENCE_THRESHOLD', '0.7'))

        # initialize model
        model = ImageModel(model_type, context.logger)

        # parse request - handle both dict and string formats
        body = event.body

        # Check if body is already a dict (Nuclio might pre-parse it)
        if isinstance(body, dict):
            data = body
        elif isinstance(body, (str, bytes)):
            if isinstance(body, bytes):
                body = body.decode('utf-8')
            data = json.loads(body) if body else {}
        else:
            data = {}

        # get image data
        image_url = data.get('image_url')
        image_base64 = data.get('image_base64')
        return_top_k = data.get('return_top_k', 5)

        if not image_url and not image_base64:
            # Let Nuclio handle the error response
            raise ValueError('either image_url or image_base64 must be provided')

        # preprocess image
        if image_url:
            context.logger.info(f'processing image from url: {image_url}')
            image_tensor = preprocess_image(image_url, from_url=True)
        else:
            context.logger.info('processing base64 image')
            image_bytes = base64.b64decode(image_base64)
            image_tensor = preprocess_image(image_bytes, from_url=False)

        # run inference
        predictions = model.predict(image_tensor, top_k=return_top_k)

        # filter by confidence threshold
        filtered_predictions = [
            pred for pred in predictions
            if pred['confidence'] >= confidence_threshold
        ]

        response = {
            'model': model_type,
            'threshold': confidence_threshold,
            'predictions': filtered_predictions,
            'total_predictions': len(predictions),
            'filtered_count': len(filtered_predictions)
        }

        # Return just the JSON string
        return json.dumps(response)

    except Exception as e:
        context.logger.error(f'classification error: {str(e)}')
        # Raise exception and let Nuclio handle the error response
        raise Exception(f'classification failed: {str(e)}')