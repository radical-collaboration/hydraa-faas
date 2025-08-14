import json
import os
from model import SentimentClassifier
from utils import preprocess_text, format_response


def lambda_handler(event, context):
    """
    ml inference lambda function for sentiment analysis
    """
    try:
        # initialize model
        model_name = os.environ.get('MODEL_NAME', 'sentiment_classifier')
        threshold = float(os.environ.get('THRESHOLD', '0.8'))

        classifier = SentimentClassifier(model_name, threshold)

        # extract textss from event
        texts = event.get('texts', [])
        if not texts:
            return format_response(400, {'error': 'no texts provided'})

        # preprocess and classify
        results = []
        for text in texts:
            processed_text = preprocess_text(text)
            sentiment, confidence = classifier.predict(processed_text)

            results.append({
                'text': text[:100] + '...' if len(text) > 100 else text,
                'sentiment': sentiment,
                'confidence': round(confidence, 3),
                'positive': sentiment == 'positive' and confidence > threshold
            })

        return format_response(200, {
            'model': model_name,
            'threshold': threshold,
            'results': results,
            'processed': len(results)
        })

    except Exception as e:
        return format_response(500, {'error': str(e)})
