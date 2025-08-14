# model.py - Mock sentiment classifier for testing
import random


class SentimentClassifier:
    """Mock sentiment classifier for testing"""

    def __init__(self, model_name, threshold):
        self.model_name = model_name
        self.threshold = threshold
        print(f"Initialized {model_name} with threshold {threshold}")

    def predict(self, text):
        """
        Mock prediction - returns sentiment and confidence
        """
        # Simple rule-based mock for testing
        text_lower = text.lower()

        # Positive words
        if any(word in text_lower for word in ['amazing', 'excellent', 'outstanding', 'best', 'love']):
            return 'positive', 0.85 + random.uniform(0, 0.14)

        # Negative words
        elif any(word in text_lower for word in ['terrible', 'awful', 'worst', 'hate', 'bad']):
            return 'negative', 0.85 + random.uniform(0, 0.14)

        # Neutral
        else:
            return 'neutral', 0.60 + random.uniform(0, 0.20)