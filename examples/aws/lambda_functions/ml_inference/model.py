import random  # simulated model for example

class SentimentClassifier:
    """
    simulated sentiment classifier for example purposes
    in production, this would load a real ml model
    """
    def __init__(self, model_name, threshold):
        self.model_name = model_name
        self.threshold = threshold
        # simulate model loading
        self.positive_words = {'amazing', 'excellent', 'great', 'wonderful', 'fantastic'}
        self.negative_words = {'terrible', 'awful', 'bad', 'horrible', 'disappointing'}
    
    def predict(self, text):
        """
        simulate sentiment prediction
        """
        text_lower = text.lower()
        
        # simple rule based simulation
        positive_score = sum(1 for word in self.positive_words if word in text_lower)
        negative_score = sum(1 for word in self.negative_words if word in text_lower)
        
        if positive_score > negative_score:
            sentiment = 'positive'
            confidence = min(0.6 + positive_score * 0.1 + random.random() * 0.2, 0.99)
        elif negative_score > positive_score:
            sentiment = 'negative'
            confidence = min(0.6 + negative_score * 0.1 + random.random() * 0.2, 0.99)
        else:
            sentiment = 'neutral'
            confidence = 0.5 + random.random() * 0.3
        
        return sentiment, confidence

