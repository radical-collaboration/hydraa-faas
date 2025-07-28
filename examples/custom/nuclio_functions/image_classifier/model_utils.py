import numpy as np

class ImageModel:
    """
    simulated image classification model
    in production, this would load a real ml model
    """
    def __init__(self, model_type, logger):
        self.model_type = model_type
        self.logger = logger
        
        # simulate model loading
        self.logger.info(f'loading {model_type} model...')
        
        # simulated class names
        self.classes = [
            'airplane', 'automobile', 'bird', 'cat', 'deer',
            'dog', 'frog', 'horse', 'ship', 'truck'
        ]
    
    def predict(self, image_tensor, top_k=5):
        """
        simulate model prediction
        """
        # simulate inference
        self.logger.info('running inference...')
        
        # generate random predictions for demo
        predictions = []
        scores = np.random.rand(len(self.classes))
        scores = scores / scores.sum()  # normalize
        
        # sort by confidence
        indices = np.argsort(scores)[::-1][:top_k]
        
        for idx in indices:
            predictions.append({
                'class': self.classes[idx],
                'confidence': float(scores[idx]),
                'index': int(idx)
            })
        
        return predictions
