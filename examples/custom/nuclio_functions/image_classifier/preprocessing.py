import io
import requests
import numpy as np
from PIL import Image


def preprocess_image(image_data, from_url=True, target_size=(224, 224)):
    """
    preprocess image for model input
    """
    if from_url:
        # download image with proper User-Agent header
        headers = {
            'User-Agent': 'NuclioImageClassifier/1.0 (https://example.com/contact)'
        }
        response = requests.get(image_data, headers=headers, timeout=10)
        response.raise_for_status()
        image = Image.open(io.BytesIO(response.content))
    else:
        # load from bytes
        image = Image.open(io.BytesIO(image_data))

    # convert to rgb if needed
    if image.mode != 'RGB':
        image = image.convert('RGB')

    # resize
    image = image.resize(target_size)

    # convert to array and normalize
    image_array = np.array(image) / 255.0

    # add batch dimension
    image_tensor = np.expand_dims(image_array, axis=0)

    return image_tensor