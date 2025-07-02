# Hydraa Faas Manager Examples

Quick examples for deploying lambda functions with the hydraa faas manager.

## Notebooks

- **`basic_lambda_deployment.ipynb`** - deploy 10 source-based lambda functions
- **`ecr_lambda_deployment.ipynb`** - deploy 5 containerized lambda functions

## Setup

1. Create function directories:
   ```bash
   # for basic example
   cd lambda_functions
   python create_project.py
   
   # for container example  
   cd container_functions
   python create_container_project.py
   ```

2. set aws credentials in `.env`:
   ```
   cat > .env_private <<EOF
   ACCESS_KEY_ID=your_key
   ACCESS_KEY_SECRET=your_secret
   AWS_REGION=us-east-1
   >EOF
   
   export $(cat .env_private | xargs)
   ```

3. Run notebooks
