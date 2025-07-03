# Hydraa Faas Manager Examples

Quick examples for deploying lambda functions with the hydraa faas manager.

## Notebooks

- `agent_examples.ipynb`: demonstrates deploying functions to the agent provider, both from inline code and from local source directories.
- `basic_lambda_deployment.ipynb`: demonstrates deploying multiple source-based (zip) functions to aws lambda.
- `ecr_lambda_deployment.ipynb`: demonstrates deploying container-based functions to aws lambda using ecr.

## Setup

1. Create function directories:
   ```bash
   # for basic example
   cd lambda_functions
   python create_project.py
   
   # for container example  
   cd container_functions
   python create_container_project.py
   
   # for agent example
   cd agent_functions
   python create_agent_functions.py
   ```

2. Install the project and its dependencies. from the root directory, run:
    ```bash
    pip install -e .
    ```
3. Set aws credentials in `.env`:
   ```
   cat > .env_private <<EOF
   ACCESS_KEY_ID=your_key
   ACCESS_KEY_SECRET=your_secret
   AWS_REGION=us-east-1
   >EOF
   
   export $(cat .env_private | xargs)
   ```

4. Run the jupyter notebooks to see the examples in action.

