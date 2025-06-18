# Radical FaaS

A light weight agent that acts as an intermediare between a user and one of the following FaaS platfroms
    * OpenFaaS
    * Knative
    * Nuclio

## Runnig the app

Install dependencies

```bash
chmod +x scripts/*.sh
./scripts/install_docker.sh
./scripts/install_minikube.sh
```

Deploy FaaS platfrom of choice. Run with `source` so enviorment variables initialized in the script remain in current shell

```bash
source ./scripts/install_openfaas.sh
```
```bash
source ./scripts/install_knative.sh
```
```bash
source ./scripts/install_nuclio.sh
```

In the same shell you executed the script in, create a virtual enviorment, install packages, and run the app

```bash
python3 -m venv venv
source venv/bin/activate
python3 install -r requirements.txt
python3 app.py
```

### Deploy a function

Here is an example function you can deploy

```bash
curl -X POST -H "Content-Type: application/json" -d '{"code": "def handle(req):\n    return f\"hello, you sent: {req}\""}' http://127.0.0.1:5001/functions
```

You should expect an output like this

```JSON
{
  "func_id": "a1b2c3d4-e5f6-7890-g1h2-i3j4k5l6m7n8", #example function id
  "status": "deployed"
}
```

### Invoke a function

Use the function id you recived from the POST request from the deployment step

```bash
FUNC_ID="a1b2c3d4-e5f6-7890-g1h2-i3j4k5l6m7n8" #replace with actual function id

curl -X POST -d "this is my input" http://127.0.0.1:5001/functions/$FUNC_ID/invoke
```

You should expect

```
hello, you sent: this is my input
```