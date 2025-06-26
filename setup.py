from setuptools import setup, find_packages

# read requiremnets.txt

setup_args = {}

setup_args['name']                 = "hydraa_faas"
setup_args['version']              = "1.0.0"
setup_args['scripts']              = ['src/hydraa_faas/agent/scripts/install_minikube.sh',
                                      'src/hydraa_faas/agent/scripts/install_docker.sh',
                                      'src/hydraa_faas/agent/scripts/install_openfaas.sh',
                                      'src/hydraa_faas/agent/scripts/install_nuclio.sh',
                                      'src/hydraa_faas/agent/scripts/install_knative.sh']
setup_args['packages']             = find_packages()
setup_args['package_data']         = {'': ['*.sh', '*.yaml'],}
setup_args['python_requires']      = '>=3.6'
setup_args['install_requires']     = []

setup(**setup_args)