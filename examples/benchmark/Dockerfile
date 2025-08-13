FROM public.ecr.aws/lambda/python:3.9
COPY requirements.txt .
RUN pip install --upgrade pip -r requirements.txt
COPY . /var/task/
CMD ["100.webapps.110.dynamic-html.python.function.handler"]
