FROM public.ecr.aws/lambda/python@sha256:24e6c828bc3bbbeef49ca71d3afd6710f1b5436570b5ee4771cbb98357f90015
COPY requirements.txt  .
RUN pip install -r requirements.txt --target ${LAMBDA_TASK_ROOT}
COPY crawler.py ${LAMBDA_TASK_ROOT}
CMD [ "crawler.handler" ]
