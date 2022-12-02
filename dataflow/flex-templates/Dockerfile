# set base image
FROM gcr.io/dataflow-templates-base/python3-template-launcher-base

ARG WORKDIR=/dataflow/template
RUN mkdir -p ${WORKDIR}
WORKDIR ${WORKDIR}

# copy files
COPY requirements.txt .
COPY agg_and_write2bq.py .

# set environment variables
ENV FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE="${WORKDIR}/requirements.txt"
ENV FLEX_TEMPLATE_PYTHON_PY_FILE="${WORKDIR}/agg_and_write2bq.py"


# Install apache-beam and other dependencies to launch the pipeline
RUN apt-get update
RUN pip install --no-cache-dir --upgrade pip
RUN pip install 'apache-beam[gcp]==2.43.0'
RUN pip install -U -r ./requirements.txt

# Since we already downloaded all the dependencies, there's no need to rebuild everything.
ENV PIP_NO_DEPS=True

