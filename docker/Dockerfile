FROM --platform=linux/amd64 public.ecr.aws/emr-serverless/spark/emr-7.13.0:latest

USER root

RUN dnf install -y \
        gcc \
        gcc-c++ \
        make \
        openssl-devel \
        bzip2-devel \
        libffi-devel \
        zlib-devel \
        xz-devel \
        sqlite-devel \
        readline-devel \
        ncurses-devel \
        tk-devel \
        wget \
        tar \
        gzip \
        findutils \
    && dnf clean all

ARG PYTHON_VERSION=3.12.7
RUN wget -q https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz \
    && tar xzf Python-${PYTHON_VERSION}.tgz \
    && cd Python-${PYTHON_VERSION} \
    && ./configure --enable-optimizations --prefix=/usr/local/python3.12 \
    && make -j$(nproc) \
    && make altinstall \
    && cd .. \
    && rm -rf Python-${PYTHON_VERSION} Python-${PYTHON_VERSION}.tgz

ENV VIRTUAL_ENV=/home/hadoop/environment
RUN /usr/local/python3.12/bin/python3.12 -m venv ${VIRTUAL_ENV}

RUN ${VIRTUAL_ENV}/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
    && ${VIRTUAL_ENV}/bin/pip install --no-cache-dir \
        reverse_geocoder \
        numpy \
        scipy \
        pandas \
        pyarrow \
        venv-pack

RUN ${VIRTUAL_ENV}/bin/python -c "import reverse_geocoder as rg; rg.RGeocoder(mode=2)" || true

RUN mkdir -p /home/hadoop/dist \
    && ${VIRTUAL_ENV}/bin/venv-pack -f -o /home/hadoop/dist/pyspark_venv.tar.gz

RUN chown -R hadoop:hadoop ${VIRTUAL_ENV} /home/hadoop/dist

ENV PYSPARK_PYTHON=${VIRTUAL_ENV}/bin/python
ENV PYSPARK_DRIVER_PYTHON=${VIRTUAL_ENV}/bin/python

USER hadoop:hadoop