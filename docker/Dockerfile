# EMR Serverless 7.12 base image (Amazon Linux 2023)
FROM --platform=linux/amd64 public.ecr.aws/emr-serverless/spark/emr-7.12.0:latest

USER root

# Cài đặt build dependencies cho Python 3.12
# AL2023 dùng dnf (không phải yum). Một số package đổi tên hoặc đã có sẵn.
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

# Build Python 3.12 từ source
ARG PYTHON_VERSION=3.12.7
RUN wget -q https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz \
    && tar xzf Python-${PYTHON_VERSION}.tgz \
    && cd Python-${PYTHON_VERSION} \
    && ./configure --enable-optimizations --prefix=/usr/local/python3.12 \
    && make -j$(nproc) \
    && make altinstall \
    && cd .. \
    && rm -rf Python-${PYTHON_VERSION} Python-${PYTHON_VERSION}.tgz

# Tạo virtualenv với Python 3.12 ở vị trí chuẩn của EMR Serverless
ENV VIRTUAL_ENV=/home/hadoop/environment
RUN /usr/local/python3.12/bin/python3.12 -m venv ${VIRTUAL_ENV}

# Cài đặt các thư viện Python cần thiết
RUN ${VIRTUAL_ENV}/bin/pip install --no-cache-dir --upgrade pip setuptools wheel \
    && ${VIRTUAL_ENV}/bin/pip install --no-cache-dir \
        reverse_geocoder \
        numpy \
        scipy \
        pandas \
        venv-pack

# (Tùy chọn) Pre-cache dataset của reverse_geocoder để runtime không phải tải lại
RUN ${VIRTUAL_ENV}/bin/python -c "import reverse_geocoder as rg; rg.RGeocoder(mode=2)" || true

# Đóng gói virtualenv thành tar.gz để Spark phân phối tới executors
RUN mkdir -p /home/hadoop/dist \
    && ${VIRTUAL_ENV}/bin/venv-pack -f -o /home/hadoop/dist/pyspark_venv.tar.gz

# Cấp quyền cho user hadoop
RUN chown -R hadoop:hadoop ${VIRTUAL_ENV} /home/hadoop/dist

# Cấu hình biến môi trường để Spark sử dụng Python 3.12
ENV PYSPARK_PYTHON=${VIRTUAL_ENV}/bin/python
ENV PYSPARK_DRIVER_PYTHON=${VIRTUAL_ENV}/bin/python

# Switch về user hadoop (yêu cầu của EMR Serverless)
USER hadoop:hadoop