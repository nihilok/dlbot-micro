FROM public.ecr.aws/lambda/python:3.11

# Install any dependencies specified in requirements.txt
COPY requirements.txt .
RUN pip install -r requirements.txt
# Download and install ffmpeg static build
RUN yum install -y wget tar xz
RUN wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-arm64-static.tar.xz && \
    tar -xJf ffmpeg-release-arm64-static.tar.xz && \
    mv ffmpeg-*-static/ffmpeg /usr/local/bin/ && \
    mv ffmpeg-*-static/ffprobe /usr/local/bin/ && \
    rm -rf ffmpeg-*-static ffmpeg-release-arm64-static.tar.xz

ENV PATH=/usr/local/bin:$PATH
ENV PYTHONPATH=${LAMBDA_TASK_ROOT}:$PYTHONPATH

COPY lib.py ${LAMBDA_TASK_ROOT}
COPY app.py ${LAMBDA_TASK_ROOT}

CMD ["app.lambda_handler"]
