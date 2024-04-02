FROM bde2020/spark-master:3.0.1-hadoop3.2

# Set Python to Python3
ENV PYSPARK_PYTHON=python3

# Update apk repositories and install build dependencies
RUN apk update
RUN apk add --no-cache python3 py3-pip python3-dev git bash build-base libffi-dev cmake make g++ alpine-sdk cmake
RUN pip3 install --upgrade pip setuptools wheel numpy 
RUN pip3 install ipython 
RUN apk upgrade

# Download and unpack GEOS source
RUN wget http://download.osgeo.org/geos/geos-3.8.0.tar.bz2 && \
    tar xjf geos-3.8.0.tar.bz2 && \
    cd geos-3.8.0 && \
    ./configure && \
    make && \
    make install

# Install Python packages
RUN pip3 install shapely --no-binary shapely 