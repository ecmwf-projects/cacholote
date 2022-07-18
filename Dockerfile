FROM continuumio/miniconda3

WORKDIR /src/callcache

COPY environment.yml /src/callcache/

RUN conda install -c conda-forge gcc python=3.10 \
    && conda env update -n base -f environment.yml

COPY . /src/callcache

RUN pip install --no-deps -e .
