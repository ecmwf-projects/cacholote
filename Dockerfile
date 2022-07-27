FROM continuumio/miniconda3

WORKDIR /src/cacholote

COPY environment.yml /src/cacholote/

RUN conda install -c conda-forge gcc python=3.10 \
    && conda env update -n base -f environment.yml

COPY . /src/cacholote

RUN pip install --no-deps -e .
