FROM debian:bookworm-slim AS build

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Makefile .
COPY inc ./inc
COPY src ./src

RUN make clean && make


FROM debian:bookworm-slim

WORKDIR /app

RUN mkdir -p logs

COPY --from=build /app/httpserver ./httpserver

CMD ["./httpserver"]
