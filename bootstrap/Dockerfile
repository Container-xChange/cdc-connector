FROM dimitri/pgloader:latest

WORKDIR /app

# Create wrapper script to run pgloader with increased heap size
RUN echo '#!/bin/bash' > /usr/local/bin/pgloader-large && \
    echo 'exec /usr/bin/pgloader "$@"' >> /usr/local/bin/pgloader-large && \
    chmod +x /usr/local/bin/pgloader-large

# Set SBCL dynamic space size to 4GB (4194304 KB)
# This controls heap memory for the Lisp runtime
ENV SBCL_DYNAMIC_SPACE_SIZE=4194304

COPY pgloader/*.load /app/
COPY pgloader/load-all.sh /app/load-all.sh

RUN chmod +x /app/load-all.sh

ENTRYPOINT ["/app/load-all.sh"]
