FROM  openjdk:11-slim-buster

RUN groupadd -g 1000 valor && \
    useradd -m -s /bin/bash -u 1000 -g 1000 valor
USER valor

COPY target/valor-cli/valor-cli /opt/valor-cli

ENTRYPOINT ["/opt/valor-cli/bin/valor", "-X", "valor.plugins.dir=/opt/valor-cli/plugins" ]

