FROM ghcr.io/oracle/oci-cli:latest

WORKDIR /workspace

ENTRYPOINT ["/bin/bash", "-lc"]
CMD ["oci --help"]
