
# Docker Alpine-Linux-OS--Python Setup
<i> Docker Alpine-Linux-OS--Python Setup

## Install Docker OS
- __Installation Commands__
    - docker run -it --name python3.9-alpine python:3.9.0-alpine
    - docker exec -it <container_name_or_id> /bin/bash
    - $ docker exec -it python3.9-alpine sh
     ```bash
    / # ls
    ES     bin    dev    etc    home   lib    media  mnt    opt    proc   root   run    sbin   srv    sys    tmp    usr    var
    / #
    ```
    - mkdir ES
    - cd ES
    - mkdir test
    - cd test
    - python -m venv .venv
    - apk add --no-cache ca-certificates
    - pip install --trusted-host pypi.org --trusted-host pypi.python.org --trusted-host files.pythonhosted.org elasticsearch==7.13.0
