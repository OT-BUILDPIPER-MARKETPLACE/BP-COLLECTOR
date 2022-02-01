FROM python:slim-buster AS builder

WORKDIR /opt/
RUN apt-get update
RUN apt-get install -y binutils libc-bin
COPY . .
RUN pip3 install --no-cache --upgrade -r requirements.txt &&  pyinstaller scripts/backup.py --onefile

FROM python:slim-buster AS deployer
WORKDIR /opt/
RUN mkdir -p config
COPY --from=builder /opt/dist/backup .
ENTRYPOINT ["./backup"]


