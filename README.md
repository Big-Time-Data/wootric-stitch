# Deploying

This needs to be packaged in the proper environmnent, especially if running from a Mac.

The `.env` needs to be present with the proper variables.

```bash

docker run -v /wootric-stitch:/wootric-stitch -it --rm nikolaik/python-nodejs:python3.8-nodejs12 bash

...


npm i -g serverless

cd /wootric-stitch

export AWS_ACCESS_KEY_ID=xxx # Admin credentials
export AWS_SECRET_ACCESS_KEY=xxx # Admin credentials

serverless deploy

```