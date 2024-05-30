#!/usr/bin/env bash

set -euo pipefail

ACCOUNT=$AWS_ACCOUNT
PROFILE=$AWS_PROFILE
REGION=$AWS_REGION
IMAGE=$LAMBDA_FUNCTION_IMAGE
FUNCTION=$LAMBDA_FUNCTION_NAME

aws ecr get-login-password --region "$REGION" --profile "$PROFILE" | docker login --username AWS --password-stdin "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
docker build -t "$IMAGE" .
docker tag "$IMAGE:latest" "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/$IMAGE:latest"
docker push "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/$IMAGE:latest"
aws lambda update-function-code \
--function-name "$LAMBDA_FUNCTION_NAME" \
--image-uri "$ACCOUNT.dkr.ecr.$REGION.amazonaws.com/$IMAGE:latest" \
--region "$REGION" \
--profile "$PROFILE"
echo "Lambda function $FUNCTION redeployed with new image."

