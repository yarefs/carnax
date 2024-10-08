#!/bin/sh
awslocal s3api delete-objects \
    --bucket carnax-test-bucket \
    --delete "$(awslocal s3api list-object-versions \
    --bucket carnax-test-bucket | \
    jq '{Objects: [.Versions[] | {Key:.Key, VersionId : .VersionId}], Quiet: false}')"
