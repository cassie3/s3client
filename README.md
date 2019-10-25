# s3_client
s3 cli tools base on boto3
```
[root@test2 dist]# ./s3_client --help
usage: s3_client [-h] -u USER -p PASSWORD [-b BUCKET] [-k KEY]
                 [-f SOURCE_FILE] [-l LOCAL_FILE] -s SERVER -c OPS
                 [--PartNumber PARTNUMBER] [--UploadId UPLOADID]
                 [--Parts PARTS] [--Concurrency CONCUR]
                 [--use_threads USE_THREADS] [--sbucket SBUCKET] [--skey SKEY]
                 [--tbucket TBUCKET] [--tkey TKEY] [--userkey USER_KEY]
                 [--suserkey SOURCE_USER_KEY] [--expires-in EXPIRESIN]
                 [--range RANGE] [--Prefix PREFIX] [--Marker MARKER]
                 [--MaxKeys MAXKEYS] [--sign_v SIGN_V]

Process some integers.

optional arguments:
  -h, --help            show this help message and exit
  -u USER               s3 user
  -p PASSWORD           password for special s3 user
  -b BUCKET             bucket name
  -k KEY                key name
  -f SOURCE_FILE        source file path or content
  -l LOCAL_FILE         local file path
  -s SERVER             The address of server
  -c OPS                The operation which you need to requestPutObject put
                        an object into bucketUploadObject put an object and
                        enable mpu defaultCreateBucket Create BucketListBucket
                        List all bucket below current userDeleteBucket Delete
                        bucketListObject List all objects in current
                        bucketDeleteObject Delet object in bucketMultipart-
                        init init multipart uploadpresign generated presign
                        urlpresign-post generated presign url for post
  --PartNumber PARTNUMBER
                        The address of server
  --UploadId UPLOADID   Multipart upload ID
  --Parts PARTS         Multipart upload parts
  --Concurrency CONCUR  Special upload concurrency threads
  --use_threads USE_THREADS
                        whether used threads when upload file, value is True
                        or False
  --sbucket SBUCKET     the source bucket for copyobject operation
  --skey SKEY           the source key for copyobject operation
  --tbucket TBUCKET     the target bucket for copyobject operation
  --tkey TKEY           the target file for copyobject operation
  --userkey USER_KEY    User-defined encrypted fields
  --suserkey SOURCE_USER_KEY
                        User-defined encrypted fields, this parameter will
                        used by copy operation
  --expires-in EXPIRESIN
                        Number of seconds until the pre-signed URL
                        expires.Default is 3600 seconds
  --range RANGE         Number of seconds until the pre-signed URL
                        expires.Default is 3600 seconds
  --Prefix PREFIX       Limits the response to keys that begin with the
                        specified prefix
  --Marker MARKER       Specifies the key to start with when listing objects
                        in a bucket
  --MaxKeys MAXKEYS     Sets the maximum number of keys returned in the
                        response
  --sign_v SIGN_V       S3 signature version
```
