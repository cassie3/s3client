# !/usr/bin/python
# coding=utf-8
"""
For S3 common function
@auth:       cassie
@date:       7/23/2018
"""
import boto3
from boto3 import Session
from botocore.client import Config
from botocore import exceptions as boto3exception
import boto3.exceptions as s3Exception
import os
from boto3.s3.transfer import TransferConfig
import base64
import hashlib
import json
import click
import datetime
import collections

SELF_VERSION = "0.1"


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime('%Y-%m-%d')
        return json.JSONEncoder.default(self, obj)


class s3_client(object):
    def __init__(self, **kwargs):
        '''

        :param
        resource: value True or None. will create s3 resource type
        access_key: string, the access key info of s3 user
        security_key: string, the password of special user
        endpoint_url: special s3 server
        :return:
        return s3 client
        '''

        param = kwargs.keys()
        if "access_key" in param:
            self.access_key = kwargs['access_key']
        if "security_key" in param:
            self.security_key = kwargs['security_key']
        if "endpoint_url" in param:
            self.endpoint_url = kwargs["endpoint_url"]
        if "region_name" in param:
            self.region_name = kwargs['region_name']
        else:
            self.region_name = "us-east-1"
        if "sign_v" in param:
            self.sign_v = kwargs["sign_v"]
        else:
            self.sign_v = "s3"

        self.client_config = Config(s3={"addressing_style": "path"},
                                    signature_version=self.sign_v,
                                    max_pool_connections=5,
                                    retries=dict(max_attempts=0),
                                    read_timeout=700)
        self.transfer_download_config = TransferConfig(
            max_concurrency=5,
            num_download_attempts=1,
            use_threads=True,
            multipart_chunksize=104857600,
        )
        self.transfer_config = TransferConfig(max_concurrency=5,
                                              num_download_attempts=5,
                                              use_threads=True, )
        self.s3 = boto3.client('s3', endpoint_url=self.endpoint_url,
                               aws_access_key_id=self.access_key,
                               aws_secret_access_key=self.security_key,
                               region_name=self.region_name,
                               config=self.client_config)

    def getallbucket(self):
        '''
            return respon of list all buckets
            :return:
            dictionary object
        '''
        try:
            response = self.s3.list_buckets()
            return 0, response
        except Exception, e:
            print("Get bucket failed: %s" % e.message)
            return 1, e.message

    def buckets(self):
        '''
            List all bucket name in a list
            :return:
            a list buckets
        '''
        response = self.getallbucket()
        if response[0] is 0:
            buckets = [bucket[u'Name'] for bucket in response[1][u'Buckets']]
            return 0, buckets
        else:
            return 1, response[1]

    def list_bucket(self):
        # print all buckets in current user
        response = self.getallbucket()
        buckets = {}
        dictmp = {}
        if response is not 1:
            for bucket in response[1][u'Buckets']:
                dictmp = {}
                name = bucket[u'Name']
                dictmp["creationdate"] = bucket[u'CreationDate']
                buckets[name] = dictmp
            ownername = response[1]['Owner']['DisplayName']
            print("All bucket:")
            print(
                "+ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - "
                "- - - - - - - - - - - - - - - - - - - +")
            print("| %20s |%25s |%50s |" % ("Name", "CreateDate", "Owner"))
            for bucket in buckets.keys():
                print("| %20s |%25s |%50s |" % (bucket,
                                                buckets[bucket][
                                                    'creationdate'].strftime(
                                                    '%b-%d-%y %H:%M:%S'),
                                                ownername))
                print(
                    "| - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"
                    " - - - - - - - - - - - - - - - - - - - - |")
            return 0
        else:
            print("Get bucket failed")
            return 1

    def get_bucket(self, bucket_name):
        '''
        List special bucket info;
        Return:
            response['name']: bucket name
            response['creationdate']: bucket creation time
            response['ownername']: bucket owner name
        '''
        bucket = {}
        response = self.getallbucket()
        ownername = response[1]['Owner']['DisplayName']
        if response is not 1:
            for bck in response[1][u'Buckets']:
                if bck[u'Name'] == bucket_name:
                    dictmp = {}
                    name = bck[u'Name']
                    dictmp['creationdate'] = bck[u'CreationDate'].strftime(
                        '%b-%d-%y %H:%M:%S')
                    dictmp['ownername'] = ownername
                    dictmp['name'] = name
                    bucket = dictmp
                    break
            if not bucket:
                return 1, "No bucket %s in server" % bucket_name
            else:
                return 0, bucket

    def headbucket(self, bucket_name):
        '''
         To determine if a bucket exists and you have permission to access it.
        :param bucket_name:
        :return:
        '''
        try:
            res = self.s3.head_bucket(Bucket=bucket_name)
            return 0, res
        except boto3exception.ClientError, e:
            return 1, e.message

    def createbucket(self, bucket_name):
        # create new bucket
        try:
            response = self.s3.create_bucket(Bucket=bucket_name)
        except Exception, e:
            return 1, e.message
        # logger.info("create bucket response: %s" %response)
        return 0, response

    def deletebucket(self, bucket_name):
        # delete bucket
        try:
            response = self.s3.delete_bucket(Bucket=bucket_name)
        except Exception, e:
            print("Delete bucket failed, detail: %s" % e.message)
            return 1, e.message
        print("delete bucket succeed response: %s" % response)
        response_code = response["ResponseMetadata"]["HTTPStatusCode"]
        if response_code is 204:
            return 0, response_code
        else:
            return 1, response_code

    def putobject(self, **kwargs):
        # put object disable MPU
        keys = kwargs.keys()
        if "bucket" in keys:
            bucket = kwargs["bucket"]
            kwargs.pop("bucket")
        if "body" in keys:
            body = kwargs["body"]
            kwargs.pop("body")
        if "key" in keys:
            key = kwargs["key"]
            kwargs.pop("key")
        if os.path.isfile(body):
            fp = open(body)
        else:
            fp = body
        try:
            response = self.s3.put_object(Bucket=bucket, Body=fp, Key=key,
                                          **kwargs)
            return 0, response
        except Exception, e:
            return 1, e.message

    def uploadobject(self, source_file, bucket_name, key_name, **kwargs):
        '''
        :param source_file: The source file which will be upload
        :param bucket_name: Target bucket name
        :param key_name: object name which upload in bucket
        :param kwargs: extra args, it's a dict. it support below field:
        'ACL', 'CacheControl', 'ContentDisposition', 'ContentEncoding', 'ContentLanguage',
        'ContentType', 'Expires', 'GrantFullControl', 'GrantRead', 'GrantReadACP', 'GrantWriteACP',
        'Metadata', 'RequestPayer', 'ServerSideEncryption', 'StorageClass', 'SSECustomerAlgorithm',
        'SSECustomerKey', 'SSECustomerKeyMD5', 'SSEKMSKeyId', 'WebsiteRedirectLocation'
        :return:
        '''

        if kwargs:
            extra_args = kwargs
        else:
            extra_args = None
        try:
            session = Session(aws_access_key_id=self.access_key,
                              aws_secret_access_key=self.security_key,
                              region_name=self.region_name)
            s3_resource = session.resource('s3', endpoint_url=self.endpoint_url,
                                           config=self.client_config)

        except s3Exception.ResourceLoadException, e:
            return 1, "Resource load exception" + e.message
        try:
            s3_resource.meta.client.upload_file(source_file, bucket_name,
                                                key_name,
                                                Config=self.transfer_config,
                                                ExtraArgs=extra_args)
            return 0, None
        except boto3exception.ClientError, e:
            return 1, "Upload object fail" + e.message
        except Exception, e:
            return 1, "Upload object meet exception" + e.__str__()

    def uploadobject_acm(self, source_file, bucket_name, file_name, **kwargs):
        # upload object into specify bucket via s3 client.
        if kwargs:
            extra_args = kwargs
        else:
            extra_args = None
        try:
            with open(source_file, "rb") as data:
                self.s3.upload_fileobj(data, bucket_name, file_name,
                                       ExtraArgs=extra_args)
        except Exception, e:
            return 1, "Upload object fail " + e.__str__()
        return 0, None

    def getallobjects(self, bucket_name, v=1, **kwargs):
        # get all objects from s3 bucket
        print("get all objects from bucket %s" % bucket_name)
        try:
            if v == 1:
                response = self.s3.list_objects(Bucket=bucket_name, **kwargs)
            else:
                response = self.s3.list_objects_v2(Bucket=bucket_name, **kwargs)
            return 0, response
        except Exception, e:
            return 1, e.message

    def objects(self, bucket_name):
        '''
            List all object in bucket info;
            :Return
            list
        '''
        response = self.getallobjects(bucket_name)
        if response[0] is 0:
            if u'Contents' not in response[1].keys():
                return 0, []
            else:
                objects = []
                objects = [obj[u'Key'] for obj in response[1][u'Contents']]
                return 0, objects
        else:
            return 1, response[1]

    def get_object_dict(self, bucket_name, key_name, **kwargs):
        '''
        :param bucket_name: bucket name
        :param key_name:  object name
        :param kwargs: extra args like:
            IfMatch='string',
            IfModifiedSince=datetime(2015, 1, 1),
            IfNoneMatch='string',
            IfUnmodifiedSince=datetime(2015, 1, 1),
            Range='string',
            ResponseCacheControl='string',
            ResponseContentDisposition='string',
            ResponseContentEncoding='string',
            ResponseContentLanguage='string',
            ResponseContentType='string',
            ResponseExpires=datetime(2015, 1, 1),
            VersionId='string',
            SSECustomerAlgorithm='string',
            SSECustomerKey='string',
            RequestPayer='requester',
            PartNumber=123
        :return:
        '''
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=key_name,
                                          **kwargs)
            return 0, response
        except s3Exception, e:
            return 1, e.message

    def listobjects(self, bucket_name, v=1, **kwargs):
        # show all objects which belows bucket

        response = self.getallobjects(bucket_name, v=v, **kwargs)
        objects = {}
        objects = collections.OrderedDict()
        dictmp = {}
        if response[0] is not 1:
            if "Contents" not in response[1].keys():
                print("no objects in this bucket")
                return 0, response[1]
            else:
                for n, pcon in enumerate(response[1]['Contents']):
                    name = pcon['Key']
                    size = pcon['Size']
                    dictmp = {}
                    dictmp = collections.OrderedDict()
                    dictmp['lastmodifytime'] = pcon['LastModified']
                    dictmp['ETag'] = pcon['ETag']
                    # if pcon['Owner']['DisplayName']:
                    if "Owner" in pcon.keys():
                        if "DisplayName" in pcon["Owner"].keys():
                            dictmp['owner'] = pcon['Owner']['DisplayName']
                        else:
                            dictmp['owner'] = ""
                    else:
                        dictmp['owner'] = ""
                    dictmp['size'] = size
                    objects[name] = dictmp
                print("All objects of bucket %s:" % bucket_name)
                print(
                    "+ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - "
                    "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +")
                print("| %10s |%20s |%40s |%35s |%20s |" % (
                    "Name", "CreateDate", "Owner", "ETag",
                    "Size"))
                for object in objects.keys():
                    print("| %10s |%20s |%40s |%35s | %18s |"
                          % (object,
                             objects[object]['lastmodifytime'].strftime(
                                 '%b-%d-%y %H:%M:%S'),
                             objects[object]['owner'],
                             objects[object]['ETag'],
                             objects[object]['size']))
                    print(
                        "| - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"
                        " - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -|")
                return 0, response[1]
        else:
            print("Could not get all objects from bucket")
            return 1, response[1]

    def listobject(self, bucket_name, key_name):
        '''
            lis an object from Amazon S3.
            :Return
            a dict object include object name, object last mondify time, Etag and object owner
        '''
        response = self.getallobjects(bucket_name)
        if response[0] is not 1:
            for pcon in response[1][u'Contents']:
                if pcon[u'Key'] == key_name:
                    tmp = {}
                    tmp['lastmodifytime'] = pcon[u'LastModified']
                    tmp['ETag'] = pcon[u'ETag']
                    tmp['owner'] = pcon[u'Owner'][u'DisplayName']
                    tmp['name'] = key_name
                    tmp['size'] = pcon[u'Size']
                    return 0, tmp
            return 1, "No found object %s in bucket %s" % key_name, bucket_name
        else:
            return 1, response[1]

    def copy_object_request(self, bucket_name, copy_source, new_key_name,
                            **kwargs):
        '''
            Creates a copy of an object that is already stored in Amazon S3.
            :Return
            a dict object include object name, object last mondify time, Etag and object owner
        '''

        try:
            response = self.s3.copy_object(Bucket=bucket_name,
                                           Key=new_key_name,
                                           CopySource=copy_source,
                                           **kwargs)
            if response[u'ResponseMetadata'][u'HTTPStatusCode'] is "200":
                return 0, response
            else:
                return 1, response
        except s3Exception, e:
            return 1, e.message

    def copy_object_in_same_bucket(self, bucket_name, key_name, new_key_name,
                                   **kwargs):
        '''

        :param bucket_name:  bucket's name
        :param key_name:  copy resource
        :param new_key_name: the new key's name
        :param kwargs: extra args like:
            ACL='private'|'public-read'|'public-read-write'|'authenticated-read'|'aws-exec-read'|'bucket-owner-read'|'bucket-owner-full-control',
            CacheControl='string',
            ContentDisposition='string',
            ContentEncoding='string',
            ContentLanguage='string',
            ContentType='string',
            CopySource='string' or {'Bucket': 'string', 'Key': 'string', 'VersionId': 'string'},
            CopySourceIfMatch='string',
            CopySourceIfModifiedSince=datetime(2015, 1, 1),
            CopySourceIfNoneMatch='string',
            CopySourceIfUnmodifiedSince=datetime(2015, 1, 1),
            Expires=datetime(2015, 1, 1),
            GrantFullControl='string',
            GrantRead='string',
            GrantReadACP='string',
            GrantWriteACP='string',
            Key='string',
            Metadata={
                'string': 'string'
            },
            MetadataDirective='COPY'|'REPLACE',
            TaggingDirective='COPY'|'REPLACE',
            ServerSideEncryption='AES256'|'aws:kms',
            StorageClass='STANDARD'|'REDUCED_REDUNDANCY'|'STANDARD_IA'|'ONEZONE_IA',
            WebsiteRedirectLocation='string',
            SSECustomerAlgorithm='string',
            SSECustomerKey='string',
            SSEKMSKeyId='string',
            CopySourceSSECustomerAlgorithm='string',
            CopySourceSSECustomerKey='string',
            RequestPayer='requester',
            Tagging='string'
        :return: return new object's last modify time, Etag
        '''
        response = self.copy_object_request(bucket_name,
                                            "%s/%s" % (bucket_name, key_name),
                                            new_key_name, **kwargs)
        if response[0] is not 0:
            obj_info = {}
            obj_info['name'] = new_key_name
            obj_info['lastmodifytime'] = response[1]['CopyObjectResult'][
                u'LastModified'].strftime('%b-%d-%y %H:%M:%S')
            obj_info[u'ETag'] = response[1]['CopyObjectResult'][u'ETag']
            return 0, obj_info
        else:
            return 1, response[1]

    def copy_object_in_different_bucket(self, **kwargs):
        '''

        :param kwargs:
            sbucket: the source bucket name
            sfile: the source file name
            tbucket: the bucket which store new object
            tfile: new file name
        :return: return new object's last modify time, Etag
        '''''
        if not kwargs:
            return 1, "Need special paramters"
        else:
            sbucket = kwargs['sbucket']
            sfile = kwargs['sfile']
            tbucket = kwargs['tbucket']
            tfile = kwargs['tfile']
            kwargs.pop("sbucket")
            kwargs.pop("sfile")
            kwargs.pop("tbucket")
            kwargs.pop("tfile")
        response = self.copy_object_request(tbucket,
                                            "%s/%s" % (sbucket, sfile),
                                            tfile, **kwargs)
        if response[0] is not 0:
            obj_info = {}
            obj_info['name'] = tfile
            obj_info['lastmodifytime'] = \
                response[1]['CopyObjectResult'][u'LastModified'].strftime(
                    '%b-%d-%y %H:%M:%S')
            obj_info[u'ETag'] = response[1]['CopyObjectResult'][u'ETag']
            return 0, obj_info
        else:
            return 1, response[1]

    def copyobject(self, **kwargs):
        '''

        :param kwargs:
            sbucket: the source bucket name
            sfile: the source file name
            tbucket: the bucket which store new object
            tfile: new file name
        :return: return new object's last modify time, Etag
        '''
        if not kwargs:
            return 1, "Need special paramters"
        else:
            sbucket = kwargs['sbucket']
            sfile = kwargs['skey']
            tbucket = kwargs['tbucket']
            tfile = kwargs['tkey']
            kwargs.pop("sbucket")
            kwargs.pop("skey")
            kwargs.pop("tbucket")
            kwargs.pop("tkey")
        response = self.copy_object_request(tbucket,
                                            "%s/%s" % (sbucket, sfile),
                                            tfile, **kwargs)
        return response[0], response[1]

    def deleteobject(self, bucket_name, key_name):
        # deleted object from bucket
        try:
            response = self.s3.delete_object(Bucket=bucket_name, Key=key_name)
            '''if response['ResponseMetadata']['HTTPStatusCode'] is 204:
                return 0, response['ResponseMetadata']['HTTPStatusCode']
            else:
                return 1, "Delete bucket fail, error code: %s" \
                       % response['ResponseMetadata']['HTTPStatusCode']'''
            return 0, response
        except Exception, e:
            return 1, e.message

    def download_object(self, bucket_name, key_name, local_name, **kwargs):
        '''
                :param bucket_name: bucket name
                :param key_name: object name
                :param local_name: special local file which to store download file
                :param kwargs: extra args like
                    ['VersionId', 'SSECustomerAlgorithm', 'SSECustomerKey', 'SSECustomerKeyMD5', 'RequestPayer']Â¶
                :return:
        '''
        if kwargs:
            extra_args = kwargs
        else:
            extra_args = None
        try:
            session = Session(aws_access_key_id=self.access_key,
                              aws_secret_access_key=self.security_key,
                              region_name=self.region_name)
            s3_resource = session.resource('s3', endpoint_url=self.endpoint_url,
                                           config=self.client_config)
            s3_resource.meta.client.download_file(bucket_name, key_name,
                                                  local_name,
                                                  ExtraArgs=extra_args,
                                                  Config=self.transfer_download_config)
            return 0, None
        except boto3exception.ClientError, e:
            return 1, e.message
        except Exception, e:
            return 1, e.__str__()

    # Get object without mpu
    def getobject(self, **kwargs):
        # put object disable MPU
        keys = kwargs.keys()
        if "bucket" in keys:
            bucket = kwargs["bucket"]
            kwargs.pop("bucket")
        if "key" in keys:
            key = kwargs["key"]
            kwargs.pop("key")
        try:
            response = self.s3.get_object(Bucket=bucket, Key=key, **kwargs)
            fr = response[u"Body"].read()
            response[u"Body"] = fr
            return 0, response
        except Exception, e:
            return 1, e.message

    '''
    def uploadobject_with_multipart(self, source_file, bucket_name, key_name,):
        try:
            init_res = self.s3.create_multipart_upload(Bucket=bucket_name,
                                                       Key=key_name)
        except Exception,e:
            return 1, "Init multipart upload fail" + e.message
        try:
            upload_res = self.s3.upload_part(Bucket=bucket_name,
                                             Key=key_name,
                                             UploadId=init_res['UploadId'],
                                             PartNumber=1,
                                             Body=source_file)
        except Exception, e:
            self.s3.abort_multipart_upload(Bucket=bucket_name,
                                           Key=key_name,
                                           UploadId=init_res['UploadId'])
            return 1, "Upload parts fail" + e.message
        try:
            complete_res = self.s3.complete_multipart_upload(Bucket=bucket_name,
                                                             Key=key_name,
                                                             MultipartUpload={
                                                                 'Parts': [
                                                                     {
                                                                         'ETag': upload_res['ETag'],
                                                                         'PartNumber': 1
                                                                     },
                                                                 ]
                                                             },
                                                             UploadId=init_res['UploadId'])
            return 0, complete_res
        except Exception, e:
            self.s3.abort_multipart_upload(Bucket=bucket_name,
                                           Key=key_name,
                                           UploadId=init_res['UploadId'])
            return 1, "Complete upload fail" + e.message'''

    def list_multipart(self, bucket_name, **kwargs):
        try:
            response = self.s3.list_multipart_uploads(Bucket=bucket_name,
                                                      **kwargs)
            '''if "Uploads" in response.keys():
                print("+ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - "
                      "- - - - - - - - - - - - - - - - - - - +")
                print("| %18s |%30s |%20s |%35s |" % ("Name", "UploadId", "InitDate", "Owner"))

                upload_dic = {}
                for upload in response["Uploads"]:
                    dictmp={}
                    dictmp["Key"] = upload["Key"]
                    dictmp["UploadId"] = upload["UploadId"]
                    dictmp["Initiated"] = upload["Initiated"].strftime('%b-%d-%y %H:%M:%S')
                    dictmp["Owner"] = upload["Owner"]["ID"]
                    print("| %18s |%30s |%20s |%35s |" % (dictmp["Key"],
                                                          dictmp["UploadId"],
                                                          dictmp["Initiated"],
                                                          dictmp["Owner"]))
                return 0, response
            else:
                print("No multipart upload object")
                return 0, response'''
            return 0, response
        except Exception, e:
            return 1, "List multipart upload fail: " + e.message

    def list_upload_part(self, **kwargs):
        bucket_name, key_name, upload_id = None, None, None
        keys = kwargs.keys()
        if "Bucket" in keys:
            bucket_name = kwargs["Bucket"]
            kwargs.pop("Bucket")
        else:
            return 1, "Need bucket name"
        if "Key" in keys:
            key_name = kwargs["Key"]
            kwargs.pop("Key")
        else:
            return 1, "Need key name"
        if "UploadId" in keys:
            upload_id = kwargs["UploadId"]
            kwargs.pop("UploadId")
        else:
            return 1, "Need upload id"
        try:
            response = self.s3.list_parts(Bucket=bucket_name,
                                          Key=key_name,
                                          UploadId=upload_id,
                                          **kwargs)
            return 0, response
        except Exception, e:
            return 1, "list multipart upload object fail: " + e.message

    def create_multipart(self, bucket_name, key_name, **kwargs):
        try:
            init_res = self.s3.create_multipart_upload(Bucket=bucket_name,
                                                       Key=key_name, **kwargs)
            return 0, init_res
        except Exception, e:
            return 1, "Init multipart upload fail" + e.message

    def delete_multipart(self, bucket_name, key_name, uploadid):
        try:
            response = self.s3.abort_multipart_upload(Bucket=bucket_name,
                                                      Key=key_name,
                                                      UploadId=uploadid)
            return 0, response
        except Exception, e:
            return 1, "Delete multipart fail " + e.message

    def upload_multipart(self, **kwargs):
        bucket_name, upload_id, key_name = None, None, None
        key_param = kwargs.keys()
        if "Bucket" in key_param:
            bucket_name = kwargs["Bucket"]
            kwargs.pop("Bucket")
        if "Key" in key_param:
            key_name = kwargs["Key"]
            kwargs.pop("Key")
        if "UploadId" in key_param:
            upload_id = kwargs["UploadId"]
            kwargs.pop("UploadId")
        if "PartNumber" in key_param:
            part_number = int(kwargs["PartNumber"])
            kwargs.pop("PartNumber")
        else:
            part_number = 1
        if "Body" in key_param:
            body = kwargs["Body"]
            kwargs.pop("Body")
        else:
            body = ""

        try:
            if os.path.exists(body):
                upload_res = self.s3.upload_part(Bucket=bucket_name,
                                                 Key=key_name,
                                                 UploadId=upload_id,
                                                 PartNumber=part_number,
                                                 Body=open(body, "r"),
                                                 **kwargs)
            else:
                upload_res = self.s3.upload_part(Bucket=bucket_name,
                                                 Key=key_name,
                                                 UploadId=upload_id,
                                                 PartNumber=part_number,
                                                 Body=body, **kwargs)
            return 0, upload_res
        except Exception, e:
            return 1, "Upload multipart fail " + e.message

    def headobject(self, bucket_name, key_name, **kwargs):
        '''
        :param bucket_name: bucket name
        :param key_name: object name
        :param kwargs: extra args like:
            IfMatch='string',
            IfModifiedSince=datetime(2015, 1, 1),
            IfNoneMatch='string',
            IfUnmodifiedSince=datetime(2015, 1, 1),
            Range='string',
            VersionId='string',
            SSECustomerAlgorithm='string',
            SSECustomerKey='string',
            SSECustomerKeyMD5='string'
            RequestPayer='requester',
            PartNumber=123
        :return:
        '''
        try:
            response = self.s3.head_object(Bucket=bucket_name, Key=key_name,
                                           **kwargs)
            return 0, response
        except Exception, e:
            return 1, "Head object fail " + e.message

    def complete_multipart(self, **kwargs):
        bucket_name, key_name, upload_id = None, None, None
        key_param = kwargs.keys()
        if "Bucket" in key_param:
            bucket_name = kwargs["Bucket"]
        if "Key" in key_param:
            key_name = kwargs["Key"]
        if "UploadId" in key_param:
            upload_id = kwargs["UploadId"]
        if "MultipartUpload" in key_param:
            multipartupload = kwargs["MultipartUpload"]
        else:
            multipartupload = None

        print multipartupload
        try:
            if multipartupload:
                complete_res = self.s3.complete_multipart_upload(
                    Bucket=bucket_name, Key=key_name,
                    MultipartUpload=multipartupload,
                    UploadId=upload_id)
            else:
                complete_res = self.s3.complete_multipart_upload(
                    Bucket=bucket_name, Key=key_name,
                    UploadId=upload_id)
            return 0, complete_res
        except Exception, e:
            return 1, "Complete multipart fail " + e.message

    def upload_object_reset_part(self, **kwargs):
        bucket_name, key_name, source_file, upload_config = None, None, None, None
        # upload object into specify bucket
        keys = kwargs.keys()
        if "Bucket" in keys:
            bucket_name = kwargs["Bucket"]
            kwargs.pop("Bucket")
        else:
            return 1, "Need bucket name"
        if "Key" in keys:
            key_name = kwargs["Key"]
            kwargs.pop("Key")
        else:
            return 1, "need key name"
        if "File" in keys:
            source_file = kwargs["File"]
            kwargs.pop("File")
        else:
            return 1, "need source file"
        if "Concurrency" in keys:
            if "use_threads" in keys and kwargs["use_threads"] == "False":
                upload_config = boto3.s3.transfer.TransferConfig(
                    max_concurrency=kwargs["Concurrency"],
                    use_threads=False)
            else:
                upload_config = boto3.s3.transfer.TransferConfig(
                    max_concurrency=kwargs["Concurrency"],
                    use_threads=True)
            kwargs.pop("use_threads")
            kwargs.pop("Concurrency")

        try:
            session = Session(aws_access_key_id=self.access_key,
                              aws_secret_access_key=self.security_key,
                              region_name=self.region_name)
            s3_resource = session.resource('s3', endpoint_url=self.endpoint_url,
                                           config=self.client_config)

        except s3Exception.ResourceLoadException, e:
            return 1, "Resource load exception" + e.message
        if kwargs:
            extra_args = kwargs
        else:
            extra_args = None
        if not upload_config:
            status, res = self.uploadobject(source_file, bucket_name, key_name,
                                            **kwargs)
            return status, res
        else:
            try:
                s3_resource.meta.client.upload_file(source_file, bucket_name,
                                                    key_name,
                                                    Config=upload_config,
                                                    ExtraArgs=extra_args)
                return 0, None
            except boto3exception.ClientError, e:
                return 1, "Upload object fail" + e.message
            except Exception, e:
                return 1, "Upload object meet exception" + e.__str__()


@click.group()
@click.option('-u',  help='s3 user')
@click.option('-p',  help='password for special s3 user')
@click.option('-s',  help='The address of serve')
@click.option('--sign_v',  help='s3 request version, default is s3v2')
@click.option('-v',  help='bucket name')
def s3test(u, p, s, sign_v, v):
    def show_version():
        click.echo("Version: {}".format(SELF_VERSION))

    if v is True:
        show_version()
    else:
       global s3_instance
       s3_instance = s3_client(access_key=u, security_key=p,
                            endpoint_url=s, sign_v=sign_v)


@click.command("createbucket", short_help="Create bucket")
@click.option('-b', required=True, help='bucket name')
def createbucket(b):
    status, res = s3_instance.createbucket(b)
    if status is 0:
        print res
        print("Success")
    else:
        print res
        exit(status)


@click.command('listbuckets', short_help='list buckets')
def listbuckets():
    status = s3_instance.list_bucket()
    exit(status)


@click.command('getbucket', short_help='get bucket')
@click.option('-b', required=True, help='bucket name')
def getbucket(b):
    status, res = s3_instance.get_bucket(b)
    if status is 0:
        print res
        print("All bucket:")
        print(
            "+ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - "
            "- - - - - - - - - - - - - - - - - - - +")
        print("| %20s |%25s |%50s |" % ("Name", "CreateDate", "Owner"))
        print("| %20s |%25s |%50s |"
              % (b,
                 res['creationdate'],
                 res["ownername"]))
        print(
            "| - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"
            " - - - - - - - - - - - - - - - - - - - - |")
        exit(status)
    else:
        print res
        exit(status)


@click.command('deletebucket', short_help='delete bucket')
@click.option('-b', required=True, help='bucket name')
def deletebucket(b):
    status, res = s3_instance.deletebucket(b)
    if status is 0:
        print res
        print("Success")
        exit(status)
    else:
        print res
        exit(status)


@click.command('headbucket', short_help='head bucket')
@click.option('-b', required=True, help='bucket name')
def headbucket(b):
    status, res = s3_instance.headbucket(b)
    if status is 0:
        print res
        print("Success")
        exit(status)
    else:
        print res
        exit(status)


@click.command('putobject', short_help='put object')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--user_key', required=False, help="user encryption key")
@click.option('--body', required=True, help='upload object name')
def putobject(b, k, body, user_key=None):
    if user_key is not None:
        encrypt_key = base64.b64encode(user_key)
        m1 = hashlib.md5(user_key)
        encrypt_md5 = base64.b64encode(m1.digest())
        status, res = s3_instance.putobject(bucket=b,
                                            body=body,
                                            key=k,
                                            SSECustomerAlgorithm="AES256",
                                            SSECustomerKey=encrypt_key,
                                            SSECustomerKeyMD5=encrypt_md5)
    else:
        status, res = s3_instance.putobject(bucket=b,
                                            body=body,
                                            key=k)
    if status is 0:
        print res
        print("Success")
        exit(status)
    else:
        print res
        exit(status)


@click.command('getobject', short_help='get object')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--user_key', required=False, help="user encryption key")
@click.option('--local_file', required=True, help='upload object name')
def getobject(b, k, local_file, user_key=None):
    if user_key is not None:
        encrypt_key = base64.b64encode(user_key)
        m1 = hashlib.md5(user_key)
        encrypt_md5 = base64.b64encode(m1.digest())
        status, res = s3_instance.getobject(bucket=b,
                                            key=k,
                                            SSECustomerAlgorithm="AES256",
                                            SSECustomerKey=encrypt_key,
                                            SSECustomerKeyMD5=str(
                                                encrypt_md5)
                                            )
    else:
        status, res = s3_instance.getobject(bucket=b,
                                            key=k)
    if status is 0:
        if not local_file:
            print res
        else:
            with open(local_file, "w+") as fr:
                fr.write(res[u"Body"])
        print("Success")
        exit(status)
    else:
        print res
        exit(status)


@click.command('uploadobject', short_help='upload object')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--user_key', default=None, help="user encryption key")
@click.option('--body', required=True, help='upload object name')
@click.option('--concurrency', type=int,
              help='Special upload concurrency threads')
@click.option('--use_threads', default=True,
              help='whether used threads when upload file,'
                   ' value is True or False')
def uploadobject(**options):
    if options['concurrency'] is not None:
        if options['user_key'] is not None:
            encrypt_key = base64.b64encode(options['user_key'])
            m1 = hashlib.md5(options['user_key'])
            encrypt_md5 = base64.b64encode(m1.digest())
            status, res = s3_instance.upload_object_reset_part(
                Bucket=options['b'],
                Key=options['k'],
                File=options['body'],
                SSECustomerAlgorithm="AES256",
                SSECustomerKey=encrypt_key,
                SSECustomerKeyMD5=str(encrypt_md5))
        else:
            status, res = s3_instance.upload_object_reset_part(
                Bucket=options['b'],
                Key=options['k'],
                File=options['body'])
    else:
        if options['user_key'] is not None:
            encrypt_key = base64.b64encode(options['user_key'])
            m1 = hashlib.md5(options['user_key'])
            encrypt_md5 = base64.b64encode(m1.digest())
            status, res = s3_instance.upload_object_reset_part(
                Bucket=options['b'],
                Key=options['k'],
                File=options['body'],
                Concurrency=options['concurrency'],
                use_threads=options['use_threads'],
                SSECustomerAlgorithm="AES256",
                SSECustomerKey=encrypt_key,
                SSECustomerKeyMD5=str(encrypt_md5))
        else:
            status, res = s3_instance.upload_object_reset_part(
                Bucket=options['b'],
                Key=options['k'],
                File=options['body'],
                Concurrency=options['concurrency'],
                use_threads=options['use_threads'])
    if status is 0:
        print res
        print("Success")
        exit(status)
    else:
        print res
        exit(status)


@click.command('listobjects', short_help='list objects')
@click.option('-b', required=True, help='bucket name')
@click.option('--prefix', required=False, help='prefix query')
@click.option('--marker', required=False, help='list with marker')
@click.option('--maxkeys', required=False, type=int, help='list with maxkeys')
@click.option('--delimiter', required=False, type=str,
              help='list with delimiter')
def listobjects(**options):
    kwargs = {}
    if options['prefix'] is not None:
        kwargs["Prefix"] = options['prefix']
    if options['marker'] is not None:
        kwargs["Marker"] = options['marker']
    if options['maxkeys'] is not None:
        kwargs['MaxKeys'] = options['maxkeys']
    if options['delimiter'] is not None:
        kwargs['Delimiter'] = options['delimiter']
    status, response = s3_instance.listobjects(options['b'], **kwargs)
    # response = s3_instance.s3.list_objects(Bucket=args.bucket, **kwargs)
    if status is not 0:
        print response
        exit(status)
    else:
        print response['ResponseMetadata']
    key_dict = response.keys()
    if 'Marker' in key_dict:
        print "marker: " + response['Marker']
    if 'NextMarker' in key_dict:
        print "NextMarker: " + response['NextMarker']
    if 'MaxKeys' in key_dict:
        print "MaxKeys: %d" % response['MaxKeys']
    if 'Name' in key_dict:
        print "Name: %s" % response['Name']
    if "Delimiter" in key_dict:
        print "Delimiter: %s" % response['Delimiter']
    if 'Prefix' in key_dict:
        print 'Prefix: %s' % response['Prefix']
    if 'IsTruncated' in key_dict:
        print "IsTruncated:  %s" % response['IsTruncated'].__str__()
    if 'CommonPrefixes' in key_dict:
        print "CommonPrefixes"
        print response['CommonPrefixes']
    # res_str = json.dumps(response,cls=DateTimeEncoder)
    # print res_str
    exit(status)


@click.command('listobjects_v2', short_help='list objects')
@click.option('-b', required=True, help='bucket name')
@click.option('--prefix', required=False, help='prefix query')
@click.option('--marker', required=False, help='list with marker')
@click.option('--maxkeys', required=False, type=int, help='list with maxkeys')
@click.option('--delimiter', required=False, type=str,
              help='list with delimiter')
@click.option('--start_after', required=False, type=str,
              help='StartAfter is where you want Amazon S3 '
                   'to start listing from. Amazon S3 starts '
                   'listing after this specified key. '
                   'StartAfter can be any key in the bucket. ')
def listobjects_v2(**options):
    kwargs = {}
    if options['prefix'] is not None:
        kwargs["Prefix"] = options['prefix']
    if options['marker'] is not None:
        kwargs["ContinuationToken"] = options['marker']
    if options['maxkeys'] is not None:
        kwargs['MaxKeys'] = options['maxkeys']
    if options['delimiter'] is not None:
        kwargs['Delimiter'] = options['delimiter']
    if options['start_after'] is not None:
        kwargs['StartAfter'] = options['start_after']

    status, response = s3_instance.listobjects(options['b'], v=2, **kwargs)
    if status is not 0:
        print response
        exit(status)
    else:
        print response['ResponseMetadata']
    key_dict = response.keys()
    if 'ContinuationToken' in key_dict:
        print "ContinuationToken: " + response['ContinuationToken']
    if 'NextContinuationToken' in key_dict:
        print "NextContinuationToken: " + response['NextContinuationToken']
    if 'MaxKeys' in key_dict:
        print "MaxKeys: %d" % response['MaxKeys']
    if 'Name' in key_dict:
        print "Name: %s" % response['Name']
    if "Delimiter" in key_dict:
        print "Delimiter: %s" % response['Delimiter']
    if 'Prefix' in key_dict:
        print 'Prefix: %s' % response['Prefix']
    if 'IsTruncated' in key_dict:
        print "IsTruncated:  %s" % response['IsTruncated'].__str__()
    if 'CommonPrefixes' in key_dict:
        print "CommonPrefixes"
        print response['CommonPrefixes']
    if 'StartAfter' in key_dict:
        print "StartAfter: " + response['StartAfter']
    if 'KeyCount' in key_dict:
        print "KeyCount: " + str(response['KeyCount'])
    # res_str = json.dumps(response,cls=DateTimeEncoder)
    # print res_str
    exit(status)


@click.command('downloadobject', short_help='download objects')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--user_key', required=False, help='user encryption key')
@click.option('--local_file', required=False, help='key name')
def downloadobject(**options):
    local_file = ""
    if options['local_file'] is not None:
        local_file = options['k']
    if options['user_key'] is not None:
        encrypt_key = base64.b64encode(options['user_key'])
        m1 = hashlib.md5(options['user_key'])
        encrypt_md5 = base64.b64encode(m1.digest())
        status, res = s3_instance.download_object(
            options['b'], options['k'], local_file,
            SSECustomerAlgorithm="AES256",
            SSECustomerKey=encrypt_key,
            SSECustomerKeyMD5=str(encrypt_md5)
        )
    else:
        status, res = s3_instance.download_object(
            options['b'], options['k'], local_file)
    if status is 0:
        print res
        print("Success")
        exit(status)
    else:
        print res
        exit(status)


@click.command('copyobject', short_help='copy objects')
@click.option('--source_bucket', required=True, help='source bucket name')
@click.option('--source_key', required=True, help='source key name')
@click.option('--target_bucket', required=True, help='target bucket name')
@click.option('--target_key', required=True, help='target key name')
@click.option('--source_user_key', required=False,
              help='source user encryption key')
@click.option('--target_user_key', required=False,
              help='target user encryption key')
@click.option('--local_file', required=False, help='key name')
def copyobject(**options):
    if options['source_user_key'] is not None:
        source_encrypt_key = base64.b64encode(options['source_user_key'])
        m1 = hashlib.md5(options['source_user_key'])
        source_encrypt_md5 = base64.b64encode(m1.digest())
        if options['target_user_key'] is not None:
            encrypt_key = base64.b64encode(options['target_user_key'])
            m1 = hashlib.md5(options['target_user_key'])
            encrypt_md5 = base64.b64encode(m1.digest())
            status, res = s3_instance.copyobject(
                sbucket=options['source_bucket'],
                skey=options['source_key'],
                tbucket=options['target_bucket'],
                tkey=options['target_key'],
                SSECustomerAlgorithm="AES256",
                SSECustomerKey=encrypt_key,
                SSECustomerKeyMD5=str(encrypt_md5),
                CopySourceSSECustomerAlgorithm="AES256",
                CopySourceSSECustomerKey=source_encrypt_key,
                CopySourceSSECustomerKeyMD5=str(source_encrypt_md5)
            )
        else:
            status, res = s3_instance.copyobject(
                sbucket=options['source_bucket'],
                skey=options['source_key'],
                tbucket=options['target_bucket'],
                tkey=options['target_key'],
                CopySourceSSECustomerAlgorithm="AES256",
                CopySourceSSECustomerKey=source_encrypt_key,
                CopySourceSSECustomerKeyMD5=str(source_encrypt_md5))
    else:
        if options['target_user_key'] is not None:
            encrypt_key = base64.b64encode(options['target_user_key'])
            m1 = hashlib.md5(options['target_user_key'])
            encrypt_md5 = base64.b64encode(m1.digest())
            status, res = s3_instance.copyobject(
                sbucket=options['source_bucket'],
                skey=options['source_key'],
                tbucket=options['target_bucket'],
                tkey=options['target_key'],
                SSECustomerAlgorithm="AES256",
                SSECustomerKey=encrypt_key,
                SSECustomerKeyMD5=str(encrypt_md5), )
        else:
            status, res = s3_instance.copyobject(
                sbucket=options['source_bucket'],
                skey=options['source_key'],
                tbucket=options['target_bucket'],
                tkey=options['target_key'])
    if status is 0:
        print res
        print("Success")
        exit(status)
    else:
        print res
        exit(status)


@click.command('headobject', short_help='head objects')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--user_key', required=False,
              help='user encryption key')
def headobject(**options):
    if options['user_key'] is not None:
        encrypt_key = base64.b64encode(options['user_key'])
        m1 = hashlib.md5(options['user_key'])
        encrypt_md5 = base64.b64encode(m1.digest())
        status, res = s3_instance.headobject(options['b'], options['k'],
                                            SSECustomerAlgorithm="AES256",
                                            SSECustomerKey=encrypt_key,
                                            SSECustomerKeyMD5=str(
                                                encrypt_md5)
                                            )
    else:
        status, res = s3_instance.headobject(options['b'], options['k'])
    if status is 0:
        print res
        print("Success")
        exit(status)
    else:
        print res
        exit(status)


@click.command('deleteobject', short_help='delete objects')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
def deleteobject(b, k):
    status, res = s3_instance.deleteobject(b, k)
    if status is 0:
        print res
        print("Success")
        exit(status)
    else:
        print res
        exit(status)


@click.command('multipart_init', short_help='multipart init objects')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--user_key', required=False,
              help='user encryption key')
def multipart_init(**options):
    if options['user_key'] is not None:
        encrypt_key = base64.b64encode(options['user_key'])
        m1 = hashlib.md5(options['user_key'])
        encrypt_md5 = base64.b64encode(m1.digest())
        kwargs = {}
        kwargs['SSECustomerAlgorithm'] = "AES256"
        kwargs['SSECustomerKey'] = encrypt_key
        kwargs['SSECustomerKeyMD5'] = str(encrypt_md5)
        status, res = s3_instance.create_multipart(options['b'], options['k'],
                                                    **kwargs
                                                    )
    else:
        status, res = s3_instance.create_multipart(options['b'], options['k'])
    if status is 0:
        print res
        print(
            "+ - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - "
            "- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -+")
        print("| %20s |%25s |%40s |%30s" % (
            "Name", "UploadId", "Etag", "date"))
        print("| %20s |%25s |%40s |%30s"
             % (options['k'],
                res['UploadId'],
                res["ResponseMetadata"]["HTTPHeaders"]["etag"],
                res["ResponseMetadata"]["HTTPHeaders"]["date"],
                   ))
        print(
            "| - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -"
            " - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -+")
    else:
        print res
        exit(status)


@click.command('multipart_upload', short_help='multipart upload objects')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--upload_id', required=True, help='upload object id')
@click.option('--body', required=True, help='source upload file')
@click.option('--part_number', required=True, help='upload part number')
@click.option('--user_key', required=False,
              help='user encryption key')
def multipart_upload(**options):
    if options['user_key'] is not None:
        kwargs = {}
        encrypt_key = base64.b64encode(options['user_key'])
        m1 = hashlib.md5(options['user_key'])
        encrypt_md5 = base64.b64encode(m1.digest())
        kwargs['SSECustomerAlgorithm'] = "AES256"
        kwargs['SSECustomerKey'] = encrypt_key
        kwargs['SSECustomerKeyMD5'] = str(encrypt_md5)
        kwargs['UploadId'] = options['upload_id']
        kwargs['PartNumber'] = options['part_number']
        kwargs['Body'] = options['body']
        status, res = s3_instance.upload_multipart(
            Bucket=options['b'],
            Key=options['k'],
            **kwargs
        )
    else:
        status, res = s3_instance.upload_multipart(
            Bucket=options['b'],
            Key=options['k'],
            UploadId=options['upload_id'],
            PartNumber=options['part_number'],
            Body=options['body'])
    print res
    exit(status)


@click.command('multipart_list', short_help='list multipart upload objects')
@click.option('-b', required=True, help='bucket name')
def multipart_list(**options):
    status, res = s3_instance.list_multipart(options['b'])
    print res
    exit(status)


@click.command('multipart_list_parts', short_help='list object upload part')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--upload_id', required=True, help='upload object id')
def multipart_list_parts(**options):
    status, res = s3_instance.list_upload_part(
        Bucket=options['b'], Key=options['k'], UploadId=options['upload_id'])
    print res
    exit(status)


@click.command('multipart_abort', short_help='Abort multipart upload')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--upload_id', required=True, help='upload object id')
def multipart_abort(b, k, upload_id):
    status, res = s3_instance.delete_multipart(
        b, k, upload_id)
    print res
    exit(status)


@click.command('multipart_complete', short_help='Abort multipart complete')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--upload_id', required=True, help='upload object id')
@click.option('--parts', required=True, help='upload object parts')
def multipart_complete(**options):
    """

    :param options parts format as blow:
     part = {
                                   'Parts': [
                                       {
                                           'ETag': upload_res['ETag'],
                                           'PartNumber': 1
                                       },
                                   ]
                        }
    :return:
    """
    if os.path.exists(options['parts']):
        fp = open(options['parts'], "r")
        upload_parts = json.load(fp)
    else:
        upload_parts = eval(options['parts'])
    status, res = s3_instance.complete_multipart(
        Bucket=options['b'], Key=options['k'],UploadId=options['upload_id'],
        MultipartUpload=upload_parts)
    print res
    exit(status)


@click.command('presign', short_help='generate presign url')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--expires_in', required=False, type=int,
              help='expire time range')
@click.option('--user_key', required=False, help='user encryption key')
def presign(**options):
    op_keys = options.keys()
    if options['expires_in'] is not None:
        if options['user_key'] is not None:
            encrypt_key = base64.b64encode(options['user_key'])
            m1 = hashlib.md5(options['user_key'])
            encrypt_md5 = base64.b64encode(m1.digest())
            kwargs = {}
            kwargs['SSECustomerAlgorithm'] = "AES256"
            kwargs['SSECustomerKey'] = encrypt_key
            kwargs['SSECustomerKeyMD5'] = str(encrypt_md5)
            kwargs['Bucket'] = options['b']
            kwargs['Key'] = options['k']
            url = s3_instance.s3.generate_presigned_url(
                ClientMethod='get_object',
                Params=kwargs,
                ExpiresIn=options['expires_in']
            )
        else:
            url = s3_instance.s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': options['b'],
                    'Key': options['k']},
                ExpiresIn=options['expires_in']
            )

    else:
        if options['user_key'] is not None:
            encrypt_key = base64.b64encode(options['user_key'])
            m1 = hashlib.md5(options['user_key'])
            encrypt_md5 = base64.b64encode(m1.digest())
            kwargs = {}
            kwargs['SSECustomerAlgorithm'] = "AES256"
            kwargs['SSECustomerKey'] = encrypt_key
            kwargs['SSECustomerKeyMD5'] = str(encrypt_md5)
            kwargs['Bucket'] = options['b']
            kwargs['Key'] = options['k']
            url = s3_instance.s3.generate_presigned_url(
                ClientMethod='get_object',
                Params=kwargs
            )
        else:
            url = s3_instance.s3.generate_presigned_url(
                ClientMethod='get_object',
                Params={
                    'Bucket': options['b'],
                    'Key': options['k'], }
            )
    print url


@click.command('presign_put', short_help='upload object by  presign url')
@click.option('-b', required=True, help='bucket name')
@click.option('-k', required=True, help='key name')
@click.option('--expires_in', required=False, help='expire time range')
@click.option('--user_key', required=False, help='user encryption key')
def presign_put(**options):
    kwargs = {}
    kwargs['Bucket'] = options['b']
    kwargs['Key'] = options['k']
    if options['user_key'] is not None:
        encrypt_key = base64.b64encode(options['user_key'])
        m1 = hashlib.md5(options['user_key'])
        encrypt_md5 = base64.b64encode(m1.digest())
        kwargs['SSECustomerAlgorithm'] = "AES256"
        kwargs['SSECustomerKey'] = encrypt_key
        kwargs['SSECustomerKeyMD5'] = str(encrypt_md5)
    if options['expires_in'] is not None:
        # url = s3_instance.s3.generate_presigned_post(args.bucket, args.key,
        #                                             ExpiresIn=args.expiresIn)
        url = s3_instance.s3.generate_presigned_url(
            ClientMethod='put_object',
            Params=kwargs,
            ExpiresIn=options['expires_in'],
            HttpMethod='PUT')
    else:
        # url = s3_instance.s3.generate_presigned_url(args.bucket, args.key)
        url = s3_instance.s3.generate_presigned_url(
            ClientMethod='put_object',
            Params=kwargs,
            HttpMethod='PUT')
    print url


s3test.add_command(createbucket)
s3test.add_command(listbuckets)
s3test.add_command(getbucket)
s3test.add_command(deletebucket)
s3test.add_command(headbucket)
s3test.add_command(putobject)
s3test.add_command(getobject)
s3test.add_command(uploadobject)
s3test.add_command(listobjects)
s3test.add_command(listobjects_v2)
s3test.add_command(downloadobject)
s3test.add_command(copyobject)
s3test.add_command(headobject)
s3test.add_command(deleteobject)
s3test.add_command(multipart_init)
s3test.add_command(multipart_upload)
s3test.add_command(multipart_list)
s3test.add_command(multipart_list_parts)
s3test.add_command(multipart_abort)
s3test.add_command(multipart_complete)
s3test.add_command(presign)
s3test.add_command(presign_put)

if __name__ == '__main__':
    s3test()
