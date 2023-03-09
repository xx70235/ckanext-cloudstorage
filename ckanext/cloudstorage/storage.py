#!/usr/bin/env python
# -*- coding: utf-8 -*-
import binascii
import cgi
import hashlib
import logging
import mimetypes
import os
import tempfile
import traceback
from ast import literal_eval
from datetime import datetime, timedelta
from urllib.parse import urljoin

import ckan.plugins as p
import libcloud.common.types as types
from ckan import model
from ckan.lib import munge
from libcloud.storage.providers import get_driver
from libcloud.storage.types import ObjectDoesNotExistError, Provider
from werkzeug.datastructures import FileStorage as FlaskFileStorage
import oss2
from oss2.exceptions import NoSuchKey
config = p.toolkit.config

log = logging.getLogger(__name__)

ALLOWED_UPLOAD_TYPES = (cgi.FieldStorage, FlaskFileStorage)
AWS_UPLOAD_PART_SIZE = 5 * 1024 * 1024


CONFIG_SECURE_TTL = "ckanext.cloudstorage.secure_ttl"
DEFAULT_SECURE_TTL = 3600


def config_secure_ttl():
    return p.toolkit.asint(p.toolkit.config.get(
        CONFIG_SECURE_TTL, DEFAULT_SECURE_TTL
    ))


def _get_underlying_file(wrapper):
    if isinstance(wrapper, FlaskFileStorage):
        return wrapper.stream
    return wrapper.file


def _md5sum(fobj):
    block_count = 0
    block = True
    md5string = b""
    while block:
        block = fobj.read(AWS_UPLOAD_PART_SIZE)
        if block:
            block_count += 1
            hash_obj = hashlib.md5()
            hash_obj.update(block)
            md5string = md5string + binascii.unhexlify(hash_obj.hexdigest())
        else:
            break
    fobj.seek(0, os.SEEK_SET)
    hash_obj = hashlib.md5()
    hash_obj.update(md5string)
    return hash_obj.hexdigest() + "-" + str(block_count)


class CloudStorage(object):
    def __init__(self):
       

        # self.driver = get_driver(getattr(Provider, self.driver_name))(
        #     **self.driver_options
          
        # )
       
        endpoint = config["ckanext.cloudstorage.endpoint"]
        container_name = config["ckanext.cloudstorage.container_name"]
        key_secret = self.driver_options
        container = oss2.Bucket(oss2.Auth(key_secret['key'], key_secret['secret']), endpoint, container_name)
        log.info("container is {}".format(container))
        self._container = container
        self.driver= None

    def path_from_filename(self, rid, filename):
        raise NotImplementedError


    @property
    def container(self):
        """
        Return the currently configured libcloud container.
        """
        # if self._container is None:
        #     self._container = self.driver.get_container(
        #         container_name=self.container_name
        #     )
        #     log.info("container is {}".format(self._container))

            # self._container.extra={"location":"oss-cn-hangzhou"}
    
        return self._container

    @property
    def driver_options(self):
        """
        A dictionary of options ckanext-cloudstorage has been configured to
        pass to the apache-libcloud driver.
        """
        return literal_eval(config["ckanext.cloudstorage.driver_options"])

    @property
    def driver_name(self):
        """
        The name of the driver (ex: AZURE_BLOBS, S3) that ckanext-cloudstorage
        is configured to use.


        .. note::

            This value is used to lookup the apache-libcloud driver to use
            based on the Provider enum.
        """
        return config["ckanext.cloudstorage.driver"]

    @property
    def container_name(self):
        """
        The name of the container (also called buckets on some providers)
        ckanext-cloudstorage is configured to use.
        """
        # return config["ckanext.cloudstorage.container_name"]
        return "china-vo-gw"

    @property
    def use_secure_urls(self):
        """
        `True` if ckanext-cloudstroage is configured to generate secure
        one-time URLs to resources, `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get("ckanext.cloudstorage.use_secure_urls", False)
        )

    @property
    def leave_files(self):
        """
        `True` if ckanext-cloudstorage is configured to leave files on the
        provider instead of removing them when a resource/package is deleted,
        otherwise `False`.
        """
        return p.toolkit.asbool(
            config.get("ckanext.cloudstorage.leave_files", False)
        )

    @property
    def can_use_advanced_azure(self):
        """
        `True` if the `azure-storage` module is installed and
        ckanext-cloudstorage has been configured to use Azure, otherwise
        `False`.
        """
        # Are we even using Azure?
        if self.driver_name == "AZURE_BLOBS":
            try:
                # Yes? Is the azure-storage package available?
                from azure import storage

                # Shut the linter up.
                assert storage
                return True
            except ImportError:
                pass

        return False

    @property
    def can_use_advanced_aws(self):
        """
        `True` if the `boto` module is installed and ckanext-cloudstorage has
        been configured to use Amazon S3, otherwise `False`.
        """
        # Are we even using AWS?
        if "S3" in self.driver_name:
            if "host" not in self.driver_options:
                # newer libcloud versions(must-use for python3)
                # requires host for secure URLs
                return False
            try:
                # Yes? Is the boto package available?
                import boto

                # Shut the linter up.
                assert boto
                return True
            except ImportError:
                pass

        return False

    @property
    def guess_mimetype(self):
        """
        `True` if ckanext-cloudstorage is configured to guess mime types,
        `False` otherwise.
        """
        return p.toolkit.asbool(
            config.get("ckanext.cloudstorage.guess_mimetype", False)
        )


class ResourceCloudStorage(CloudStorage):
    def __init__(self, resource):
        """
        Support for uploading resources to any storage provider
        implemented by the apache-libcloud library.

        :param resource: The resource dict.
        """
        super(ResourceCloudStorage, self).__init__()

        self.filename = None
        self.old_filename = None
        self.file = None
        self.resource = resource

        upload_field_storage = resource.pop("upload", None)
        self._clear = resource.pop("clear_upload", None)
        multipart_name = resource.pop("multipart_name", None)

        # Check to see if a file has been provided
        if (
            isinstance(upload_field_storage, (ALLOWED_UPLOAD_TYPES))
            and upload_field_storage.filename
        ):
            self.filename = munge.munge_filename(upload_field_storage.filename)
            self.file_upload = _get_underlying_file(upload_field_storage)
            resource["url"] = self.filename
            resource["url_type"] = "upload"
            resource["last_modified"] = datetime.utcnow()
        elif multipart_name and self.can_use_advanced_aws:
            # This means that file was successfully uploaded and stored
            # at cloud.
            # Currently implemented just AWS version
            resource["url"] = munge.munge_filename(multipart_name)
            resource["url_type"] = "upload"
            resource["last_modified"] = datetime.utcnow()
        elif self._clear and resource.get("id"):
            # Apparently, this is a created-but-not-commited resource whose
            # file upload has been canceled. We're copying the behaviour of
            # ckaenxt-s3filestore here.
            old_resource = model.Session.query(model.Resource).get(
                resource["id"]
            )

            self.old_filename = old_resource.url
            resource["url_type"] = ""

    def path_from_filename(self, rid, filename):
        """
        Returns a bucket path for the given resource_id and filename.

        :param rid: The resource ID.
        :param filename: The unmunged resource filename.
        """
        return os.path.join("resources", rid, munge.munge_filename(filename))
    
    #主要修改这一部分
    def upload(self, id, max_size=10):
        """
        Complete the file upload, or clear an existing upload.

        :param id: The resource_id.
        :param max_size: Ignored.
        """
        if self.filename:
            log.info("filename is {}".format(self.filename))

            try:
                file_upload = self.file_upload
                log.info("file_upload is {}".format(file_upload))

                # check if already uploaded
                object_name = self.path_from_filename(id, self.filename)
                log.info("object_name is {}".format(object_name))
                try:
                    # check if the file exist
                    # cloud_object = self.container.get_object(object_name)
                    # log.info("\t Object found: %s", object_name)
                    if os.path.isfile(object_name):
                        file_size = os.path.getsize(object_name)
                    else:
                        self.file_upload.seek(0, os.SEEK_END)
                        file_size = self.file_upload.tell()
                        self.file_upload.seek(0, os.SEEK_SET)


                    log.debug(
                        "\t - File size %s: %s", self.filename, file_size
                    )
                    
                    #  check the file size and md5 
                    # if file_size == int(cloud_object.size):
                    #     log.debug(
                    #         "\t Size fits, checking hash %s: %s",
                    #         object_name,
                    #         cloud_object.hash,
                    #     )
                    #     hash_file = hashlib.md5(
                    #         self.file_upload.read()
                    #     ).hexdigest()
                    #     self.file_upload.seek(0, os.SEEK_SET)
                    #     log.debug(
                    #         "\t - File hash %s: %s",
                    #         self.filename,
                    #         hash_file,
                    #     )
                    #     # basic hash
                    #     if hash_file == cloud_object.hash:
                    #         log.debug(
                    #             "\t => File found, matching hash, skipping"
                    #             " upload"
                    #         )
                    #         return
                    #     # multipart hash
                    #     multi_hash_file = _md5sum(self.file_upload)
                    #     log.debug(
                    #         "\t - File multi hash %s: %s",
                    #         self.filename,
                    #         multi_hash_file,
                    #     )
                    #     if multi_hash_file == cloud_object.hash:
                    #         log.debug(
                    #             "\t => File found, matching hash, skipping"
                    #             " upload"
                    #         )
                    #         return
                    log.debug(
                        "\t Resource found in the cloud but outdated,"
                        " uploading"
                    )
                except ObjectDoesNotExistError:
                    log.debug(
                        "\t Resource not found in the cloud, uploading"
                    )

                # If it's temporary file, we'd better convert it
                # into FileIO. Otherwise libcloud will iterate
                # over lines, not over chunks and it will really
                # slow down the process for files that consist of
                # millions of short linew
                if isinstance(file_upload, tempfile.SpooledTemporaryFile):
                    file_upload.rollover()
                    try:
                        # extract underlying file
                        file_upload_iter = file_upload._file.detach()
                      

                    except AttributeError:
                        # It's python2
                        file_upload_iter = file_upload._file
                        
                else:
                    file_upload_iter = iter(file_upload)
          

                # log.info("container is {}".format(self.container))
                # self.driver.upload_object_via_stream(
                #     iterator=file_upload_iter, container = self.container, object_name=object_name
                # )
                # 也可以直接调用分片上传接口。
                # 首先可以用帮助函数设定分片大小，设我们期望的分片大小为128KB
                total_size = file_size
                part_size = oss2.determine_part_size(total_size, preferred_size=128 * 1024)

                # 初始化分片上传，得到Upload ID。接下来的接口都要用到这个Upload ID。
                key = object_name
                upload_id = self.container.init_multipart_upload(key).upload_id
                # 逐个上传分片
                # 其中oss2.SizedFileAdapter()把fileobj转换为一个新的文件对象，新的文件对象可读的长度等于size_to_upload
                fileobj =file_upload_iter
                parts = []
                part_number = 1
                offset = 0
                while offset < total_size:
                    size_to_upload = min(part_size, total_size - offset)
                    result = self.container.upload_part(key, upload_id, part_number,
                                                oss2.SizedFileAdapter(fileobj, size_to_upload))
                    parts.append(oss2.models.PartInfo(part_number, result.etag, size = size_to_upload, part_crc = result.crc))

                    offset += size_to_upload
                    part_number += 1

                # 完成分片上传
                self.container.complete_multipart_upload(key, upload_id, parts)

                # 验证一下
                # with self.file_upload.read() as fileobj:
                # assert self.container.get_object(key).read() == fileobj.read()


                # os.remove(self.filename)
                log.debug(
                    "\t => UPLOADED %s: %s", self.filename, object_name
                )
            except (ValueError, types.InvalidCredsError) as err:
                log.error(traceback.format_exc())
                raise err

        elif self._clear and self.old_filename and not self.leave_files:
            # This is only set when a previously-uploaded file is replace
            # by a link. We want to delete the previously-uploaded file.
            try:
                log.debug("id is  {}, self.old_filename is {}".format(id, self.old_filename))
                log.debug("path form file name is {}".format(self.path_from_filename(id, self.old_filename)))
                self.container.delete_object(
                    
                        self.path_from_filename(id, self.old_filename)
                    
                )
            except NoSuchKey:
                # It's possible for the object to have already been deleted, or
                # for it to not yet exist in a committed state due to an
                # outstanding lease.
                return

    def get_url_from_filename(self, rid, filename, content_type=None):
        path = self.path_from_filename(rid, filename)

        return self.get_url_by_path(path, content_type)

    def get_url_by_path(self, path, content_type=None):
        """
        Retrieve a publically accessible URL for the given path

        .. note::

            Works for Azure and any libcloud driver that implements
            support for get_object_cdn_url (ex: AWS S3).

        :param path: The resource name on cloud.
        :param content_type: Optionally a Content-Type header.

        :returns: Externally accessible URL or None.
        """
        # If advanced azure features are enabled, generate a temporary
        # shared access link instead of simply redirecting to the file.
   

        # Find the object for the given key.
        try:
            expiration_time = 3600
            url = self.container.sign_url('GET',path,expiration_time)
        except ObjectDoesNotExistError:
            return "404"
        if url is not None:
            return url

      

    @property
    def package(self):
        return model.Package.get(self.resource["package_id"])
