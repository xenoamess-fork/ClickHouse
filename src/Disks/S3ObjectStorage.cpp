#include <Disks/S3ObjectStorage.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/RemoteDisksCommon.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/AsynchronousReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ReadIndirectBufferFromRemoteFS.h>
#include <Disks/IO/WriteIndirectBufferFromRemoteFS.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <IO/WriteBufferFromS3.h>
#include <IO/SeekAvoidingReadBuffer.h>
#include <Interpreters/threadPoolCallbackRunner.h>

#include <Common/FileCache.h>
#include <Common/FileCacheFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int S3_ERROR;
    extern const int FILE_ALREADY_EXISTS;
    extern const int UNKNOWN_FORMAT;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

template <typename Result, typename Error>
void throwIfError(Aws::Utils::Outcome<Result, Error> & response)
{
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw Exception(std::to_string(static_cast<int>(err.GetErrorType())) + ": " + err.GetMessage(), ErrorCodes::S3_ERROR);
    }
}

template <typename Result, typename Error>
void throwIfError(const Aws::Utils::Outcome<Result, Error> & response)
{
    if (!response.IsSuccess())
    {
        const auto & err = response.GetError();
        throw Exception(err.GetMessage(), static_cast<int>(err.GetErrorType()));
    }
}

}

Aws::S3::Model::HeadObjectOutcome S3ObjectStorage::requestObjectHeadData(const std::string & key) const
{
    Aws::S3::Model::HeadObjectRequest request;
    request.SetBucket(bucket);
    request.SetKey(key);

    return client->HeadObject(request);
}

bool S3ObjectStorage::exists(const std::string & path) const
{
    auto object_head = requestObjectHeadData(path);
    if (!object_head.IsSuccess())
    {
        if (object_head.GetError().GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY)
            return false;
    }
    return true;
}

std::unique_ptr<ReadBufferFromFileBase> S3ObjectStorage::readObjects( /// NOLINT
    const std::string & common_path_prefix,
    const BlobsPathToSize & blobs_to_read,
    const ReadSettings & read_settings,
    std::optional<size_t>,
    std::optional<size_t>) const
{

   ReadSettings disk_read_settings{read_settings};
   if (cache)
   {
       if (IFileCache::isReadOnly())
           disk_read_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = true;

       disk_read_settings.remote_fs_cache = cache;
   }

   auto s3_impl = std::make_unique<ReadBufferFromS3Gather>(
       client, bucket, common_path_prefix, blobs_to_read,
       s3_settings.s3_max_single_read_retries, disk_read_settings);

   if (read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
   {
       auto reader = getThreadPoolReader();
       return std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(reader, disk_read_settings, std::move(s3_impl));
   }
   else
   {
       auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(s3_impl));
       return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), s3_settings.min_bytes_for_seek);
   }
}

std::unique_ptr<WriteBufferFromFileBase> S3ObjectStorage::writeObject(
    const std::string & path,
    std::optional<ObjectAttributes> attributes,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & write_settings)
{
    bool cache_on_write = cache
        && fs::path(path).extension() != ".tmp"
        && write_settings.enable_filesystem_cache_on_write_operations
        && FileCacheFactory::instance().getSettings(getCacheBasePath()).cache_on_write_operations;

    auto s3_buffer = std::make_unique<WriteBufferFromS3>(
        client,
        bucket,
        path,
        s3_settings.s3_min_upload_part_size,
        s3_settings.s3_upload_part_size_multiply_factor,
        s3_settings.s3_upload_part_size_multiply_parts_count_threshold,
        s3_settings.s3_max_single_part_upload_size,
        attributes ? *attributes : {},
        buf_size, threadPoolCallbackRunner(getThreadPoolWriter()),
        blob_name, cache_on_write ? cache : nullptr);

}



}
