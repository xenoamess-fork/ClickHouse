#include <Disks/S3ObjectStorage.h>
#include <aws/s3/model/HeadObjectRequest.h>

#include <Common/FileCache.h>

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

std::unique_ptr<ReadBufferFromFileBase> S3ObjectStorage::readObject( /// NOLINT
    const std::string & path,
    const ReadSettings & settings,
    std::optional<size_t> read_hint,
    std::optional<size_t> file_size) const
{
   ReadSettings disk_read_settings{settings};
   if (cache)
   {
       if (IFileCache::isReadOnly())
           disk_read_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = true;

       disk_read_settings.remote_fs_cache = cache;
   }

   auto s3_impl = std::make_unique<ReadBufferFromS3Gather>(
       client, bucket, metadata,
       settings->s3_max_single_read_retries, disk_read_settings);

   if (read_settings.remote_fs_method == RemoteFSReadMethod::threadpool)
   {
       auto reader = getThreadPoolReader();
       return std::make_unique<AsynchronousReadIndirectBufferFromRemoteFS>(reader, disk_read_settings, std::move(s3_impl));
   }
   else
   {
       auto buf = std::make_unique<ReadIndirectBufferFromRemoteFS>(std::move(s3_impl));
       return std::make_unique<SeekAvoidingReadBuffer>(std::move(buf), settings->min_bytes_for_seek);
   }

}


}
