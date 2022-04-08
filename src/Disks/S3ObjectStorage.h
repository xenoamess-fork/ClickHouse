#pragma once
#include <Disks/IObjectStorage.h>

#include <Common/config.h>

#if USE_AWS_S3

#include <memory>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Result.h>


namespace DB
{

struct S3ObjectStorageSettings
{
    uint64_t s3_max_single_read_retries;
    uint64_t s3_min_upload_part_size;
    uint64_t s3_upload_part_size_multiply_factor;
    uint64_t s3_upload_part_size_multiply_parts_count_threshold;
    uint64_t s3_max_single_part_upload_size;
};


class S3ObjectStorage : public IObjectStorage
{
public:
    bool exists(const std::string & path) const override;

    /// Open the file for read and return ReadBufferFromFileBase object.
    std::unique_ptr<ReadBufferFromFileBase> readObject( /// NOLINT
        const std::string & path,
        const ReadSettings & settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

private:

    Aws::S3::Model::HeadObjectOutcome requestObjectHeadData(const std::string & key) const;

    std::string bucket;
    std::shared_ptr<Aws::S3::S3Client> client;
    FileCachePtr cache;
};

}

#endif
