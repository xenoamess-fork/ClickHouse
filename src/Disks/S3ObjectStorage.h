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
    uint64_t min_bytes_for_seek;
    int32_t list_object_keys_size;
    int32_t objects_chunk_size_to_delete;
};


class S3ObjectStorage : public IObjectStorage
{
public:
    bool exists(const std::string & path) const override;

    std::unique_ptr<ReadBufferFromFileBase> readObjects( /// NOLINT
        const std::string & common_path_prefix,
        const BlobsPathToSize & blobs_to_read,
        const ReadSettings & read_settings = ReadSettings{},
        std::optional<size_t> read_hint = {},
        std::optional<size_t> file_size = {}) const override;

    /// Open the file for write and return WriteBufferFromFileBase object.
    std::unique_ptr<WriteBufferFromFileBase> writeObject( /// NOLINT
        const std::string & path,
        std::optional<ObjectAttributes> attributes = {},
        size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode = WriteMode::Rewrite,
        const WriteSettings & write_settings = {}) override;

private:

    Aws::S3::Model::HeadObjectOutcome requestObjectHeadData(const std::string & key) const;

    std::string bucket;
    std::shared_ptr<Aws::S3::S3Client> client;
    S3ObjectStorageSettings s3_settings;
};

}

#endif
