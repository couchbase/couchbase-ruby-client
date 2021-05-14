#pragma once

namespace couchbase::operations
{
struct bucket_settings {
    enum class bucket_type { unknown, couchbase, memcached, ephemeral };
    enum class compression_mode { unknown, off, active, passive };
    enum class eviction_policy {
        unknown,

        /**
         * During ejection, everything (including key, metadata, and value) will be ejected.
         *
         * Full Ejection reduces the memory overhead requirement, at the cost of performance.
         *
         * This value is only valid for buckets of type COUCHBASE.
         */
        full,

        /**
         * During ejection, only the value will be ejected (key and metadata will remain in memory).
         *
         * Value Ejection needs more system memory, but provides better performance than Full Ejection.
         *
         * This value is only valid for buckets of type COUCHBASE.
         */
        value_only,

        /**
         * Couchbase Server keeps all data until explicitly deleted, but will reject
         * any new data if you reach the quota (dedicated memory) you set for your bucket.
         *
         * This value is only valid for buckets of type EPHEMERAL.
         */
        no_eviction,

        /**
         * When the memory quota is reached, Couchbase Server ejects data that has not been used recently.
         *
         * This value is only valid for buckets of type EPHEMERAL.
         */
        not_recently_used,
    };
    enum class conflict_resolution_type { unknown, timestamp, sequence_number };
    struct node {
        std::string hostname;
        std::string status;
        std::string version;
        std::vector<std::string> services;
        std::map<std::string, std::uint16_t> ports;
    };

    std::string name;
    std::string uuid;
    bucket_type bucket_type{ bucket_type::unknown };
    std::uint64_t ram_quota_mb{ 100 };
    std::uint32_t max_expiry{ 0 };
    compression_mode compression_mode{ compression_mode::unknown };
    std::optional<protocol::durability_level> minimum_durability_level{};
    std::uint32_t num_replicas{ 1 };
    bool replica_indexes{ false };
    bool flush_enabled{ false };
    eviction_policy eviction_policy{ eviction_policy::unknown };
    conflict_resolution_type conflict_resolution_type{ conflict_resolution_type::unknown };
    std::vector<std::string> capabilities{};
    std::vector<node> nodes{};
};
} // namespace couchbase::operations

namespace tao::json
{
template<>
struct traits<couchbase::operations::bucket_settings> {
    template<template<typename...> class Traits>
    static couchbase::operations::bucket_settings as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::bucket_settings result;
        result.name = v.at("name").get_string();
        result.uuid = v.at("uuid").get_string();
        const static std::uint64_t megabyte = 1024 * 1024;
        result.ram_quota_mb = v.at("quota").at("rawRAM").get_unsigned() / megabyte;
        result.max_expiry = v.at("maxTTL").template as<std::uint32_t>();
        result.num_replicas = v.at("replicaNumber").template as<std::uint32_t>();

        if (auto& str = v.at("bucketType").get_string(); str == "couchbase" || str == "membase") {
            result.bucket_type = couchbase::operations::bucket_settings::bucket_type::couchbase;
        } else if (str == "ephemeral") {
            result.bucket_type = couchbase::operations::bucket_settings::bucket_type::ephemeral;
        } else if (str == "memcached") {
            result.bucket_type = couchbase::operations::bucket_settings::bucket_type::memcached;
        }

        if (auto& str = v.at("compressionMode").get_string(); str == "active") {
            result.compression_mode = couchbase::operations::bucket_settings::compression_mode::active;
        } else if (str == "passive") {
            result.compression_mode = couchbase::operations::bucket_settings::compression_mode::passive;
        } else if (str == "off") {
            result.compression_mode = couchbase::operations::bucket_settings::compression_mode::off;
        }

        if (auto& str = v.at("evictionPolicy").get_string(); str == "valueOnly") {
            result.eviction_policy = couchbase::operations::bucket_settings::eviction_policy::value_only;
        } else if (str == "fullEviction") {
            result.eviction_policy = couchbase::operations::bucket_settings::eviction_policy::full;
        } else if (str == "noEviction") {
            result.eviction_policy = couchbase::operations::bucket_settings::eviction_policy::no_eviction;
        } else if (str == "nruEviction") {
            result.eviction_policy = couchbase::operations::bucket_settings::eviction_policy::not_recently_used;
        }

        if (auto* min_level = v.find("durabilityMinLevel"); min_level != nullptr) {
            auto& str = min_level->get_string();
            if (str == "none") {
                result.minimum_durability_level = couchbase::protocol::durability_level::none;
            } else if (str == "majority") {
                result.minimum_durability_level = couchbase::protocol::durability_level::majority;
            } else if (str == "majorityAndPersistActive") {
                result.minimum_durability_level = couchbase::protocol::durability_level::majority_and_persist_to_active;
            } else if (str == "persistToMajority") {
                result.minimum_durability_level = couchbase::protocol::durability_level::persist_to_majority;
            }
        }

        if (auto& str = v.at("conflictResolutionType").get_string(); str == "lww") {
            result.conflict_resolution_type = couchbase::operations::bucket_settings::conflict_resolution_type::timestamp;
        } else if (str == "seqno") {
            result.conflict_resolution_type = couchbase::operations::bucket_settings::conflict_resolution_type::sequence_number;
        }

        result.flush_enabled = v.at("controllers").find("flush") != nullptr;
        const auto ri = v.find("replicaIndex");
        if (ri) {
            result.replica_indexes = ri->get_boolean();
        }
        const auto caps = v.find("bucketCapabilities");
        if (caps != nullptr) {
            for (auto& cap : caps->get_array()) {
                result.capabilities.emplace_back(cap.get_string());
            }
        }
        for (auto& n : v.at("nodes").get_array()) {
            couchbase::operations::bucket_settings::node node;
            node.status = n.at("status").get_string();
            node.hostname = n.at("hostname").get_string();
            node.version = n.at("version").get_string();
            for (auto& s : n.at("services").get_array()) {
                node.services.emplace_back(s.get_string());
            }
            for (auto& p : n.at("ports").get_object()) {
                node.ports.emplace(p.first, p.second.template as<std::uint16_t>());
            }
            result.nodes.emplace_back(node);
        }
        return result;
    }
};
} // namespace tao::json
