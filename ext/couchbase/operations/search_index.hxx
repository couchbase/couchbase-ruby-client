#pragma once

namespace couchbase::operations
{
struct search_index {
    std::string uuid;
    std::string name;
    std::string type;
    std::string params_json;

    std::string source_uuid;
    std::string source_name;
    std::string source_type;
    std::string source_params_json;

    std::string plan_params_json;
};

} // namespace couchbase::operations

namespace tao::json
{
template<>
struct traits<couchbase::operations::search_index> {
    template<template<typename...> class Traits>
    static couchbase::operations::search_index as(const tao::json::basic_value<Traits>& v)
    {
        couchbase::operations::search_index result;
        result.uuid = v.at("uuid").get_string();
        result.name = v.at("name").get_string();
        result.type = v.at("type").get_string();
        {
            const auto* params = v.find("params");
            if (params != nullptr && params->is_object()) {
                result.params_json = tao::json::to_string(*params);
            }
        }
        if (v.find("sourceUUID") != nullptr) {
            result.source_uuid = v.at("sourceUUID").get_string();
        }
        if (v.find("sourceName") != nullptr) {
            result.source_name = v.at("sourceName").get_string();
        }
        if (v.find("sourceType") != nullptr) {
            result.source_type = v.at("sourceType").get_string();
        }
        {
            const auto* params = v.find("sourceParams");
            if (params != nullptr && params->is_object()) {
                result.source_params_json = tao::json::to_string(*params);
            }
        }
        {
            const auto* params = v.find("planParams");
            if (params != nullptr && params->is_object()) {
                result.plan_params_json = tao::json::to_string(*params);
            }
        }
        return result;
    }
};
} // namespace tao::json
