/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *   Copyright 2020-2021 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include <optional>
#include <sstream>
#include <string>

#include <operations/analytics_link_azure_blob_external.hxx>
#include <operations/analytics_link_couchbase_remote.hxx>
#include <operations/analytics_link_s3_external.hxx>

namespace couchbase::operations::analytics_link
{
template<typename analytics_link_type>
std::string
endpoint_from_link(const analytics_link_type& link)
{
    if (std::count(link.dataverse.begin(), link.dataverse.end(), '/') > 0) {
        return fmt::format("/analytics/link/{}/{}", utils::string_codec::v2::path_escape(link.dataverse), link.link_name);
    }
    return "/analytics/link";
}
} // namespace couchbase::operations::analytics_link
