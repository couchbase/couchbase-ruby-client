/*
 *     Copyright 2018 Couchbase, Inc.
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

#include <cbsasl/plain/plain.h>

namespace couchbase::sasl::mechanism::plain
{

std::pair<error, std::string_view>
ClientBackend::start()
{
    auto usernm = usernameCallback();
    auto passwd = passwordCallback();

    buffer.push_back(0);
    std::copy(usernm.begin(), usernm.end(), std::back_inserter(buffer));
    buffer.push_back(0);
    std::copy(passwd.begin(), passwd.end(), std::back_inserter(buffer));

    return { error::OK, { buffer.data(), buffer.size() } };
}

} // namespace couchbase::sasl::mechanism::plain
