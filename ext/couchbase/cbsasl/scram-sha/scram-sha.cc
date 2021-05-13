/* -*- Mode: C++; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */
/*
 *     Copyright 2016 Couchbase, Inc.
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

#include <spdlog/spdlog.h>

#include <platform/base64.h>
#include <platform/random.h>
#include <platform/string_hex.h>

#include <cbcrypto/cbcrypto.h>

#include <cbsasl/scram-sha/scram-sha.h>
#include <cbsasl/scram-sha/stringutils.h>

#include <cstring>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>

namespace couchbase::sasl::mechanism::scram
{

using AttributeMap = std::map<char, std::string>;

/**
 * Decode the attribute list into a set. The attribute list looks like:
 * "k=value,y=value" etc
 *
 * @param list the list to parse
 * @param attributes where to store the attributes
 * @return true if success, false otherwise
 */
static bool
decodeAttributeList(const std::string& list, AttributeMap& attributes)
{
    size_t pos = 0;

    spdlog::trace("decoding attribute list [{}]", list);

    while (pos < list.length()) {
        auto equal = list.find('=', pos);
        if (equal == std::string::npos) {
            // syntax error!!
            spdlog::error("decode attribute list [{}] failed: no '='", list);
            return false;
        }

        if ((equal - pos) != 1) {
            spdlog::error("decode attribute list [{}] failed: no key is multichar", list);
            return false;
        }

        char key = list.at(pos);
        pos = equal + 1;

        // Make sure we haven't seen this key before..
        if (attributes.find(key) != attributes.end()) {
            spdlog::error("decode attribute list [{}] failed: key [{}] is multichar", list, key);
            return false;
        }

        auto comma = list.find(',', pos);
        if (comma == std::string::npos) {
            attributes.insert(std::pair(key, list.substr(pos)));
            pos = list.length();
        } else {
            attributes.insert(std::pair(key, list.substr(pos, comma - pos)));
            pos = comma + 1;
        }
    }

    return true;
}

/********************************************************************
 * Common API
 *******************************************************************/
std::string
ScramShaBackend::getAuthMessage()
{
    if (client_first_message_bare.empty()) {
        throw std::logic_error("can't call getAuthMessage without client_first_message_bare "
                               "is set");
    }
    if (server_first_message.empty()) {
        throw std::logic_error("can't call getAuthMessage without server_first_message is "
                               "set");
    }
    if (client_final_message_without_proof.empty()) {
        throw std::logic_error("can't call getAuthMessage without "
                               "client_final_message_without_proof is set");
    }
    return client_first_message_bare + "," + server_first_message + "," + client_final_message_without_proof;
}

void
ScramShaBackend::addAttribute(std::ostream& out, char key, const std::string& value, bool more)
{
    out << key << '=';

    switch (key) {
        case 'n': // username ..
            out << encode_username(sasl_prep(value));
            break;

        case 'r': // client nonce.. printable characters
            for (const auto& c : value) {
                if (c == ',' || (isprint(c) == 0)) {
                    throw std::invalid_argument("ScramShaBackend::addAttribute: Invalid character in "
                                                "client nonce");
                }
            }
            out << value;
            break;

        case 'c': // base64 encoded GS2 header and channel binding data
        case 's': // base64 encoded salt
        case 'p': // base64 encoded client proof
        case 'v': // base64 encoded server signature
            out << couchbase::base64::encode(value);
            break;

        case 'i': // iterator count
            // validate that it is an integer value
            try {
                std::stoi(value);
            } catch (...) {
                throw std::invalid_argument("ScramShaBackend::addAttribute: "
                                            "Iteration count must be a numeric"
                                            " value");
            }
            out << value;
            break;

        case 'e':
            for (const auto& c : value) {
                if (c == ',' || (isprint(c) == 0)) {
                    throw std::invalid_argument("ScramShaBackend::addAttribute: Invalid character in "
                                                "error message");
                }
            }
            out << value;
            break;

        default:
            throw std::invalid_argument("ScramShaBackend::addAttribute:"
                                        " Invalid key");
    }

    if (more) {
        out << ',';
    }
}

void
ScramShaBackend::addAttribute(std::ostream& out, char key, int value, bool more)
{
    out << key << '=';

    std::string base64_encoded;

    switch (key) {
        case 'n': // username ..
        case 'r': // client nonce.. printable characters
        case 'c': // base64 encoded GS2 header and channel binding data
        case 's': // base64 encoded salt
        case 'p': // base64 encoded client proof
        case 'v': // base64 encoded server signature
        case 'e': // error message
            throw std::invalid_argument("ScramShaBackend::addAttribute:"
                                        " Invalid value (should not be int)");

        case 'i': // iterator count
            out << value;
            break;

        default:
            throw std::invalid_argument("ScramShaBackend::addAttribute:"
                                        " Invalid key");
    }

    if (more) {
        out << ',';
    }
}

/**
 * Generate the Server Signature. It is computed as:
 *
 * SaltedPassword  := Hi(Normalize(password), salt, i)
 * ServerKey       := HMAC(SaltedPassword, "Server Key")
 * ServerSignature := HMAC(ServerKey, AuthMessage)
 */
std::string
ScramShaBackend::getServerSignature()
{
    auto serverKey = couchbase::crypto::HMAC(algorithm, getSaltedPassword(), "Server Key");

    return couchbase::crypto::HMAC(algorithm, serverKey, getAuthMessage());
}

/**
 * Generate the Client Proof. It is computed as:
 *
 * SaltedPassword  := Hi(Normalize(password), salt, i)
 * ClientKey       := HMAC(SaltedPassword, "Client Key")
 * StoredKey       := H(ClientKey)
 * AuthMessage     := client-first-message-bare + "," +
 *                    server-first-message + "," +
 *                    client-final-message-without-proof
 * ClientSignature := HMAC(StoredKey, AuthMessage)
 * ClientProof     := ClientKey XOR ClientSignature
 */
std::string
ScramShaBackend::getClientProof()
{
    auto clientKey = couchbase::crypto::HMAC(algorithm, getSaltedPassword(), "Client Key");
    auto storedKey = couchbase::crypto::digest(algorithm, clientKey);
    std::string authMessage = getAuthMessage();
    auto clientSignature = couchbase::crypto::HMAC(algorithm, storedKey, authMessage);

    // Client Proof is ClientKey XOR ClientSignature
    const auto* ck = clientKey.data();
    const auto* cs = clientSignature.data();

    std::string proof;
    proof.resize(clientKey.size());

    auto total = proof.size();
    for (std::size_t ii = 0; ii < total; ++ii) {
        proof[ii] = ck[ii] ^ cs[ii];
    }

    return proof;
}

ClientBackend::ClientBackend(GetUsernameCallback& user_cb,
                             GetPasswordCallback& password_cb,
                             ClientContext& ctx,
                             Mechanism mech,
                             couchbase::crypto::Algorithm algo)
  : MechanismBackend(user_cb, password_cb, ctx)
  , ScramShaBackend(mech, algo)
{
    couchbase::RandomGenerator randomGenerator;

    std::array<char, 8> nonce{};
    if (!randomGenerator.getBytes(nonce.data(), nonce.size())) {
        spdlog::error("failed to generate server nonce");
        throw std::bad_alloc();
    }

    clientNonce = couchbase::to_hex({ nonce.data(), nonce.size() });
}

std::pair<error, std::string_view>
ClientBackend::start()
{
    std::stringstream out;
    out << "n,,";
    addAttribute(out, 'n', usernameCallback(), true);
    addAttribute(out, 'r', clientNonce, false);

    client_first_message = out.str();
    client_first_message_bare = client_first_message.substr(3); // skip n,,

    return { error::OK, client_first_message };
}

std::pair<error, std::string_view>
ClientBackend::step(std::string_view input)
{
    if (input.empty()) {
        return { error::BAD_PARAM, {} };
    }

    if (server_first_message.empty()) {
        server_first_message.assign(input.data(), input.size());

        AttributeMap attributes;
        if (!decodeAttributeList(server_first_message, attributes)) {
            return { error::BAD_PARAM, {} };
        }

        for (const auto& attribute : attributes) {
            switch (attribute.first) {
                case 'r': // combined nonce
                    nonce_ = attribute.second;
                    break;
                case 's':
                    salt = couchbase::base64::decode(attribute.second);
                    break;
                case 'i':
                    try {
                        iterationCount = static_cast<unsigned int>(std::stoul(attribute.second));
                    } catch (...) {
                        return { error::BAD_PARAM, {} };
                    }
                    break;
                default:
                    return { error::BAD_PARAM, {} };
            }
        }

        if (attributes.find('r') == attributes.end() || attributes.find('s') == attributes.end() ||
            attributes.find('i') == attributes.end()) {
            spdlog::error("missing r/s/i in server message");
            return { error::BAD_PARAM, {} };
        }

        // I've got the SALT, lets generate the salted password
        if (!generateSaltedPassword(passwordCallback())) {
            spdlog::error("failed to generated salted password");
            return { error::FAIL, {} };
        }

        // Ok so we have salted hased password :D

        std::stringstream out;
        addAttribute(out, 'c', "n,,", true);
        addAttribute(out, 'r', nonce_, false);
        client_final_message_without_proof = out.str();
        out << ",";

        addAttribute(out, 'p', getClientProof(), false);

        client_final_message = out.str();

        return { error::CONTINUE, client_final_message };
    }
    server_final_message.assign(input.data(), input.size());

    AttributeMap attributes;
    if (!decodeAttributeList(server_final_message, attributes)) {
        spdlog::error("SCRAM: failed to decode server-final-message");
        return { error::BAD_PARAM, {} };
    }

    if (attributes.find('e') != attributes.end()) {
        spdlog::error("failed to authenticate: {}", attributes['e']);
        return { error::FAIL, {} };
    }

    if (attributes.find('v') == attributes.end()) {
        spdlog::error("syntax error server final message is missing 'v'");
        return { error::BAD_PARAM, {} };
    }

    auto encoded = couchbase::base64::encode(getServerSignature());
    if (encoded != attributes['v']) {
        spdlog::error("incorrect ServerKey received");
        return { error::FAIL, {} };
    }

    return { error::OK, {} };
}

bool
ClientBackend::generateSaltedPassword(const std::string& secret)
{
    try {
        saltedPassword = couchbase::crypto::PBKDF2_HMAC(algorithm, secret, salt, iterationCount);
        return true;
    } catch (...) {
        return false;
    }
}

} // namespace couchbase::sasl::mechanism::scram
