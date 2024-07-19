# frozen_string_literal: true

#  Copyright 2020-2021 Couchbase, Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

require "couchbase"
include Couchbase # rubocop:disable Style/MixinUsage for brevity

options = Cluster::ClusterOptions.new
# initializes {PasswordAuthenticator} internally
options.authenticate("Administrator", "password")
Cluster.connect("couchbase://localhost", options)

# the same as above, but more explicit
options.authenticator = PasswordAuthenticator.new("Administrator", "password")
Cluster.connect("couchbase://localhost", options)

# this variant must use used when the cluster configured to use LDAP,
# but for some reason TLS authentication cannot be used. In this case the
# library will be told explicitly to downgrade SASL to use PLAIN mechanism,
# because LDAP service need password value.
options.authenticator = PasswordAuthenticator.ldap_compatible("Administrator", "password")
Cluster.connect("couchbase://localhost", options)

# shorter version, more useful for interactive sessions
Cluster.connect("couchbase://localhost", "Administrator", "password")

# authentication with TLS client certificate
# @see https://docs.couchbase.com/server/current/manage/manage-security/configure-client-certificates.html
options.authenticator = CertificateAuthenticator.new("/tmp/certificate.pem", "/tmp/private.key")
Cluster.connect("couchbases://localhost?trust_certificate=/tmp/ca.pem", options)
