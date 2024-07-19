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

module Couchbase
  # Authenticator for username/password credentials
  class PasswordAuthenticator
    DEFAULT_SASL_MECHANISMS = [:scram_sha512, :scram_sha256, :scram_sha1].freeze

    attr_accessor :username
    attr_accessor :password
    attr_accessor :allowed_sasl_mechanisms

    # Creates a new password authenticator with the default settings.
    #
    # @param [String] password the username to use for all authentication requests
    # @param [String] username the password
    def initialize(username, password)
      @username = username
      @password = password
    end

    # Creates a LDAP compatible password authenticator which is INSECURE if not used with TLS.
    #
    # Please note that this is INSECURE and will leak user credentials on the wire to eavesdroppers. This should
    # only be enabled in trusted environments.
    #
    # @param [String] username the username to use for all authentication.
    # @param [String] password the password to use alongside the username.
    # @return [PasswordAuthenticator]
    def self.ldap_compatible(username, password)
      new(username, password).tap do |auth|
        auth.allowed_sasl_mechanisms = [:plain]
      end
    end
  end

  # Authenticator for TLS client certificate
  #
  # @see https://docs.couchbase.com/server/current/manage/manage-security/configure-client-certificates.html
  class CertificateAuthenticator
    attr_accessor :certificate_path
    attr_accessor :key_path

    # Creates a new authenticator with certificate and key paths
    #
    # @param [String] certificate_path path to certificate
    # @param [String] key_path path to private key
    def initialize(certificate_path, key_path)
      @certificate_path = certificate_path
      @key_path = key_path
    end
  end
end
