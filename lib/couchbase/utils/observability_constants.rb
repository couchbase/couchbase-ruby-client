# frozen_string_literal: true

#  Copyright 2025-Present Couchbase, Inc.
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
  module Observability # rubocop:disable Metrics/ModuleLength
    OP_GET = "get"
    OP_GET_MULTI = "get_multi"
    OP_GET_AND_LOCK = "get_and_lock"
    OP_GET_AND_TOUCH = "get_and_touch"
    OP_GET_ALL_REPLICAS = "get_all_replicas"
    OP_GET_ANY_REPLICA = "get_any_replica"
    OP_GET_REPLICA = "get_replica"
    OP_EXISTS = "exists"
    OP_REPLACE = "replace"
    OP_UPSERT = "upsert"
    OP_UPSERT_MULTI = "upsert_multi"
    OP_REMOVE = "remove"
    OP_REMOVE_MULTI = "remove_multi"
    OP_INSERT = "insert"
    OP_TOUCH = "touch"
    OP_UNLOCK = "unlock"
    OP_LOOKUP_IN = "lookup_in"
    OP_LOOKUP_IN_ALL_REPLICAS = "lookup_all_replicas"
    OP_LOOKUP_IN_ANY_REPLICA = "lookup_any_replica"
    OP_LOOKUP_IN_REPLICA = "lookup_in_replica"
    OP_MUTATE_IN = "mutate_in"
    OP_SCAN = "scan"
    OP_INCREMENT = "increment"
    OP_DECREMENT = "decrement"
    OP_APPEND = "append"
    OP_PREPEND = "prepend"

    OP_QUERY = "query"
    OP_SEARCH_QUERY = "search"
    OP_ANALYTICS_QUERY = "analytics"
    OP_VIEW_QUERY = "views"

    OP_PING = "ping"
    OP_DIAGNOSTICS = "diagnostics"

    OP_BM_CREATE_BUCKET = "manager_buckets_create_bucket"
    OP_BM_DROP_BUCKET = "manager_buckets_drop_bucket"
    OP_BM_FLUSH_BUCKET = "manager_buckets_flush_bucket"
    OP_BM_GET_ALL_BUCKETS = "manager_buckets_get_all_buckets"
    OP_BM_GET_BUCKET = "manager_buckets_get_bucket"
    OP_BM_UPDATE_BUCKET = "manager_buckets_update_bucket"

    OP_CM_CREATE_COLLECTION = "manager_collections_create_collection"
    OP_CM_UPDATE_COLLECTION = "manager_collections_update_collection"
    OP_CM_DROP_COLLECTION = "manager_collections_drop_collection"
    OP_CM_CREATE_SCOPE = "manager_collections_create_scope"
    OP_CM_DROP_SCOPE = "manager_collections_drop_scope"
    OP_CM_GET_ALL_SCOPES = "manager_collections_get_all_scopes"
    OP_CM_GET_SCOPE = "manager_collections_get_scope"

    OP_QM_BUILD_DEFERRED_INDEXES = "manager_query_build_deferred_indexes"
    OP_QM_CREATE_INDEX = "manager_query_create_index"
    OP_QM_CREATE_PRIMARY_INDEX = "manager_query_create_primary_index"
    OP_QM_DROP_INDEX = "manager_query_drop_index"
    OP_QM_DROP_PRIMARY_INDEX = "manager_query_drop_primary_index"
    OP_QM_GET_ALL_INDEXES = "manager_query_get_all_indexes"
    OP_QM_WATCH_INDEXES = "manager_query_watch_indexes"

    OP_AM_CREATE_DATAVERSE = "manager_analytics_create_dataverse"
    OP_AM_DROP_DATAVERSE = "manager_analytics_drop_dataverse"
    OP_AM_CREATE_DATASET = "manager_analytics_create_dataset"
    OP_AM_DROP_DATASET = "manager_analytics_drop_dataset"
    OP_AM_GET_ALL_DATASETS = "manager_analytics_get_all_datasets"
    OP_AM_CREATE_INDEX = "manager_analytics_create_index"
    OP_AM_DROP_INDEX = "manager_analytics_drop_index"
    OP_AM_GET_ALL_INDEXES = "manager_analytics_get_all_indexes"
    OP_AM_CONNECT_LINK = "manager_analytics_connect_link"
    OP_AM_DISCONNECT_LINK = "manager_analytics_disconnect_link"
    OP_AM_GET_PENDING_MUTATIONS = "manager_analytics_get_pending_mutations"
    OP_AM_CREATE_LINK = "manager_analytics_create_link"
    OP_AM_REPLACE_LINK = "manager_analytics_replace_link"
    OP_AM_DROP_LINK = "manager_analytics_drop_link"
    OP_AM_GET_LINKS = "manager_analytics_get_links"

    OP_SM_GET_INDEX = "manager_search_get_index"
    OP_SM_GET_ALL_INDEXES = "manager_search_get_all_indexes"
    OP_SM_UPSERT_INDEX = "manager_search_upsert_index"
    OP_SM_DROP_INDEX = "manager_search_drop_index"
    OP_SM_GET_INDEXED_DOCUMENTS_COUNT = "manager_search_get_indexed_documents_count"
    OP_SM_GET_INDEX_STATS = "manager_search_get_index_stats"
    OP_SM_GET_STATS = "manager_search_get_stats"
    OP_SM_PAUSE_INGEST = "manager_search_pause_index"
    OP_SM_RESUME_INGEST = "manager_search_resume_index"
    OP_SM_ALLOW_QUERYING = "manager_search_allow_querying"
    OP_SM_DISALLOW_QUERYING = "manager_search_disallow_querying"
    OP_SM_FREEZE_PLAN = "manager_search_freeze_plan"
    OP_SM_UNFREEZE_PLAN = "manager_search_unfreeze_plan"
    OP_SM_ANALYZE_DOCUMENT = "manager_search_analyze_document"

    OP_UM_DROP_GROUP = "manager_users_drop_group"
    OP_UM_DROP_USER = "manager_users_drop_user"
    OP_UM_GET_ALL_GROUPS = "manager_users_get_all_groups"
    OP_UM_GET_ALL_USERS = "manager_users_get_all_users"
    OP_UM_GET_GROUP = "manager_users_get_group"
    OP_UM_GET_ROLES = "manager_users_get_roles"
    OP_UM_GET_USER = "manager_users_get_user"
    OP_UM_UPSERT_GROUP = "manager_users_upsert_group"
    OP_UM_UPSERT_USER = "manager_users_upsert_user"
    OP_UM_CHANGE_PASSWORD = "manager_users_change_password"

    OP_VM_GET_DESIGN_DOCUMENT = "manager_views_get_design_document"
    OP_VM_GET_ALL_DESIGN_DOCUMENTS = "manager_views_get_all_design_documents"
    OP_VM_UPSERT_DESIGN_DOCUMENT = "manager_views_upsert_design_document"
    OP_VM_DROP_DESIGN_DOCUMENT = "manager_views_drop_design_document"
    OP_VM_PUBLISH_DESIGN_DOCUMENT = "manager_views_publish_design_document"

    OP_LIST_EACH = "list_each"
    OP_LIST_LENGTH = "list_length"
    OP_LIST_PUSH = "list_push"
    OP_LIST_UNSHIFT = "list_unshift"
    OP_LIST_INSERT = "list_insert"
    OP_LIST_AT = "list_at"
    OP_LIST_DELETE_AT = "list_delete_at"
    OP_LIST_CLEAR = "list_clear"

    OP_MAP_EACH = "map_each"
    OP_MAP_LENGTH = "map_length"
    OP_MAP_CLEAR = "map_clear"
    OP_MAP_FETCH = "map_fetch"
    OP_MAP_DELETE = "map_delete"
    OP_MAP_KEY_EXISTS = "map_key"
    OP_MAP_STORE = "map_store"

    OP_QUEUE_EACH = "queue_each"
    OP_QUEUE_LENGTH = "queue_length"
    OP_QUEUE_CLEAR = "queue_clear"
    OP_QUEUE_PUSH = "queue_push"
    OP_QUEUE_POP = "queue_pop"

    OP_SET_EACH = "set_each"
    OP_SET_LENGTH = "set_length"
    OP_SET_ADD = "set_add"
    OP_SET_CLEAR = "set_clear"
    OP_SET_DELETE = "set_delete"

    STEP_REQUEST_ENCODING = "request_encoding"
    STEP_DISPATCH_TO_SERVER = "dispatch_to_server"

    # Common attributes
    ATTR_SYSTEM_NAME = "db.system.name"
    ATTR_CLUSTER_NAME = "couchbase.cluster.name"
    ATTR_CLUSTER_UUID = "couchbase.cluster.uuid"

    # Operation-level attributes
    ATTR_OPERATION_NAME = "db.operation.name"
    ATTR_SERVICE = "couchbase.service"
    ATTR_BUCKET_NAME = "db.namespace"
    ATTR_SCOPE_NAME = "couchbase.scope.name"
    ATTR_COLLECTION_NAME = "couchbase.collection.name"
    ATTR_RETRIES = "couchbase.retries"
    ATTR_DURABILITY = "couchbase.durability"
    ATTR_QUERY_STATEMENT = "db.query.text"
    ATTR_ERROR_TYPE = "error.type"

    # Dispatch-level attributes
    ATTR_LOCAL_ID = "couchbase.local_id"
    ATTR_OPERATION_ID = "couchbase.operation_id"
    ATTR_PEER_ADDRESS = "network.peer.address"
    ATTR_PEER_PORT = "network.peer.port"
    ATTR_SERVER_DURATION = "couchbase.server_duration"

    # Reserved attributes
    ATTR_RESERVED_UNIT = "__unit"

    ATTR_VALUE_SYSTEM_NAME = "couchbase"

    ATTR_VALUE_DURABILITY_MAJORITY = "majority"
    ATTR_VALUE_DURABILITY_MAJORITY_AND_PERSIST_TO_ACTIVE = "majority_and_persist_active"
    ATTR_VALUE_DURABILITY_PERSIST_TO_MAJORITY = "persist_majority"

    ATTR_VALUE_SERVICE_KV = "kv"
    ATTR_VALUE_SERVICE_QUERY = "query"
    ATTR_VALUE_SERVICE_SEARCH = "search"
    ATTR_VALUE_SERVICE_VIEWS = "views"
    ATTR_VALUE_SERVICE_ANALYTICS = "analytics"
    ATTR_VALUE_SERVICE_MANAGEMENT = "management"

    ATTR_VALUE_RESERVED_UNIT_SECONDS = "s"

    METER_NAME_OPERATION_DURATION = "db.client.operation.duration"
  end
end
