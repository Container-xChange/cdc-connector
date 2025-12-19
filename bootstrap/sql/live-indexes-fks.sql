-- ============================================================================
-- Live Database (xchangelive) - Indexes and Foreign Keys
-- ============================================================================
-- This file contains all indexes and foreign keys to be added AFTER pgloader
-- initial snapshot when "create indexes" is disabled for performance.
--
-- Usage:
--   psql -h <host> -U <user> -d <database> -f live-indexes-fks.sql
-- ============================================================================

-- =========================
-- INDEXES
-- =========================

-- pipedrive_id_lookup
CREATE INDEX idx_source_id ON xchangelive.pipedrive_id_lookup (source_id);
CREATE INDEX idx_source_object_type ON xchangelive.pipedrive_id_lookup (source_object_type);

-- T_CARRIER
CREATE INDEX company_group_id ON xchangelive.T_CARRIER (company_group_id);
CREATE UNIQUE INDEX public_profile_domain_handle ON xchangelive.T_CARRIER (public_profile_domain_handle);

-- T_COMPANY_PAYMENT
CREATE UNIQUE INDEX paymentCompanyIdUnique ON xchangelive.T_COMPANY_PAYMENT (company_id);

-- T_COMPANY_REVIEW
CREATE INDEX fk_company_reference_estimated_company_id ON xchangelive.T_COMPANY_REVIEW (review_to_company_id);
CREATE INDEX fk_from_carrier_id ON xchangelive.T_COMPANY_REVIEW (review_from_company_id);
CREATE INDEX fk_request_id ON xchangelive.T_COMPANY_REVIEW (request_id);
CREATE UNIQUE INDEX unique_index ON xchangelive.T_COMPANY_REVIEW (review_from_company_id, review_to_company_id, request_id);

-- T_LOCATION
CREATE INDEX country_IX ON xchangelive.T_LOCATION (country);
CREATE UNIQUE INDEX locationUnlocodeUnique ON xchangelive.T_LOCATION (unlocode);
CREATE INDEX one_way_app_IX ON xchangelive.T_LOCATION (one_way_app);
CREATE INDEX tracking_app_IX ON xchangelive.T_LOCATION (tracking_app);
CREATE INDEX trading_app_IX ON xchangelive.T_LOCATION (trading_app);

-- T_REQUEST
CREATE INDEX fk_request_carrier_on_turn_id ON xchangelive.T_REQUEST (carrier_on_turn_id);

-- T_REQUEST_ATTACHMENT
CREATE INDEX fk_attachment_request ON xchangelive.T_REQUEST_ATTACHMENT (attachment_id);

-- T_REQUEST_CONTRACT
CREATE INDEX fk_rc_request ON xchangelive.T_REQUEST_CONTRACT (request_id);
CREATE INDEX fk_rc_request_contract_attachment ON xchangelive.T_REQUEST_CONTRACT (request_contract_attachment_id);

-- T_REQUEST_CONVERSATION_POSTING
CREATE INDEX fk_message_reply_id ON xchangelive.T_REQUEST_CONVERSATION_POSTING (reply_message_id);
CREATE INDEX fk_request_conversation_posting_request_id ON xchangelive.T_REQUEST_CONVERSATION_POSTING (request_id);
CREATE INDEX fk_request_conversation_posting_user_id ON xchangelive.T_REQUEST_CONVERSATION_POSTING (user_id);
CREATE UNIQUE INDEX uq_request_id_message_id ON xchangelive.T_REQUEST_CONVERSATION_POSTING (request_id, message_id);

-- T_REQUEST_PAYMENT
CREATE INDEX invoice_id_index ON xchangelive.T_REQUEST_PAYMENT (invoice_id);
CREATE INDEX request_id_index ON xchangelive.T_REQUEST_PAYMENT (request_id);
CREATE UNIQUE INDEX request_id_transaction_category ON xchangelive.T_REQUEST_PAYMENT (request_id, transaction_category);
CREATE INDEX transaction_id_index ON xchangelive.T_REQUEST_PAYMENT (transaction_id);

-- T_REQUEST_VERSION
CREATE INDEX fk_requestversion_agent_id ON xchangelive.T_REQUEST_VERSION (agent_id);
CREATE INDEX fk_requestversion_cc_agent_id ON xchangelive.T_REQUEST_VERSION (cc_agent_id);
CREATE INDEX fk_requestversion_request_id ON xchangelive.T_REQUEST_VERSION (request_id);
CREATE INDEX fk_request_editor_id ON xchangelive.T_REQUEST_VERSION (editor_id);
CREATE INDEX fk_request_location_id ON xchangelive.T_REQUEST_VERSION (location_id);
CREATE INDEX fk_request_provider_id ON xchangelive.T_REQUEST_VERSION (addressee_id);
CREATE INDEX fk_request_redeliverylocation_id ON xchangelive.T_REQUEST_VERSION (redeliverylocation_id);
CREATE INDEX fk_request_requester_id ON xchangelive.T_REQUEST_VERSION (requester_id);
CREATE INDEX fk_request_requestrejectreason_id ON xchangelive.T_REQUEST_VERSION (request_reject_reason_id);
CREATE INDEX fk_request_version_equipment_type ON xchangelive.T_REQUEST_VERSION (equipment_type);
CREATE INDEX fk_request_version_pickup_charge_payer_id ON xchangelive.T_REQUEST_VERSION (pickup_charge_payer_id);
CREATE INDEX status_IX ON xchangelive.T_REQUEST_VERSION (status);
CREATE UNIQUE INDEX unique_version_per_request_constraint ON xchangelive.T_REQUEST_VERSION (request_id, version);

-- T_USER
CREATE INDEX fk_user_carrier ON xchangelive.T_USER (carrier);
CREATE UNIQUE INDEX idx_user_login ON xchangelive.T_USER (login);
CREATE UNIQUE INDEX login ON xchangelive.T_USER (login);

-- =========================
-- FOREIGN KEYS
-- =========================

-- Note: No foreign keys were found in the live database.
-- This is common for high-performance CDC scenarios where referential
-- integrity is maintained at the application level.
