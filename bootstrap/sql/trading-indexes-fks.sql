-- ============================================================================
-- Trading Database (xchange_trading) - Indexes and Foreign Keys
-- ============================================================================
-- This file contains all indexes and foreign keys to be added AFTER pgloader
-- initial snapshot when "create indexes" is disabled for performance.
--
-- Usage:
--   psql -h <host> -U <user> -d <database> -f trading-indexes-fks.sql
-- ============================================================================

-- =========================
-- INDEXES
-- =========================

-- T_ABSTRACT_OFFER
CREATE INDEX I_COMPANY_ID ON xchange_trading.T_ABSTRACT_OFFER (company_id);
CREATE INDEX I_GENERAL_SEARCH ON xchange_trading.T_ABSTRACT_OFFER (source, offer_category, equipment_type, valid_until_date);
CREATE INDEX I_SEARCH ON xchange_trading.T_ABSTRACT_OFFER (unit_condition, equipment_type, location_unlocode, company_id);
CREATE INDEX I_SEARCH_SCORE_ID ON xchange_trading.T_ABSTRACT_OFFER (search_score, id);
CREATE INDEX I_VALID_UNTIL_DATE ON xchange_trading.T_ABSTRACT_OFFER (valid_until_date);

-- T_ABSTRACT_OFFER_ATTACHMENT
CREATE INDEX fk_offer_attachment__abstract_offer ON xchange_trading.T_ABSTRACT_OFFER_ATTACHMENT (abstract_offer_id);
CREATE UNIQUE INDEX unique_object_id ON xchange_trading.T_ABSTRACT_OFFER_ATTACHMENT (object_Id);

-- T_ABSTRACT_OFFER_HISTORIZATION
CREATE INDEX idx_insert_date ON xchange_trading.T_ABSTRACT_OFFER_HISTORIZATION (insert_date);
CREATE INDEX idx_original_id ON xchange_trading.T_ABSTRACT_OFFER_HISTORIZATION (original_id);
CREATE INDEX idx_source_category ON xchange_trading.T_ABSTRACT_OFFER_HISTORIZATION (source, offer_category);
CREATE INDEX idx_valid_until ON xchange_trading.T_ABSTRACT_OFFER_HISTORIZATION (valid_until_date);

-- T_DEAL
CREATE INDEX fk_deal__abstract_offer ON xchange_trading.T_DEAL (abstract_offer_id);
CREATE INDEX fk_deal__automatically_created_abstract_offer ON xchange_trading.T_DEAL (automatically_created_abstract_offer_id);
CREATE INDEX fk_deal__buyer_deal_proposal ON xchange_trading.T_DEAL (buyer_deal_proposal_id);
CREATE INDEX fk_deal__seller_deal_proposal ON xchange_trading.T_DEAL (seller_deal_proposal_id);
CREATE INDEX T_DEAL_state_index ON xchange_trading.T_DEAL (state);

-- T_DEAL_ADMIN_NOTE
CREATE INDEX fk_deal_admin_note_deal ON xchange_trading.T_DEAL_ADMIN_NOTE (deal_id);

-- T_DEAL_ATTACHMENT
CREATE INDEX fk_deal_attachment__deal ON xchange_trading.T_DEAL_ATTACHMENT (deal_id);

-- T_DEAL_DECLINE_REASON
CREATE UNIQUE INDEX deal_id ON xchange_trading.T_DEAL_DECLINE_REASON (deal_id);
CREATE INDEX fk_deal_decline_reason__deal ON xchange_trading.T_DEAL_DECLINE_REASON (deal_id);
CREATE INDEX fk_deal_decline_reason__decline_reason ON xchange_trading.T_DEAL_DECLINE_REASON (decline_reason_id);

-- T_DEAL_HISTORY
CREATE INDEX fk_deal_history__abstract_offer ON xchange_trading.T_DEAL_HISTORY (deal_id);
CREATE INDEX fk_deal_history__buyer_deal_proposal ON xchange_trading.T_DEAL_HISTORY (buyer_deal_proposal_id);
CREATE INDEX fk_deal_history__seller_deal_proposal ON xchange_trading.T_DEAL_HISTORY (seller_deal_proposal_id);

-- T_DEAL_INVOICE
CREATE INDEX fk_deal_invoice_deal_attachment ON xchange_trading.T_DEAL_INVOICE (deal_attachment_id);
CREATE UNIQUE INDEX uc_deal_invoice_deal_category ON xchange_trading.T_DEAL_INVOICE (deal_id, category);

-- T_DEAL_PROPOSAL
CREATE INDEX idx_tdp_id_and_company ON xchange_trading.T_DEAL_PROPOSAL (company_id);

-- T_LOCATION
CREATE INDEX t_location_default_name ON xchange_trading.T_LOCATION (default_name);
CREATE INDEX t_location_unlocode ON xchange_trading.T_LOCATION (unlocode);
CREATE UNIQUE INDEX unlocode ON xchange_trading.T_LOCATION (unlocode);

-- =========================
-- FOREIGN KEYS
-- =========================

-- Note: No foreign keys were found in the trading database.
-- This is common for high-performance CDC scenarios where referential
-- integrity is maintained at the application level.
