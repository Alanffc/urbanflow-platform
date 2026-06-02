-- SQL Schema Initialization for UrbanFlow Payment & Audit System (PostgreSQL)
-- All tables and columns are structured in English as per coding standards.

-- Drop existing tables if they exist to allow clean runs
DROP TABLE IF EXISTS audit_tariff_changes CASCADE;
DROP TABLE IF EXISTS transactions CASCADE;
DROP TABLE IF EXISTS user_cards CASCADE;
DROP TABLE IF EXISTS tariffs CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- 1. Users Table
CREATE TABLE users (
    id VARCHAR(50) PRIMARY KEY,
    full_name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. User Cards (NFC / Mobile App QR linking)
CREATE TABLE user_cards (
    card_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES users(id) ON DELETE CASCADE,
    card_type VARCHAR(20) NOT NULL, -- 'NFC_PHYSICAL', 'MOBILE_QR', 'MOBILE_NFC'
    balance DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
    status VARCHAR(20) NOT NULL DEFAULT 'ACTIVE', -- 'ACTIVE', 'BLOCKED', 'SUSPENDED'
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_balance CHECK (balance >= -5.00) -- Allow minor negative balance for emergency single rides
);

-- 3. Tariffs (Base rules for different transit combinations and CO2 parameters)
CREATE TABLE tariffs (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    base_cost DECIMAL(10, 2) NOT NULL,
    cost_per_km DECIMAL(10, 2) NOT NULL,
    mode_combination VARCHAR(100) NOT NULL, -- e.g., 'BUS', 'METRO', 'BUS+METRO', 'SCOOTER'
    co2_factor_g_km DECIMAL(6, 2) NOT NULL, -- Estimated CO2 grams emitted per km
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Transactions Table (Unified Ledger with strict constraint audit tracking)
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES users(id) ON DELETE RESTRICT,
    card_id VARCHAR(50) REFERENCES user_cards(card_id) ON DELETE RESTRICT,
    journey_id VARCHAR(100) NOT NULL, -- Grouping multiple transit legs under single journey
    amount DECIMAL(10, 2) NOT NULL,
    discount_applied DECIMAL(10, 2) DEFAULT 0.00,
    payment_method VARCHAR(30) NOT NULL, -- 'NFC', 'QR', 'APP'
    status VARCHAR(20) NOT NULL, -- 'PENDING', 'COMPLETED', 'FAILED', 'REFUNDED'
    tariff_id VARCHAR(50) REFERENCES tariffs(id),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 5. Audit Tariff Changes (Strict regulatory tracing for Context Aditional b)
CREATE TABLE audit_tariff_changes (
    audit_id SERIAL PRIMARY KEY,
    tariff_id VARCHAR(50) NOT NULL,
    previous_base_cost DECIMAL(10, 2),
    new_base_cost DECIMAL(10, 2) NOT NULL,
    previous_cost_per_km DECIMAL(10, 2),
    new_cost_per_km DECIMAL(10, 2) NOT NULL,
    changed_by_user VARCHAR(100) NOT NULL, -- Identity of operator or system executor
    change_reason TEXT NOT NULL,
    authorized_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexing for transactional query speed
CREATE INDEX idx_transactions_user ON transactions(user_id);
CREATE INDEX idx_transactions_journey ON transactions(journey_id);
CREATE INDEX idx_transactions_created_at ON transactions(created_at);

-- Insert seed data for testing
INSERT INTO users (id, full_name, email) VALUES
('usr-1001', 'John Doe', 'john.doe@example.com'),
('usr-1002', 'Jane Smith', 'jane.smith@example.com');

INSERT INTO user_cards (card_id, user_id, card_type, balance, status) VALUES
('card-nfc-9901', 'usr-1001', 'NFC_PHYSICAL', 25.50, 'ACTIVE'),
('card-qr-9902', 'usr-1002', 'MOBILE_QR', 10.00, 'ACTIVE');

INSERT INTO tariffs (id, name, base_cost, cost_per_km, mode_combination, co2_factor_g_km, is_active) VALUES
('trf-bus-single', 'Standard Bus Fare', 1.50, 0.10, 'BUS', 85.00, TRUE),
('trf-metro-single', 'Standard Metro Fare', 2.00, 0.05, 'METRO', 0.00, TRUE),
('trf-multi-integrated', 'Integrated Bus and Metro Fare', 2.50, 0.08, 'BUS+METRO', 45.00, TRUE),
('trf-scooter-rent', 'Shared Scooter Standard', 1.00, 0.25, 'SCOOTER', 0.00, TRUE);
