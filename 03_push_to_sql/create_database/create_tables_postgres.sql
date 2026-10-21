-- ==============================================================================
-- DATABASE SCHEMA DÀNH CHO POSTGRESQL / SUPABASE
-- Dự án: Tiki Analytics
-- ==============================================================================

-- 1. Bảng dim_category
CREATE TABLE IF NOT EXISTS dim_category (
    category_id     BIGINT PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    "urlKey"        VARCHAR(255),
    src             VARCHAR(50),
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 2. Bảng dim_sub_category
CREATE TABLE IF NOT EXISTS dim_sub_category (
    id                  BIGSERIAL PRIMARY KEY,
    category_id         BIGINT NOT NULL REFERENCES dim_category(category_id) ON DELETE CASCADE,
    category_name       VARCHAR(255),
    sub_category_1_id   BIGINT,
    sub_category_1      VARCHAR(255),
    sub_category_2_id   BIGINT,
    sub_category_2      VARCHAR(255),
    sub_category_3_id   BIGINT,
    sub_category_3      VARCHAR(255),
    sub_category_4_id   BIGINT,
    sub_category_4      VARCHAR(255),
    sub_category_5_id   BIGINT,
    sub_category_5      VARCHAR(255)
);
CREATE INDEX IF NOT EXISTS ix_sub_category_catid ON dim_sub_category(category_id);

-- 3. Bảng dim_product_listing
CREATE TABLE IF NOT EXISTS dim_product_listing (
    product_id          BIGINT NOT NULL,
    seller_product_id   BIGINT NOT NULL,
    category_id         BIGINT NOT NULL,
    crawl_date          VARCHAR(7) NOT NULL, -- Snapshot theo tháng: YYYY-MM
    PRIMARY KEY (product_id, seller_product_id, category_id, crawl_date)
);

-- 4. Bảng product_detail
CREATE TABLE IF NOT EXISTS product_detail (
    product_id              BIGINT NOT NULL,
    master_id               BIGINT,
    sku                     VARCHAR(100),
    product_name            TEXT,
    product_type            VARCHAR(50),
    url_key                 VARCHAR(500),
    created_date            DATE,
    favourite_count         INT DEFAULT 0,
    brand_id                BIGINT,
    brand_name              VARCHAR(255),
    rating_average          NUMERIC(3, 2) DEFAULT 0.00,
    review_count            INT DEFAULT 0,
    all_time_quantity_sold  BIGINT DEFAULT 0,
    crawl_date              VARCHAR(7) NOT NULL, -- Snapshot theo tháng: YYYY-MM
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id, crawl_date)
);
CREATE INDEX IF NOT EXISTS ix_product_detail_brand ON product_detail(brand_name);
CREATE INDEX IF NOT EXISTS ix_product_detail_sold ON product_detail(all_time_quantity_sold DESC);

-- Dimension hiện tại cho Power BI. product_detail vẫn giữ lịch sử snapshot tháng;
-- bảng này giữ phiên bản mới nhất của từng sản phẩm.
CREATE TABLE IF NOT EXISTS dim_product (
    product_id              BIGINT PRIMARY KEY,
    master_id               BIGINT,
    sku                     VARCHAR(100),
    product_name            TEXT,
    product_type            VARCHAR(50),
    url_key                 VARCHAR(500),
    created_date            DATE,
    favourite_count         INT DEFAULT 0,
    brand_id                BIGINT,
    brand_name              VARCHAR(255),
    rating_average          NUMERIC(3, 2) DEFAULT 0.00,
    review_count            INT DEFAULT 0,
    all_time_quantity_sold  BIGINT DEFAULT 0,
    latest_crawl_month      VARCHAR(7) NOT NULL,
    updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_dim_product_brand ON dim_product(brand_name);

-- 5. Bảng product_seller
CREATE TABLE IF NOT EXISTS product_seller (
    id                  BIGSERIAL PRIMARY KEY,
    product_id          BIGINT NOT NULL,
    seller_product_id   BIGINT NOT NULL,
    seller_id           BIGINT,
    seller_name         VARCHAR(255),
    seller_link         VARCHAR(500),
    store_id            BIGINT,
    seller_price        BIGINT DEFAULT 0,
    is_current_seller   BOOLEAN DEFAULT FALSE,
    is_best_store       BOOLEAN DEFAULT FALSE,
    crawl_date          VARCHAR(7) NOT NULL -- Snapshot theo tháng: YYYY-MM
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_product_seller_key
    ON product_seller (product_id, seller_product_id, COALESCE(seller_id, -1), crawl_date);

-- 6. Bảng product_category_map
CREATE TABLE IF NOT EXISTS product_category_map (
    id                  BIGSERIAL PRIMARY KEY,
    product_id          BIGINT NOT NULL,
    category_id         BIGINT NOT NULL,
    category_name       VARCHAR(255),
    category_level      INT DEFAULT 1,
    category_url        VARCHAR(500),
    is_leaf             BOOLEAN DEFAULT FALSE,
    is_primary          BOOLEAN DEFAULT FALSE,
    crawl_date          VARCHAR(7) NOT NULL, -- Snapshot theo tháng: YYYY-MM
    CONSTRAINT uq_product_category UNIQUE (product_id, category_id, crawl_date)
);

-- 7. Bảng fact_daily_price
CREATE TABLE IF NOT EXISTS fact_daily_price (
    id                      BIGSERIAL PRIMARY KEY,
    product_id              BIGINT NOT NULL,
    seller_product_id       BIGINT NOT NULL,
    seller_id               BIGINT,
    category_id             BIGINT NOT NULL,
    crawl_date              DATE NOT NULL,
    price                   BIGINT DEFAULT 0,
    list_price              BIGINT DEFAULT 0,
    original_price          BIGINT DEFAULT 0,
    discount                BIGINT DEFAULT 0,
    discount_rate           INT DEFAULT 0,
    rating_average          NUMERIC(3, 2) DEFAULT 0.00,
    review_count            INT DEFAULT 0,
    all_time_quantity_sold  BIGINT DEFAULT 0,
    inventory_status        VARCHAR(50),
    CONSTRAINT uq_daily_price UNIQUE (product_id, seller_product_id, crawl_date)
);
CREATE INDEX IF NOT EXISTS ix_daily_price_date ON fact_daily_price(crawl_date);

-- 8. Bảng fact_review
CREATE TABLE IF NOT EXISTS fact_review (
    review_id           BIGINT NOT NULL,
    product_id          BIGINT NOT NULL,
    seller_product_id   BIGINT NOT NULL,
    seller_id           BIGINT,
    rating              SMALLINT NOT NULL CHECK (rating BETWEEN 1 AND 5),
    title               VARCHAR(500),
    content             TEXT,
    review_created_at   TIMESTAMP,
    purchased           BOOLEAN DEFAULT FALSE,
    thank_count         INT DEFAULT 0,
    comment_count       INT DEFAULT 0,
    is_photo            BOOLEAN DEFAULT FALSE,
    crawl_time          TIMESTAMP,
    CONSTRAINT pk_fact_review PRIMARY KEY (review_id, product_id, seller_product_id)
);
CREATE INDEX IF NOT EXISTS ix_review_prodid ON fact_review(product_id);
CREATE INDEX IF NOT EXISTS ix_review_rating ON fact_review(rating);

-- Views thân thiện với Power BI, giữ nguyên các bảng lưu lịch sử nguồn.
CREATE OR REPLACE VIEW vw_daily_price_analysis AS
SELECT p.*, d.product_name, d.brand_name, d.product_type
FROM fact_daily_price AS p
LEFT JOIN dim_product AS d USING (product_id);

CREATE OR REPLACE VIEW vw_review_analysis AS
SELECT r.*, d.product_name, d.brand_name, d.product_type
FROM fact_review AS r
LEFT JOIN dim_product AS d USING (product_id);
