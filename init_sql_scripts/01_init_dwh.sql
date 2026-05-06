create schema if not exists dwh;

create table if not exists dwh.dim_customer (
    customer_id int primary key,
    first_name text,
    last_name text,
    age int,
    email text,
    country text,
    postal_code text
);

create table if not exists dwh.dim_customer_pet (
    customer_id int primary key,
    pet_type text,
    pet_name text,
    pet_breed text,
    pet_category text
);

create table if not exists dwh.dim_seller (
    seller_id int primary key,
    first_name text,
    last_name text,
    email text,
    country text,
    postal_code text
);

create table if not exists dwh.dim_supplier (
    supplier_name text primary key,
    contact text,
    email text,
    phone text,
    supplier_address text,
    city text,
    country text
);

create table if not exists dwh.dim_store (
    store_name text primary key,
    store_location text,
    city text,
    store_state text,
    country text,
    phone text,
    email text
);

create table if not exists dwh.dim_product (
    product_id int primary key,
    product_name text,
    product_category text,
    price numeric(12, 2),
    quantity int,
    product_weight numeric(12, 2),
    color text,
    product_size text,
    brand text,
    material text,
    product_description text,
    rating numeric(3, 1),
    reviews int,
    date_release date,
    date_expiry date
);

create table if not exists dwh.fact_sales (
    sale_id int primary key,
    sale_date date not null,
    customer_id int not null,
    seller_id int not null,
    product_id int not null,
    store_name text not null,
    supplier_name text not null,
    quantity int,
    total_price numeric(12, 2)
);
