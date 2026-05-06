create table kafka_raw (
    id string,
    customer_first_name string,
    customer_last_name string,
    customer_age string,
    customer_email string,
    customer_country string,
    customer_postal_code string,
    customer_pet_type string,
    customer_pet_name string,
    customer_pet_breed string,
    seller_first_name string,
    seller_last_name string,
    seller_email string,
    seller_country string,
    seller_postal_code string,
    product_name string,
    product_category string,
    product_price string,
    product_quantity string,
    sale_date string,
    sale_customer_id string,
    sale_seller_id string,
    sale_product_id string,
    sale_quantity string,
    sale_total_price string,
    store_name string,
    store_location string,
    store_city string,
    store_state string,
    store_country string,
    store_phone string,
    store_email string,
    pet_category string,
    product_weight string,
    product_color string,
    product_size string,
    product_brand string,
    product_material string,
    product_description string,
    product_rating string,
    product_reviews string,
    product_release_date string,
    product_expiry_date string,
    supplier_name string,
    supplier_contact string,
    supplier_email string,
    supplier_phone string,
    supplier_address string,
    supplier_city string,
    supplier_country string,
    global_sale_id string,
    file_name string,
    file_sequence string
) with (
    'connector' = 'kafka',
    'topic' = 'mock_data_input',
    'properties.bootstrap.servers' = 'kafka:29092',
    'properties.group.id' = 'flink_consumer_group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

create table dim_customer (
    customer_id int,
    first_name string,
    last_name string,
    age int,
    email string,
    country string,
    postal_code string,
    primary key (customer_id) not enforced
) with (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_customer',
    'username' = 'postgres',
    'password' = 'postgres'
);

create table dim_customer_pet (
    customer_id int,
    pet_type string,
    pet_name string,
    pet_breed string,
    pet_category string,
    primary key (customer_id) not enforced
) with (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_customer_pet',
    'username' = 'postgres',
    'password' = 'postgres'
);

create table dim_seller (
    seller_id int,
    first_name string,
    last_name string,
    email string,
    country string,
    postal_code string,
    primary key (seller_id) not enforced
) with (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_seller',
    'username' = 'postgres',
    'password' = 'postgres'
);

create table dim_supplier (
    supplier_name string,
    contact string,
    email string,
    phone string,
    supplier_address string,
    city string,
    country string,
    primary key (supplier_name) not enforced
) with (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_supplier',
    'username' = 'postgres',
    'password' = 'postgres'
);

create table dim_store (
    store_name string,
    store_location string,
    city string,
    store_state string,
    country string,
    phone string,
    email string,
    primary key (store_name) not enforced
) with (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_store',
    'username' = 'postgres',
    'password' = 'postgres'
);

create table dim_product (
    product_id int,
    product_name string,
    product_category string,
    price decimal(12, 2),
    quantity int,
    product_weight decimal(12, 2),
    color string,
    product_size string,
    brand string,
    material string,
    product_description string,
    rating decimal(3, 1),
    reviews int,
    date_release date,
    date_expiry date,
    primary key (product_id) not enforced
) with (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.dim_product',
    'username' = 'postgres',
    'password' = 'postgres'
);

create table fact_sales (
    sale_id int,
    sale_date date,
    customer_id int,
    seller_id int,
    product_id int,
    store_name string,
    supplier_name string,
    quantity int,
    total_price decimal(12, 2),
    primary key (sale_id) not enforced
) with (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/bigdata_flink',
    'table-name' = 'dwh.fact_sales',
    'username' = 'postgres',
    'password' = 'postgres'
);

execute statement set 
begin

insert into dim_customer
select distinct 
    cast(sale_customer_id as int) as customer_id,
    customer_first_name,
    customer_last_name,
    cast(nullif(customer_age, '') as int) as age,
    customer_email,
    customer_country,
    customer_postal_code
from
    kafka_raw
where sale_customer_id is not null and sale_customer_id <> '';

insert into dim_customer_pet
select distinct 
    cast(sale_customer_id as int) as customer_id,
    customer_pet_type,
    customer_pet_name,
    customer_pet_breed,
    pet_category
from
    kafka_raw
where sale_customer_id is not null and sale_customer_id <> '';

insert into dim_seller
select distinct 
    cast(sale_seller_id as int) as seller_id,
    seller_first_name,
    seller_last_name,
    seller_email,
    seller_country,
    seller_postal_code
from
    kafka_raw
where sale_seller_id is not null and sale_seller_id <> '';

insert into dim_supplier
select distinct
    supplier_name,
    supplier_contact,
    supplier_email,
    supplier_phone,
    supplier_address,
    supplier_city,
    supplier_country
from
    kafka_raw
where supplier_name is not null and supplier_name <> '';

insert into dim_store
select distinct
    store_name,
    store_location,
    store_city,
    store_state,
    store_country,
    store_phone,
    store_email
from
    kafka_raw
where store_name is not null and store_name <> '';

insert into dim_product
select distinct
    cast(sale_product_id as int) as product_id,
    product_name,
    product_category,
    cast(nullif(product_price, '') as decimal(12, 2)) as price,
    cast(nullif(product_quantity, '') as int) as quantity,
    cast(nullif(product_weight, '') as decimal(12, 2)) as product_weight,
    product_color as color,
    product_size,
    product_brand as brand,
    product_material as material,
    product_description,
    cast(nullif(product_rating, '') as decimal(3, 1)) as rating,
    cast(nullif(product_reviews, '') as int) as reviews,
    to_date(nullif(product_release_date, ''), 'MM/dd/yyyy') as date_release,
    to_date(nullif(product_expiry_date, ''), 'MM/dd/yyyy') as date_expiry
from kafka_raw
where sale_product_id is not null and sale_product_id <> '';

insert into fact_sales
select distinct
    cast(global_sale_id as int) as sale_id,
    to_date(sale_date, 'MM/dd/yyyy') as sale_date,
    cast(sale_customer_id as int) as customer_id,
    cast(sale_seller_id as int) as seller_id,
    cast(sale_product_id as int) as product_id,
    store_name,
    supplier_name,
    cast(nullif(sale_quantity, '') as int) as quantity,
    cast(nullif(sale_total_price, '') as decimal(12, 2)) as total_price
from kafka_raw
where id is not null and id <> ''
    and global_sale_id is not null and global_sale_id <> ''
    and sale_date is not null and sale_date <> ''
    and sale_customer_id is not null and sale_customer_id <> ''
    and sale_seller_id is not null and sale_seller_id <> ''
    and sale_product_id is not null and sale_product_id <> ''
    and store_name is not null and store_name <> ''
    and supplier_name is not null and supplier_name <> '';

end;
