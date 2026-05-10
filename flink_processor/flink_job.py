import json
from datetime import datetime
from decimal import Decimal, InvalidOperation

from pyflink.common import Row
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.jdbc import (
    JdbcConnectionOptions,
    JdbcExecutionOptions,
    JdbcSink,
)
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource


KAFKA_BOOTSTRAP_SERVERS = "kafka:29092"
KAFKA_TOPIC = "mock_data_input"
KAFKA_GROUP_ID = "flink_datastream_consumer_group"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/bigdata_flink"
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DRIVER = "org.postgresql.Driver"


def parse_json(raw_message):
    try:
        row = json.loads(raw_message)
        if isinstance(row, dict):
            return row
    except Exception:
        return None
    return None


def clean(value):
    if value is None:
        return None

    value = str(value).strip()

    if value == "":
        return None

    return value


def to_int(value):
    value = clean(value)

    if value is None:
        return None

    try:
        return int(value)
    except ValueError:
        return None


def to_decimal(value):
    value = clean(value)

    if value is None:
        return None

    try:
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return None


def to_date(value):
    value = clean(value)

    if value is None:
        return None

    try:
        return datetime.strptime(value, "%m/%d/%Y").date()
    except ValueError:
        return None


def to_dim_customer(raw_message):
    row = parse_json(raw_message)

    if row is None:
        return

    customer_id = to_int(row.get("sale_customer_id"))

    if customer_id is None:
        return

    yield Row(
        customer_id,
        clean(row.get("customer_first_name")),
        clean(row.get("customer_last_name")),
        to_int(row.get("customer_age")),
        clean(row.get("customer_email")),
        clean(row.get("customer_country")),
        clean(row.get("customer_postal_code")),
    )


def to_dim_customer_pet(raw_message):
    row = parse_json(raw_message)

    if row is None:
        return

    customer_id = to_int(row.get("sale_customer_id"))

    if customer_id is None:
        return

    yield Row(
        customer_id,
        clean(row.get("customer_pet_type")),
        clean(row.get("customer_pet_name")),
        clean(row.get("customer_pet_breed")),
        clean(row.get("pet_category")),
    )


def to_dim_seller(raw_message):
    row = parse_json(raw_message)

    if row is None:
        return

    seller_id = to_int(row.get("sale_seller_id"))

    if seller_id is None:
        return

    yield Row(
        seller_id,
        clean(row.get("seller_first_name")),
        clean(row.get("seller_last_name")),
        clean(row.get("seller_email")),
        clean(row.get("seller_country")),
        clean(row.get("seller_postal_code")),
    )


def to_dim_supplier(raw_message):
    row = parse_json(raw_message)

    if row is None:
        return

    supplier_name = clean(row.get("supplier_name"))

    if supplier_name is None:
        return

    yield Row(
        supplier_name,
        clean(row.get("supplier_contact")),
        clean(row.get("supplier_email")),
        clean(row.get("supplier_phone")),
        clean(row.get("supplier_address")),
        clean(row.get("supplier_city")),
        clean(row.get("supplier_country")),
    )


def to_dim_store(raw_message):
    row = parse_json(raw_message)

    if row is None:
        return

    store_name = clean(row.get("store_name"))

    if store_name is None:
        return

    yield Row(
        store_name,
        clean(row.get("store_location")),
        clean(row.get("store_city")),
        clean(row.get("store_state")),
        clean(row.get("store_country")),
        clean(row.get("store_phone")),
        clean(row.get("store_email")),
    )


def to_dim_product(raw_message):
    row = parse_json(raw_message)

    if row is None:
        return

    product_id = to_int(row.get("sale_product_id"))

    if product_id is None:
        return

    yield Row(
        product_id,
        clean(row.get("product_name")),
        clean(row.get("product_category")),
        to_decimal(row.get("product_price")),
        to_int(row.get("product_quantity")),
        to_decimal(row.get("product_weight")),
        clean(row.get("product_color")),
        clean(row.get("product_size")),
        clean(row.get("product_brand")),
        clean(row.get("product_material")),
        clean(row.get("product_description")),
        to_decimal(row.get("product_rating")),
        to_int(row.get("product_reviews")),
        to_date(row.get("product_release_date")),
        to_date(row.get("product_expiry_date")),
    )


def to_fact_sales(raw_message):
    row = parse_json(raw_message)

    if row is None:
        return

    sale_id = to_int(row.get("global_sale_id"))
    sale_date = to_date(row.get("sale_date"))
    customer_id = to_int(row.get("sale_customer_id"))
    seller_id = to_int(row.get("sale_seller_id"))
    product_id = to_int(row.get("sale_product_id"))
    store_name = clean(row.get("store_name"))
    supplier_name = clean(row.get("supplier_name"))

    if None in (
        sale_id,
        sale_date,
        customer_id,
        seller_id,
        product_id,
        store_name,
        supplier_name,
    ):
        return

    yield Row(
        sale_id,
        sale_date,
        customer_id,
        seller_id,
        product_id,
        store_name,
        supplier_name,
        to_int(row.get("sale_quantity")),
        to_decimal(row.get("sale_total_price")),
    )


def jdbc_connection_options():
    return JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
        .with_url(POSTGRES_URL) \
        .with_driver_name(POSTGRES_DRIVER) \
        .with_user_name(POSTGRES_USER) \
        .with_password(POSTGRES_PASSWORD) \
        .build()


def jdbc_execution_options():
    return JdbcExecutionOptions.builder() \
        .with_batch_interval_ms(1000) \
        .with_batch_size(200) \
        .with_max_retries(5) \
        .build()


def add_jdbc_sink(stream, sql, type_info):
    stream.add_sink(
        JdbcSink.sink(
            sql,
            type_info,
            jdbc_connection_options(),
            jdbc_execution_options(),
        )
    )


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.enable_checkpointing(10000)

    env.add_jars(
        "file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar",
        "file:///opt/flink/lib/flink-connector-jdbc-3.1.2-1.18.jar",
        "file:///opt/flink/lib/postgresql-42.7.3.jar",
    )

    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVERS) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id(KAFKA_GROUP_ID) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    raw_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Raw JSON stream",
    )

    customer_type = Types.ROW([
        Types.INT(), Types.STRING(), Types.STRING(), Types.INT(),
        Types.STRING(), Types.STRING(), Types.STRING(),
    ])

    customer_pet_type = Types.ROW([
        Types.INT(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
    ])

    seller_type = Types.ROW([
        Types.INT(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.STRING(),
    ])

    supplier_type = Types.ROW([
        Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.STRING(),
    ])

    store_type = Types.ROW([
        Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.STRING(), Types.STRING(),
    ])

    product_type = Types.ROW([
        Types.INT(), Types.STRING(), Types.STRING(), Types.BIG_DEC(), Types.INT(),
        Types.BIG_DEC(), Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING(),
        Types.STRING(), Types.BIG_DEC(), Types.INT(), Types.SQL_DATE(), Types.SQL_DATE(),
    ])

    fact_sales_type = Types.ROW([
        Types.INT(), Types.SQL_DATE(), Types.INT(), Types.INT(), Types.INT(),
        Types.STRING(), Types.STRING(), Types.INT(), Types.BIG_DEC(),
    ])

    add_jdbc_sink(
        raw_stream.flat_map(to_dim_customer, output_type=customer_type),
        """
        insert into dwh.dim_customer
            (customer_id, first_name, last_name, age, email, country, postal_code)
        values (?, ?, ?, ?, ?, ?, ?)
        on conflict (customer_id) do update set
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            age = excluded.age,
            email = excluded.email,
            country = excluded.country,
            postal_code = excluded.postal_code
        """,
        customer_type,
    )

    add_jdbc_sink(
        raw_stream.flat_map(to_dim_customer_pet, output_type=customer_pet_type),
        """
        insert into dwh.dim_customer_pet
            (customer_id, pet_type, pet_name, pet_breed, pet_category)
        values (?, ?, ?, ?, ?)
        on conflict (customer_id) do update set
            pet_type = excluded.pet_type,
            pet_name = excluded.pet_name,
            pet_breed = excluded.pet_breed,
            pet_category = excluded.pet_category
        """,
        customer_pet_type,
    )

    add_jdbc_sink(
        raw_stream.flat_map(to_dim_seller, output_type=seller_type),
        """
        insert into dwh.dim_seller
            (seller_id, first_name, last_name, email, country, postal_code)
        values (?, ?, ?, ?, ?, ?)
        on conflict (seller_id) do update set
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            email = excluded.email,
            country = excluded.country,
            postal_code = excluded.postal_code
        """,
        seller_type,
    )

    add_jdbc_sink(
        raw_stream.flat_map(to_dim_supplier, output_type=supplier_type),
        """
        insert into dwh.dim_supplier
            (supplier_name, contact, email, phone, supplier_address, city, country)
        values (?, ?, ?, ?, ?, ?, ?)
        on conflict (supplier_name) do update set
            contact = excluded.contact,
            email = excluded.email,
            phone = excluded.phone,
            supplier_address = excluded.supplier_address,
            city = excluded.city,
            country = excluded.country
        """,
        supplier_type,
    )

    add_jdbc_sink(
        raw_stream.flat_map(to_dim_store, output_type=store_type),
        """
        insert into dwh.dim_store
            (store_name, store_location, city, store_state, country, phone, email)
        values (?, ?, ?, ?, ?, ?, ?)
        on conflict (store_name) do update set
            store_location = excluded.store_location,
            city = excluded.city,
            store_state = excluded.store_state,
            country = excluded.country,
            phone = excluded.phone,
            email = excluded.email
        """,
        store_type,
    )

    add_jdbc_sink(
        raw_stream.flat_map(to_dim_product, output_type=product_type),
        """
        insert into dwh.dim_product
            (product_id, product_name, product_category, price, quantity, product_weight,
             color, product_size, brand, material, product_description, rating, reviews,
             date_release, date_expiry)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict (product_id) do update set
            product_name = excluded.product_name,
            product_category = excluded.product_category,
            price = excluded.price,
            quantity = excluded.quantity,
            product_weight = excluded.product_weight,
            color = excluded.color,
            product_size = excluded.product_size,
            brand = excluded.brand,
            material = excluded.material,
            product_description = excluded.product_description,
            rating = excluded.rating,
            reviews = excluded.reviews,
            date_release = excluded.date_release,
            date_expiry = excluded.date_expiry
        """,
        product_type,
    )

    add_jdbc_sink(
        raw_stream.flat_map(to_fact_sales, output_type=fact_sales_type),
        """
        insert into dwh.fact_sales
            (sale_id, sale_date, customer_id, seller_id, product_id,
             store_name, supplier_name, quantity, total_price)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict (sale_id) do update set
            sale_date = excluded.sale_date,
            customer_id = excluded.customer_id,
            seller_id = excluded.seller_id,
            product_id = excluded.product_id,
            store_name = excluded.store_name,
            supplier_name = excluded.supplier_name,
            quantity = excluded.quantity,
            total_price = excluded.total_price
        """,
        fact_sales_type,
    )

    env.execute("Flink Job")


if __name__ == "__main__":
    main()