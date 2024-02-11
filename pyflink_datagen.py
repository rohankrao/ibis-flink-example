from pyflink.table import EnvironmentSettings, TableEnvironment
import ibis


# 1. create a TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)

con = ibis.flink.connect(table_env)

# 2. create source Table
source_schema = ibis.schema(
    {
        "createTime": "timestamp(3)",
        "orderId": "int64",
        "payAmount": "float64",
        "payPlatform": "int32",
        "provinceId": "int32",
    }
)

source_configs = {
    "connector": "datagen",
    "number-of-rows": "10",
    "fields.orderId.kind": "sequence",
    "fields.orderId.start": "1",
    "fields.orderId.end": "10",
    "fields.provinceId.kind": "sequence",
    "fields.provinceId.start": "1",
    "fields.provinceId.end": "10",
}

t = con.create_table(
    "payment_msg",
    schema=source_schema,
    tbl_properties=source_configs,
    watermark=ibis.watermark(
        time_col="createTime", allowed_delay=ibis.interval(seconds=15)
    ),
)


# 3. query from source table and perform calculations
agged = t.select(
    province_id=t.provinceId,
    pay_amount=t.payAmount.sum().over(
        range=(-ibis.interval(seconds=10), 0),
        group_by=t.provinceId,
        order_by=t.createTime,
    ),
)


df = agged.execute()
print(df)