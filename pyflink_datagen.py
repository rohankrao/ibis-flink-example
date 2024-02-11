import ibis
from pyflink.common import Types
from pyflink.datastream import KeyedProcessFunction, RuntimeContext
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

# 1. create a TableEnvironment
env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)

env_settings = EnvironmentSettings.in_streaming_mode()

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


class MyProcessFunction(KeyedProcessFunction):

    def __init__(self):
        self.seen = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("seen", Types.INT())
        self.seen = runtime_context.get_state(state_descriptor)

    def process_element(self, value, ctx):
        provinceId = value[0]
        if self.seen.value() is None:
            self.seen.update(provinceId)
            yield provinceId


sql = con.compile(agged.as_table())

res_ds = table_env.to_data_stream(table_env.sql_query(sql))

res_ds.key_by(lambda r: r[0]) \
    .process(MyProcessFunction()) \
    .print()

env.execute()

# df = agged.execute()
# print(agged.to_pandas())
