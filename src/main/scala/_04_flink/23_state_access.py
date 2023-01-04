from pyflink.common import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig


class Sum(KeyedProcessFunction):
    """이름을 기준으로 값을 합산하는 객체"""

    def __init__(self):
        self.state = None

    def open(self, runtime_context: RuntimeContext):
        state_descriptor = ValueStateDescriptor("state", Types.FLOAT())
        # ttl state가 얼마나 오랫동안 생존하는지 설정, Time To Leave
        state_ttl_config = StateTtlConfig.new_builder(Time.seconds(1)) \
            .set_update_type(StateTtlConfig.UpdateType.OnReadAndWrite) \
            .disable_cleanup_in_background().build()
        state_descriptor.enable_time_to_live(state_ttl_config)
        self.state = runtime_context.get_state(state_descriptor)

    def process_element(self, value, ctx: 'KeyedProcessFunction.Context'):
        """

        :param value:
        :param ctx: window function 함수를 사용할 때
        :return:
        """
        current = self.state.value()
        if current is None:
            current = 0

        current += value[1]
        self.state.update(current)
        # generator 함수이기 때문에 yield 사용 가능
        # 다음 함수에 값을 넘겨주는 행위
        yield value[0], current


env = StreamExecutionEnvironment.get_execution_environment()

ds = env.from_collection(
    collection=[
        ('Alice', 110.1),
        ('Bob', 30.2),
        ('Alice', 20.1),
        ('Bob', 53.1),
        ('Alice', 13.1),
        ('Bob', 3.1),
        ('Bob', 16.1),
        ('Alice', 20.1),
    ],
    type_info=Types.TUPLE([Types.STRING(), Types.FLOAT()])
)

ds.key_by(lambda x: x[0]).process(Sum()).print()
env.execute()
