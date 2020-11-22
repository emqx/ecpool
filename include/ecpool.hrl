-type callback() :: fun((any()) -> any()).
-type action() :: fun((pid()) -> any()).
-type apply_mode() :: handover | handover_async
                     | {handover, timeout()}
                     | {handover_async, callback()}
                     | no_handover.
-type pool_type() :: random | hash | direct | round_robin.
-type pool_name() :: term().
-type reconn_callback() :: fun((pid()) -> term()).
-type option() :: {pool_size, pos_integer()}
                | {pool_type, pool_type()}
                | {auto_reconnect, false | pos_integer()}
                | {on_reconnect, reconn_callback()}
                | tuple().
