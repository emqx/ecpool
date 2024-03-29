-type callback() :: {module(), atom(), [any()]} | fun((any()) -> any()).
-type action() :: {module(), atom(), [any()]} | fun((pid()) -> any()).
-type apply_mode() :: handover
    | handover_async
    | {handover, timeout()}
    | {handover_async, callback()}
    | no_handover.
-type pool_type() :: random | hash | direct | round_robin.
-type conn_callback() :: {module(), atom(), [any()]}.
-type option() :: {pool_size, pos_integer()}
    | {pool_type, pool_type()}
    | {auto_reconnect, false | pos_integer()}
    | {on_reconnect, conn_callback()}
    | {on_disconnect, conn_callback()}
    | tuple().

