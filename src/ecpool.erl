%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(ecpool).

-export([ pool_spec/4
        , start_pool/3
        , start_sup_pool/3
        , stop_sup_pool/1
        , get_client/1
        , get_client/2
        , pick_and_do/3
        , name/1
        , workers/1
        ]).

-export([set_reconnect_callback/2,
         add_reconnect_callback/2,
         remove_reconnect_callback/2]).

%% NOTE: Obsolete APIs.
%% Use pick_and_do/3 APIs instead
-export([ with_client/2
        , with_client/3
        ]).

-export_type([
    pool_name/0,
    callback/0,
    action/0,
    apply_mode/0,
    pool_type/0,
    conn_callback/0,
    option/0
]).

%% cannot be tuple
-type pool_name() :: atom() | binary().

-type callback() :: {module(), atom(), [any()]} | fun((any()) -> any()).
-type action(Result) :: {module(), atom(), [any()]} | fun((_Client :: pid()) -> Result).
-type action() :: action(any()).
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
-type get_client_ret() :: pid() | false | no_such_pool.

-define(IS_ACTION(ACTION), ((is_tuple(ACTION) andalso tuple_size(ACTION) == 3) orelse is_function(ACTION, 1))).

pool_spec(ChildId, Pool, Mod, Opts) ->
    #{id => ChildId,
      start => {?MODULE, start_pool, [Pool, Mod, Opts]},
      restart => permanent,
      shutdown => 5000,
      type => supervisor,
      modules => [ecpool_pool_sup]}.

%% @doc Start the pool sup.
-spec(start_pool(pool_name(), atom(), [option()]) -> {ok, pid()} | {error, term()}).
start_pool(Pool, Mod, Opts) ->
    %% See start_sup_pool/3 for an explanation of InitialConnectResponseRef
    InitialConnectResponseRef = make_ref(),
    CollectorProcessPID = spawn_initial_connect_result_collector_process(self(),
                                                                         InitialConnectResponseRef,
                                                                         Opts),
    {ok, Pid} = OkResponse = ecpool_pool_sup:start_link(Pool,
                                                        Mod,
                                                        Opts,
                                                        {CollectorProcessPID, InitialConnectResponseRef}),
    case get_result_from_collector_process(InitialConnectResponseRef) of
        ok ->
           OkResponse ;
        Error ->
            unlink(Pid),
            exit(Pid, shutdown),
            Error
    end.

%% @doc Start the pool supervised by ecpool_sup
start_sup_pool(Pool, Mod, Opts) ->
    %% To avoid confusing OTP crash report log entries, supervisors and the
    %% gen_server ecpool_worker should never return a non-ok response from the
    %% init function. Instead, to handle errors from the initial connect
    %% attempt, we pass our process ID and a response reference to
    %% ecpool_worker.
    InitialConnectResponseRef = make_ref(),
    CollectorProcessPID = spawn_initial_connect_result_collector_process(self(),
                                                                         InitialConnectResponseRef,
                                                                         Opts),
    OkResponse = ecpool_sup:start_pool(Pool,
                                       Mod,
                                       Opts,
                                       {CollectorProcessPID, InitialConnectResponseRef}),
    case get_result_from_collector_process(InitialConnectResponseRef) of
        ok ->
           OkResponse;
        Error ->
            _ = stop_sup_pool(Pool),
            Error
    end.

%% @doc Stop the pool supervised by ecpool_sup
stop_sup_pool(Pool) ->
    ecpool_sup:stop_pool(Pool).

%% @doc Get client/connection
-spec(get_client(pool_name()) -> get_client_ret()).
get_client(Pool) ->
    try gproc_pool:pick_worker(name(Pool))
    catch
        error:badarg -> no_such_pool
    end.

%% @doc Get client/connection with hash key.
-spec(get_client(pool_name(), any()) -> get_client_ret()).
get_client(Pool, Key) ->
    try gproc_pool:pick_worker(name(Pool), Key)
    catch
        error:badarg -> no_such_pool
    end.

-spec(set_reconnect_callback(pool_name(), conn_callback()) -> ok).
set_reconnect_callback(Pool, Callback) ->
    [ecpool_worker:set_reconnect_callback(Worker, Callback)
     || {_WorkerName, Worker} <- ecpool:workers(Pool)],
    ok.

-spec(add_reconnect_callback(pool_name(), conn_callback()) -> ok).
add_reconnect_callback(Pool, Callback) ->
    [ecpool_worker:add_reconnect_callback(Worker, Callback)
     || {_WorkerName, Worker} <- ecpool:workers(Pool)],
    ok.

-spec(remove_reconnect_callback(pool_name(), {module(), atom()}) -> ok).
remove_reconnect_callback(Pool, Callback) ->
    [ecpool_worker:remove_reconnect_callback(Worker, Callback)
     || {_WorkerName, Worker} <- ecpool:workers(Pool)],
    ok.

%% NOTE: Use pick_and_do/3 instead of with_client/2,3
%%   to avoid applying action failure with 'badfun'.
%%
%% @doc Call the fun with client/connection
-spec with_client(pool_name(), action(Result)) ->
    Result | {error, disconnected | ecpool_empty}.
with_client(Pool, Fun) when ?IS_ACTION(Fun) ->
    with_worker(get_client(Pool), Fun, no_handover).

%% @doc Call the fun with client/connection
-spec with_client(pool_name(), any(), action(Result)) ->
    Result | {error, disconnected | ecpool_empty}.
with_client(Pool, Key, Fun) when ?IS_ACTION(Fun) ->
    with_worker(get_client(Pool, Key), Fun, no_handover).

-spec pick_and_do({pool_name(), term()} | pool_name(), action(Result), apply_mode()) ->
    Result | {error, disconnected | ecpool_empty}.
pick_and_do({Pool, KeyOrNum}, Action, ApplyMode) when ?IS_ACTION(Action) ->
    with_worker(get_client(Pool, KeyOrNum), Action, ApplyMode);
pick_and_do(Pool, Action, ApplyMode) when ?IS_ACTION(Action) ->
    with_worker(get_client(Pool), Action, ApplyMode).

-spec with_worker(get_client_ret(), action(Result), apply_mode()) ->
    Result | {error, disconnected | ecpool_empty}.
with_worker(no_such_pool, Action, _Mode) when ?IS_ACTION(Action) ->
    {error, no_such_pool};
with_worker(false, Action, _Mode) when ?IS_ACTION(Action) ->
    {error, ecpool_empty};
with_worker(Worker, Action, no_handover) when ?IS_ACTION(Action) ->
    case ecpool_monitor:get_client_global(Worker) of
        {ok, Client} ->
            exec(Action, Client);
        {error, _} ->
            case ecpool_worker:client(Worker) of
                {ok, Client} -> exec(Action, Client);
                {error, _} = Err -> Err
            end
    end;
with_worker(Worker, Action, handover) when ?IS_ACTION(Action) ->
    ecpool_worker:exec(Worker, Action, infinity);
with_worker(Worker, Action, {handover, Timeout}) when is_integer(Timeout) andalso ?IS_ACTION(Action) ->
    ecpool_worker:exec(Worker, Action, Timeout);
with_worker(Worker, Action, handover_async) when ?IS_ACTION(Action) ->
    ecpool_worker:exec_async(Worker, Action);
with_worker(Worker, Action, {handover_async, CallbackFun = {_,_,_}}) when ?IS_ACTION(Action) ->
    ecpool_worker:exec_async(Worker, Action, CallbackFun).

%% @doc Pool workers
workers(Pool) ->
    gproc_pool:active_workers(name(Pool)).

%% @doc ecpool name
name(Pool) -> {?MODULE, Pool}.

exec({M, F, A}, Client) ->
    erlang:apply(M, F, [Client]++A);
exec(Action, Client) when is_function(Action) ->
    Action(Client).

%% Internal functions

%% Since ecpool_workers that are restarted could send more initial connect
%% response results after the first initialization, we collect responses in a
%% separte process to not get unhandled messages in the callers mailbox
spawn_initial_connect_result_collector_process(CallerPid, InitialConnectResponseRef, Opts) ->
    spawn(fun() ->
                  Result = aggregate_initial_connect_responses(InitialConnectResponseRef, Opts),
                  CallerPid ! {InitialConnectResponseRef, Result}
          end).

get_result_from_collector_process(InitialConnectResponseRef) ->
    receive
        {InitialConnectResponseRef, Result} ->
            Result
    end.

aggregate_initial_connect_responses(InitialConnectResponseRef, Opts) ->
    PoolSize = ecpool_worker_sup:pool_size(Opts),
    aggregate_initial_connect_responses_helper(PoolSize,
                                               InitialConnectResponseRef,
                                               ok).

aggregate_initial_connect_responses_helper(0 = _RespLeft, _RespRef, CurrResp) ->
    CurrResp;
aggregate_initial_connect_responses_helper(RespLeft, RespRef, ok = _CurrResp) ->
    receive
        {Ref, Resp} when Ref =:= RespRef ->
            aggregate_initial_connect_responses_helper(RespLeft - 1, RespRef, Resp)
    end;
aggregate_initial_connect_responses_helper(RespLeft, RespRef, CurrResp) ->
    receive
        {Ref, _Resp} when Ref =:= RespRef ->
            %% We keep the current response if it is something different than ok
            aggregate_initial_connect_responses_helper(RespLeft - 1, RespRef, CurrResp)
    end.


