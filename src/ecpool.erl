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
        , stop_sup_pool/2
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
    ecpool_pool_sup:start_link(Pool, Mod, Opts).

%% @doc Start the pool supervised by ecpool_sup
start_sup_pool(Pool, Mod, Opts) ->
    ecpool_sup:start_pool(Pool, Mod, Opts).

%% @doc Start the pool supervised by ecpool_sup
stop_sup_pool(Pool) ->
    ecpool_sup:stop_pool(Pool).

stop_sup_pool(Pool, Opts) ->
    ecpool_sup:stop_pool(Pool, Opts).

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
