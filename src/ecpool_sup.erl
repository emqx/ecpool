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

-module(ecpool_sup).

-behaviour(supervisor).

-export([start_link/0]).

%% API
-export([ start_pool/3
        , stop_pool/1
        , stop_pool/2
        , get_pool/1
        ]).

-export([pools/0]).

%% Supervisor callbacks
-export([init/1]).

-type pool_name() :: ecpool:pool_name().
-type stop_opts() :: #{timeout => non_neg_integer()}.

-define(STOP_TIMOEUT, infinity).

%% @doc Start supervisor.
-spec(start_link() -> {ok, pid()} | {error, term()}).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% Start/Stop a pool
%%--------------------------------------------------------------------

%% @doc Start a pool.
-spec(start_pool(pool_name(), atom(), list(tuple())) -> {ok, pid()} | {error, term()}).
start_pool(Pool, Mod, Opts) ->
    supervisor:start_child(?MODULE, pool_spec(Pool, Mod, Opts)).

%% @doc Stop a pool.
-spec(stop_pool(Pool :: pool_name()) -> ok | {error, term()}).
stop_pool(Pool) ->
    stop_pool(Pool, #{}).

-spec(stop_pool(Pool :: pool_name(), stop_opts()) -> ok | {error, term()}).
stop_pool(Pool, Opts) ->
    ChildId = child_id(Pool),
    Timeout = maps:get(timeout, Opts, ?STOP_TIMOEUT),
    try gen_server:call(?MODULE, {terminate_child, ChildId}, Timeout) of
        ok -> delete_child(ChildId, Timeout);
        {error, Reason} -> {error, Reason}
    catch
        exit:{R, _} when R == noproc; R == normal; R == shutdown ->
            {error, not_found};
        exit:{timeout, _} ->
            %% Sometimes the `ecpool_sup` is not responding to terminate request as the `ecpool_pool_sup`
            %% process got stuck in connecting. In this case, we need to cancel the connection
            %% by force killing it so the `ecpool_sup` won't be stuck.
            _ = kill_ecpool_pool_sup_if_stuck(),
            %% Now the `ecpool_pool_sup` process can be in one of the following state:
            %%   - has been force killed, or
            %%   - has gone down by itself or by the `terminate_child` call, or
            %%   - is still running normally
            %% In any case we try to remove it from childspec as we have sent a `terminate_child`.
            delete_child(ChildId, Timeout)
    end.

%% @doc Get a pool.
-spec(get_pool(pool_name()) -> undefined | pid()).
get_pool(Pool) ->
    ChildId = child_id(Pool),
    case [Pid || {Id, Pid, supervisor, _} <- supervisor:which_children(?MODULE), Id =:= ChildId] of
        [] -> undefined;
        L  -> hd(L)
    end.

%% @doc Get All Pools supervisored by the ecpool_sup.
-spec(pools() -> [{pool_name(), pid()}]).
pools() ->
    [{Pool, Pid} || {{pool_sup, Pool}, Pid, supervisor, _}
                    <- supervisor:which_children(?MODULE)].

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 10, 100}, [ecpool_monitor:monitor_spec()]}}.

pool_spec(Pool, Mod, Opts) ->
    #{id => child_id(Pool),
      start => {ecpool_pool_sup, start_link, [Pool, Mod, Opts]},
      restart => transient,
      shutdown => infinity,
      type => supervisor,
      modules => [ecpool_pool_sup]}.

child_id(Pool) -> {pool_sup, Pool}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

kill_ecpool_pool_sup_if_stuck() ->
    case process_info(whereis(?MODULE), links) of
        {links, LinkedPids} ->
            case search_stuck_ecpool_pool_sup(LinkedPids) of
                {ok, Pid} ->
                    exit(Pid, kill),
                    {ok, Pid};
                Err ->
                    Err
            end;
        undefined ->
            {error, not_found}
    end.

search_stuck_ecpool_pool_sup([]) ->
    {error, not_found};
search_stuck_ecpool_pool_sup([Pid | Rest]) ->
    case process_info(Pid, dictionary) of
        {dictionary, Dicts} ->
            case proplists:get_value('$initial_call', Dicts) of
                {supervisor, ecpool_pool_sup, _} ->
                    case proplists:get_value(init_incomplete, Dicts) of
                        true -> {ok, Pid};
                        _ -> {error, {not_stuck_in_init, Pid}}
                    end;
                _ ->
                    search_stuck_ecpool_pool_sup(Rest)
            end;
        undefined ->
            search_stuck_ecpool_pool_sup(Rest)
    end.

delete_child(ChildId, Timeout) ->
    try gen_server:call(?MODULE, {delete_child, ChildId}, Timeout)
    catch
        exit:{R, _} when R == noproc; R == normal; R == shutdown ->
            {error, not_found};
        exit:{timeout, _} ->
            {error, timeout}
    end.
