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
        , get_pool/1
        , check_pool_integrity/1
        ]).

-export([pools/0]).

%% Supervisor callbacks
-export([init/1]).

-type pool_name() :: ecpool:pool_name().

%% @doc Start supervisor.
-spec(start_link() -> {ok, pid()} | {error, term()}).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% Start/Stop a pool
%%--------------------------------------------------------------------

%% @doc Start a pool.
-spec(start_pool(pool_name(), atom(), [term()]) -> {ok, pid()} | {error, term()}).
start_pool(Pool, Mod, Opts) ->
    supervisor:start_child(?MODULE, pool_spec(Pool, Mod, Opts)).

%% @doc Stop a pool.
-spec(stop_pool(Pool :: pool_name()) -> ok | {error, term()}).
stop_pool(Pool) ->
    ChildId = child_id(Pool),
	case supervisor:terminate_child(?MODULE, ChildId) of
        ok ->
            supervisor:delete_child(?MODULE, ChildId);
        {error, Reason} ->
            {error, Reason}
	end.

%% @doc Get a pool.
-spec(get_pool(pool_name()) -> undefined | pid()).
get_pool(Pool) ->
    ChildId = child_id(Pool),
    case [Pid || {Id, Pid, supervisor, _} <- supervisor:which_children(?MODULE), Id =:= ChildId] of
        [] -> undefined;
        L  -> hd(L)
    end.

-spec check_pool_integrity(pool_name) ->
    ok | {error, {processes_down, [term()]} | not_found}.
check_pool_integrity(Pool) ->
    case get_pool(Pool) of
        undefined ->
            {error, not_found};
        SupPid when is_pid(SupPid) ->
            do_check_pool_integrity_root(SupPid)
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
      start => {ecpool_pool_sup,
                start_link,
                [Pool, Mod, Opts]},
      restart => transient,
      shutdown => infinity,
      type => supervisor,
      modules => [ecpool_pool_sup]}.

child_id(Pool) -> {pool_sup, Pool}.

%%--------------------------------------------------------------------
%% Internal fns
%%--------------------------------------------------------------------

do_check_pool_integrity_root(SupPid) ->
    try supervisor:which_children(SupPid) of
        Children ->
            %% We ignore `restarting` here because those processes are still being
            %% managed.
            DeadChildren = [Id || {Id, undefined, _, _} <- Children],
            %% Currently, at root, we only have one supervisor: `ecpool_worker_sup`, and
            %% it does not contain other supervisors under it, so no need to dig deeper.
            case DeadChildren of
                [_ | _] ->
                    {error, {processes_down, DeadChildren}};
                [] ->
                    ok
            end
    catch
        exit:{noproc, _} ->
            {error, {processes_down, [root]}}
    end.
