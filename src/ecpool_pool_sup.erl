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

-module(ecpool_pool_sup).

-behaviour(supervisor).

%% API
-export([start_link/3]).

%% Supervisor callbacks
-export([init/1]).

-export([clear_init_incomplete/0]).

start_link(Pool, Mod, Opts) ->
    case supervisor:start_link(?MODULE, [Pool, Mod, Opts]) of
        {ok, SupPid} = Ret ->
            _ = init_complete(SupPid),
            Ret;
        Error ->
            Error
    end.

init([Pool, Mod, Opts]) ->
    set_init_incomplete(),
    {ok, { {one_for_all, 10, 100}, [
            {pool, {ecpool_pool, start_link, [Pool, Opts]},
                transient, 16#ffff, worker, [ecpool_pool]},
            {worker_sup, {ecpool_worker_sup, start_link, [Pool, Mod, Opts]},
                transient, infinity, supervisor, [ecpool_worker_sup]}] }}.

init_complete(SupPid) ->
    %% Not a real supervisor child, just for clearing the process dict with key `init_incomplete`
    DummySpec = #{
        id => clear_init_incomplete,
        start => {?MODULE, clear_init_incomplete, []},
        restart => temporary,
        shutdown => brutal_kill,
        type => worker,
        modules => [?MODULE]
    },
    supervisor:start_child(SupPid, DummySpec).

set_init_incomplete() ->
    _ = erlang:put(init_incomplete, true),
    ok.

clear_init_incomplete() ->
    _ = erlang:erase(init_incomplete),
    {error, dummy_child}.
