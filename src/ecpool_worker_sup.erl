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

-module(ecpool_worker_sup).

-behaviour(supervisor).

-export([start_link/3]).

-export([init/1]).

-define(START_TIMEOUT, 30000).

start_link(Pool, Mod, Opts) when is_atom(Pool) ->
    case supervisor:start_link(?MODULE, [Pool, Mod, Opts, self()]) of
        {ok, Pid} ->
            case wait_connect_complete(pool_size(Opts), start_worker_timeout(Opts)) of
                ok -> {ok, Pid};
                {error, _} = Err ->
                    exit(Pid, {shutdown, some_workers_failed}),
                    Err
            end;
        {error, _} = Err -> Err
    end.

init([Pool, Mod, Opts, Parent]) ->
    WorkerSpec = fun(Id) ->
                     #{id => {worker, Id},
                       start => {ecpool_worker, start_link, [Pool, Id, Mod, Opts, Parent]},
                       restart => transient,
                       shutdown => 5000,
                       type => worker,
                       modules => [ecpool_worker, Mod]}
                 end,
    Workers = [WorkerSpec(I) || I <- lists:seq(1, pool_size(Opts))],
    {ok, { {one_for_one, 10, 60}, Workers} }.

pool_size(Opts) ->
    Schedulers = erlang:system_info(schedulers),
    proplists:get_value(pool_size, Opts, Schedulers).

start_worker_timeout(Opts) ->
    proplists:get_value(start_worker_timeout, Opts, ?START_TIMEOUT).

wait_connect_complete(PoolSize, Timeout) when PoolSize > 0 ->
    try
        Responses = collect_worker_responses(PoolSize, Timeout),
        case lists:filter(fun(R) -> R =/= connect_complete end, Responses) of
            [] -> ok;
            [{error, FirstError} | _] -> {error, FirstError}
        end
    catch
        throw:timeout ->
            logger:log(error, "[ecpool_worker_sup] wait_connect_complete timeout"),
            {error, timeout}
    end.

collect_worker_responses(PoolSize, Timeout) ->
    [
        receive {ecpool_worker_reply, Resp} -> Resp
        after Timeout -> %% the overall timeout waiting for all workers
            throw(timeout)
        end || _ <- lists:seq(1, PoolSize)
    ].
