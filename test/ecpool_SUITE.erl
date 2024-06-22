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

-module(ecpool_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(POOL, <<"test_pool">>).

-define(POOL_OPTS,
        [%% schedulers number
         {pool_size, 10},
         %% round-robbin | random | hash
         {pool_type, random},
         %% false | pos_integer()
         {auto_reconnect, false},

         %% DB Parameters
         {host, "localhost"},
         {port, 5432},
         {username, "feng"},
         {password, ""},
         {database, "mqtt"},
         {encoding,  utf8}
        ]).

all() ->
    [{group, all}].

groups() ->
    [{all, [sequence],
      [t_start_pool,
       t_start_pool_any_name,
       t_start_sup_pool,
       t_empty_pool,
       t_empty_hash_pool,
       t_restart_client,
       t_reconnect_client,
       t_client_exec_hash,
       t_client_exec2_hash,
       t_client_exec_random,
       t_client_exec2_random,
       t_multiprocess_client,
       t_multiprocess_client_not_restart,
       t_pick_and_do_fun
      ]}].

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(ecpool),
    Config.

end_per_suite(_Config) ->
    ok = application:stop(ecpool),
    ok = application:stop(gproc).

t_start_pool(_Config) ->
    ecpool:start_pool(?POOL, test_client, ?POOL_OPTS),
    ?assertEqual(10, length(ecpool:workers(?POOL))),
    ?debugFmt("~p~n",  [ecpool:workers(?POOL)]),
    lists:foreach(fun(I) ->
                      ecpool:with_client(?POOL, fun(Client) ->
                                                        ?debugFmt("Call ~p: ~p~n", [I, Client])
                                                end)
                  end, lists:seq(1, 10)).

t_start_pool_any_name(_Config) ->
    PoolName = {<<"a">>, b, [#{c => 1}]},
    {ok, _} = ecpool:start_pool(PoolName, test_client, ?POOL_OPTS),
    ?assertEqual(10, length(ecpool:workers(PoolName))),
    lists:foreach(fun(_I) ->
            ecpool:with_client(PoolName,
            fun(Client) ->
                ?assert(is_pid(Client))
            end)
        end, lists:seq(1, 10)).

t_empty_pool(_Config) ->
    ecpool:start_pool(?POOL, test_failing_client, [{pool_size, 4}, {pool_type, random}]),
    % NOTE: give some time to clients to exit
    ok = timer:sleep(100),
    ?assertEqual([], ecpool:workers(?POOL)),
    ?assertEqual({error, ecpool_empty}, ecpool:with_client(?POOL, fun(_) -> ok end)).

t_empty_hash_pool(_Config) ->
    ecpool:start_pool(?POOL, test_failing_client, [{pool_size, 4}, {pool_type, hash}]),
    % NOTE: give some time to clients to exit
    ok = timer:sleep(100),
    ?assertEqual([], ecpool:workers(?POOL)),
    ?assertEqual({error, ecpool_empty}, ecpool:with_client(?POOL, 42, fun(_) -> ok end)).

t_start_sup_pool(_Config) ->
    {ok, Pid1} = ecpool:start_sup_pool(xpool, test_client, ?POOL_OPTS),
    {ok, Pid2} = ecpool:start_sup_pool(ypool, test_client, ?POOL_OPTS),
    ?assertEqual([{xpool, Pid1}, {ypool, Pid2}], lists:sort(ecpool_sup:pools())),
    ecpool:stop_sup_pool(ypool),
    ecpool:stop_sup_pool(xpool),
    ?assertEqual([], ecpool_sup:pools()).

t_restart_client(_Config) ->
    ecpool:start_pool(?POOL, test_client, [{pool_size, 4}]),
    Workers1 = ecpool:workers(?POOL),
    ?assertEqual(4, length(Workers1)),

    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, normal) end),
    timer:sleep(50),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(3, length(ecpool:workers(?POOL))),

    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, {shutdown, x}) end),
    timer:sleep(50),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(2, length(ecpool:workers(?POOL))),

    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, badarg) end),
    timer:sleep(50),
    Workers2 = ecpool:workers(?POOL),
    ?debugFmt("~n~p~n", [Workers2]),
    ?assertEqual(1, length(Workers2)),

    lists:foreach(fun({_, WorkerPid}) ->
            ?assertMatch({ok, _}, ecpool_monitor:get_client_global(WorkerPid))
        end, Workers2),

    lists:foreach(fun({_, WorkerPid}) ->
            ?assertMatch({error, ecpool_client_disconnected},
                ecpool_monitor:get_client_global(WorkerPid))
        end, Workers1 -- Workers2).

t_reconnect_client(_Config) ->
    ecpool:start_pool(?POOL, test_client, [{pool_size, 4}, {auto_reconnect, 1}]),
    ?assertEqual(4, length(ecpool:workers(?POOL))),
    ecpool:with_client(?POOL, fun(Client) ->
                                      test_client:stop(Client, normal)
                              end),
    ?assert(lists:member(false, [ecpool_worker:is_connected(Pid) || {_, Pid} <- ecpool:workers(?POOL)])),
    timer:sleep(1100),
    ?assertNot(lists:member(false, [ecpool_worker:is_connected(Pid) || {_, Pid} <- ecpool:workers(?POOL)])),
    ecpool:with_client(?POOL, fun(Client) ->
                                      test_client:stop(Client, {shutdown, badarg})
                              end),
    ?assert(lists:member(false, [ecpool_worker:is_connected(Pid) || {_, Pid} <- ecpool:workers(?POOL)])),
    timer:sleep(1100),
    ?assertEqual(4, length(ecpool:workers(?POOL))).

t_multiprocess_client(_Config) ->
    ecpool:start_pool(?POOL, test_client, [{pool_size, 4}, {auto_reconnect, 1}, {multiprocess, true}]),
    ?assertEqual(4, length(ecpool:workers(?POOL))),
    %% check client status
    [begin
        true = ecpool_worker:is_connected(Worker),
        {ok, _C = {Pid1, Pid2}} = ecpool_worker:client(Worker),
        true = is_process_alive(Pid1),
        true = is_process_alive(Pid2)
     end || {_WorkerName, Worker} <- ecpool:workers(?POOL)],

    %% stop one of the clients
    Client = ecpool:with_client(?POOL,
                    fun(C = {P1, _P2}) ->
                        test_client:stop(P1, normal), C
                    end),
    ct:sleep(1500),

    %% check that client is reconnected
    [begin
        true = ecpool_worker:is_connected(Worker),
        {ok, Client2 = {Pid3, Pid4}} = ecpool_worker:client(Worker),
        true = is_process_alive(Pid3),
        true = is_process_alive(Pid4),
        ?assert(Client2 =/= Client)
     end || {_WorkerName, Worker} <- ecpool:workers(?POOL)].

t_multiprocess_client_not_restart(_Config) ->
    ecpool:start_pool(?POOL, test_client, [{pool_size, 4}, {auto_reconnect, false}, {multiprocess, true}]),
    ?assertEqual(4, length(ecpool:workers(?POOL))),
    %% check client status
    [begin
        true = ecpool_worker:is_connected(Worker),
        {ok, {Pid1, Pid2}} = ecpool_worker:client(Worker),
        true = is_process_alive(Pid1),
        true = is_process_alive(Pid2)
     end || {_WorkerName, Worker} <- ecpool:workers(?POOL)],

    %% stop all the clients
    [begin
        true = ecpool_worker:is_connected(Worker),
        {ok, {Pid1, _Pid2}} = ecpool_worker:client(Worker),
        test_client:stop(Pid1, normal)
     end || {_WorkerName, Worker} <- ecpool:workers(?POOL)],
    ct:sleep(1500),

    %% check that all the clients are disconnected and not restarted.
    [begin
        ?assertEqual(false, ecpool_worker:is_connected(Worker))
     end || {_WorkerName, Worker} <- ecpool:workers(?POOL)].

t_client_exec_hash(_Config) ->
    Opts = [{pool_size, 5}, {pool_type, hash}, {auto_reconnect, false}],
    {ok, Pid1} = ecpool:start_pool(abc, test_client, Opts),
    ct:pal("----- pid: ~p", [is_process_alive(Pid1)]),
    ?assertEqual(4, ecpool:with_client(abc, <<"abc">>, fun(Client) ->
                        test_client:plus(Client, 1, 3)
                    end)),
    ?assertEqual(4, ecpool:with_client(abc, 2, fun(Client) ->
                        test_client:plus(Client, 1, 3)
                    end)),
    ecpool:stop_sup_pool(abc).

t_client_exec2_hash(_Config) ->
    Action = {test_client, plus, [1,3]},
    Opts = [{pool_size, 5}, {pool_type, hash}, {auto_reconnect, false}],
    {ok, Pid1} = ecpool:start_pool(abc, test_client, Opts),
    ct:pal("----- pid: ~p", [is_process_alive(Pid1)]),
    ?assertEqual(4, ecpool:pick_and_do({abc, <<"abc">>}, Action, no_handover)),
    ?assertEqual(4, ecpool:pick_and_do({abc, 3}, Action, no_handover)),
    Callback = {test_client, callback, [self()]},
    ?assertEqual(4, ecpool:pick_and_do({abc, <<"abc">>}, Action, handover)),
    ?assertEqual(ok, ecpool:pick_and_do({abc, 1}, Action, handover_async)),
    ?assertEqual(ok, ecpool:pick_and_do({abc, <<"abc">>}, Action, {handover_async, Callback})),
    receive
        {result, 4} -> ok;
        R2 -> ct:fail({unexpected_result, R2})
    end,
    ecpool:stop_sup_pool(abc).

t_client_exec_random(_Config) ->
    ecpool:start_pool(?POOL, test_client, ?POOL_OPTS),
    ?assertEqual(4, ecpool:with_client(?POOL, fun(Client) ->
                        test_client:plus(Client, 1, 3)
                    end)).

t_client_exec2_random(_Config) ->
    Action = {test_client, plus, [1,3]},
    ecpool:start_pool(?POOL, test_client, ?POOL_OPTS),
    ?assertEqual(4, ecpool:pick_and_do(?POOL, Action, no_handover)),
    ?assertEqual(4, ecpool:pick_and_do(?POOL, Action, handover)),
    ?assertEqual(ok, ecpool:pick_and_do(?POOL, Action, handover_async)),
    Callback = {test_client, callback, [self()]},
    ?assertEqual(ok, ecpool:pick_and_do(?POOL, Action, {handover_async, Callback})),
    receive
        {result, 4} -> ok;
        R1 -> ct:fail({unexpected_result, R1})
    end.

t_pick_and_do_fun(_Config) ->
    Pool = ?FUNCTION_NAME,
    Action = fun(Client) -> test_client:plus(Client, 1, 3) end,
    Opts = [{pool_size, 5}, {pool_type, hash}, {auto_reconnect, false}],
    {ok, _} = ecpool:start_pool(Pool, test_client, Opts),
    ?assertEqual(4, ecpool:pick_and_do({Pool, <<"abc">>}, Action, no_handover)),
    ecpool:stop_sup_pool(Pool).
