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

-define(POOL, test_pool).

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
       t_start_sup_pool,
       t_start_good_sup_pool,
       t_start_bad_sup_pool_1,
       t_start_bad_sup_pool_2,
       t_restart_client,
       t_reconnect_client,
       t_client_exec_hash,
       t_client_exec2_hash,
       t_client_exec_random,
       t_client_exec2_random,
       t_multiprocess_client,
       t_multiprocess_client_not_restart
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
    ?assertEqual(10, length(ecpool:workers(test_pool))),
    ?debugFmt("~p~n",  [ecpool:workers(test_pool)]),
    lists:foreach(fun(I) ->
                      ecpool:with_client(?POOL, fun(Client) ->
                                                        ?debugFmt("Call ~p: ~p~n", [I, Client])
                                                end)
                  end, lists:seq(1, 10)).

t_start_sup_pool(_Config) ->
    {ok, Pid1} = ecpool:start_sup_pool(xpool, test_client, ?POOL_OPTS),
    {ok, Pid2} = ecpool:start_sup_pool(ypool, test_client, ?POOL_OPTS),
    ?assertEqual([{xpool, Pid1}, {ypool, Pid2}], lists:sort(ecpool_sup:pools())),
    ecpool:stop_sup_pool(ypool),
    ecpool:stop_sup_pool(xpool),
    ?assertEqual([], ecpool_sup:pools()).

t_start_good_sup_pool(_Config) ->
    Succ = fun() ->
        case rand:uniform(1000) rem 2 of
            0 -> ok;
            1 ->
                %% we'll set the start_worker_timeout to 1s, so should not timeout
                timer:sleep(300)
        end,
        {ok, ecpool_test_client:spawn_dummy_proc()}
    end,
    Opts = [
        {pool_size, 1000},
        {pool_type, random},
        {auto_reconnect, false},
        {start_worker_timeout, 1000},
        {connect_fun, {Succ, []}}
    ],
    {ok, Pid1} = ecpool:start_sup_pool(xpool, ecpool_test_client, Opts),
    ?assertEqual([{xpool, Pid1}], lists:sort(ecpool_sup:pools())),
    From = self(),
    ecpool:with_client(xpool, fun(Client) -> Client ! {echo, {From, hello}} end),
    receive
        hello -> ok
    after
        1000 ->
            ct:fail(wait_reply_timeout)
    end,
    ok = ecpool:stop_sup_pool(xpool),
    ?assertEqual([], ecpool_sup:pools()).

t_start_bad_sup_pool_1(_Config) ->
    RandFail = fun() ->
        case rand:uniform(1000) rem 3 of
            0 -> {ok, ecpool_test_client:spawn_dummy_proc()};
            1 -> throw(connect_failed);
            2 -> {error, connect_failed}
        end
    end,
    Opts = [
        {pool_size, 1000},
        {pool_type, random},
        {auto_reconnect, false},
        {start_worker_timeout, 1000},
        {connect_fun, {RandFail, []}}
    ],
    {error, _} = ecpool:start_sup_pool(xpool, ecpool_test_client, Opts),
    ?assertEqual([], lists:sort(ecpool_sup:pools())),
    {error, not_found} = ecpool:stop_sup_pool(xpool),
    ?assertEqual([], ecpool_sup:pools()).

t_start_bad_sup_pool_2(_Config) ->
    TimeoutFun = fun() ->
        timer:sleep(1200), %% this will causes ecpool timeout on starting workers
        {ok, ecpool_test_client:spawn_dummy_proc()}
    end,
    Opts = [
        {pool_size, 1000},
        {pool_type, random},
        {auto_reconnect, false},
        {start_worker_timeout, 1000},
        {connect_fun, {TimeoutFun, []}}
    ],
    {error, _} = ecpool:start_sup_pool(xpool, ecpool_test_client, Opts),
    ?assertEqual([], lists:sort(ecpool_sup:pools())),
    {error, not_found} = ecpool:stop_sup_pool(xpool),
    ?assertEqual([], ecpool_sup:pools()).

t_restart_client(_Config) ->
    ecpool:start_pool(?POOL, test_client, [{pool_size, 4}]),
    ?assertEqual(4, length(ecpool:workers(?POOL))),

    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, normal) end),
    timer:sleep(50),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(3, length(ecpool:workers(?POOL))),

    ecpool:with_client(?POOL, fun(Client) -> test_client:stop(Client, {shutdown, x}) end),
    timer:sleep(50),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(2, length(ecpool:workers(?POOL))),

    ecpool:with_client(?POOL, fun(Client) ->
                                      test_client:stop(Client, badarg)
                              end),
    timer:sleep(100),
    ?debugFmt("~n~p~n", [ecpool:workers(?POOL)]),
    ?assertEqual(2, length(ecpool:workers(?POOL))).

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
