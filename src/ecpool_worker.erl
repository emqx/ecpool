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

-module(ecpool_worker).

-behaviour(gen_server).

-export([start_link/4]).

%% API Function Exports
-export([ client/1
        , exec/3
        , exec_async/2
        , exec_async/3
        , is_connected/1
        , set_reconnect_callback/2
        , set_disconnect_callback/2
        , add_reconnect_callback/2
        , remove_reconnect_callback/2
        , remove_reconnect_callback_by_signature/2
        , get_reconnect_callbacks/1
        , add_disconnect_callback/2
        ]).

%% gen_server Function Exports
-export([ init/1
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , terminate/2
        , code_change/3
        ]).

-type pool_name() :: ecpool:pool_name().

-record(state, {
          pool :: pool_name(),
          id :: pos_integer(),
          client :: pid() | {pid(), pid()} | undefined,
          mod :: module(),
          on_reconnect :: [ecpool:conn_callback()],
          on_disconnect :: [ecpool:conn_callback()],
          supervisees = [],
          opts :: proplists:proplist()
         }).

%%--------------------------------------------------------------------
%% Callback
%%--------------------------------------------------------------------

-callback(connect(ConnOpts :: list())
          -> {ok, pid()} | {ok, {pid(), pid()}, map()} | {error, Reason :: term()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start a pool worker.
-spec(start_link(pool_name(), pos_integer(), module(), list()) ->
      {ok, pid()} | ignore | {error, any()}).
start_link(Pool, Id, Mod, Opts) ->
    gen_server:start_link(?MODULE, [Pool, Id, Mod, Opts], []).

%% @doc Get client/connection.
-spec(client(pid()) -> {ok, Client :: pid()} | {ok, {Client :: pid(), pid()}} | {error, Reason :: term()}).
client(Pid) ->
    gen_server:call(Pid, client, infinity).

-spec(exec(pid(), ecpool:action(), timeout()) -> Result :: any() | {error, Reason :: term()}).
exec(Pid, Action, Timeout) ->
    gen_server:call(Pid, {exec, Action}, Timeout).

-spec exec_async(pid(), ecpool:action()) -> ok.
exec_async(Pid, Action) ->
    gen_server:cast(Pid, {exec_async, Action}).

-spec exec_async(pid(), ecpool:action(), ecpool:callback()) -> ok.
exec_async(Pid, Action, Callback) ->
    gen_server:cast(Pid, {exec_async, Action, Callback}).

%% @doc Is client connected?
-spec(is_connected(pid()) -> boolean()).
is_connected(Pid) ->
    gen_server:call(Pid, is_connected, infinity).

-spec(set_reconnect_callback(pid(), ecpool:conn_callback()) -> ok).
set_reconnect_callback(Pid, OnReconnect) ->
    gen_server:cast(Pid, {set_reconn_callbk, OnReconnect}).

-spec(set_disconnect_callback(pid(), ecpool:conn_callback()) -> ok).
set_disconnect_callback(Pid, OnDisconnect) ->
    gen_server:cast(Pid, {set_disconn_callbk, OnDisconnect}).

-spec(add_reconnect_callback(pid(), ecpool:conn_callback()) -> ok).
add_reconnect_callback(Pid, OnReconnect) ->
    gen_server:cast(Pid, {add_reconn_callbk, OnReconnect}).

-spec(remove_reconnect_callback(pid(), {module(), atom()}) -> ok).
remove_reconnect_callback(Pid, OnReconnect) ->
    gen_server:cast(Pid, {remove_reconn_callbk, OnReconnect}).

-spec(remove_reconnect_callback_by_signature(pid(), term()) -> ok).
remove_reconnect_callback_by_signature(Pid, CallbackSignature) ->
    gen_server:cast(Pid, {remove_reconnect_callback_by_signature, CallbackSignature}).

-spec(get_reconnect_callbacks(pid()) -> [{module(), atom(), list()}]).
get_reconnect_callbacks(Pid) ->
    gen_server:call(Pid, get_reconnect_callbacks, infinity).

-spec(add_disconnect_callback(pid(), ecpool:conn_callback()) -> ok).
add_disconnect_callback(Pid, OnDisconnect) ->
    gen_server:cast(Pid, {add_disconn_callbk, OnDisconnect}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Mod, Opts]) ->
    ecpool_monitor:reg_worker(),
    process_flag(trap_exit, true),
    State = #state{pool = Pool,
                   id   = Id,
                   mod  = Mod,
                   opts = Opts,
                   on_reconnect = ensure_callback(proplists:get_value(on_reconnect, Opts)),
                   on_disconnect = ensure_callback(proplists:get_value(on_disconnect, Opts))
                  },
    case connect_internal(State) of
        {ok, NewState} ->
            gproc_pool:connect_worker(ecpool:name(Pool), {Pool, Id}),
            {ok, NewState};
        {error, Error} -> {stop, Error}
    end.

handle_call(is_connected, _From, State = #state{client = Client}) when is_pid(Client) ->
    IsAlive = Client =/= undefined andalso is_process_alive(Client),
    {reply, IsAlive, State};

handle_call(is_connected, _From, State = #state{client = Client}) ->
    {reply, Client =/= undefined, State};

handle_call(client, _From, State = #state{client = undefined}) ->
    {reply, {error, disconnected}, State};

handle_call(client, _From, State = #state{client = Client}) ->
    {reply, {ok, Client}, State};

handle_call({exec, Action}, _From, State = #state{client = Client}) ->
    {reply, safe_exec(Action, Client), State};

handle_call(get_reconnect_callbacks, _From, #state{on_reconnect = OnReconnect} = State) ->
    {reply, OnReconnect, State};

handle_call(Req, _From, State) ->
    logger:error("[PoolWorker] unexpected call: ~p", [Req]),
    {reply, ignored, State}.

handle_cast({exec_async, Action}, State = #state{client = Client}) ->
    _ = safe_exec(Action, Client),
    {noreply, State};

handle_cast({exec_async, Action, Callback}, State = #state{client = Client}) ->
    _ = safe_exec(Callback, safe_exec(Action, Client)),
    {noreply, State};

handle_cast({set_reconn_callbk, OnReconnect}, State) ->
    {noreply, State#state{on_reconnect = ensure_callback(OnReconnect)}};

handle_cast({set_disconn_callbk, OnDisconnect}, State) ->
    {noreply, State#state{on_disconnect = ensure_callback(OnDisconnect)}};

handle_cast({add_reconn_callbk, OnReconnect}, State = #state{on_reconnect = OldOnReconnect0}) ->
    OldOnReconnect =
        case reconnect_callback_signature(OnReconnect) of
            {ok, Signature} ->
                drop_reconnect_callbacks_by_signature(OldOnReconnect0, Signature);
            error ->
                OldOnReconnect0
        end,
    {noreply, State#state{on_reconnect = add_conn_callback(OnReconnect, OldOnReconnect)}};

handle_cast({remove_reconnect_callback_by_signature, CallbackSignature}, State = #state{on_reconnect = OnReconnect0}) ->
    OldOnReconnect = drop_reconnect_callbacks_by_signature(OnReconnect0, CallbackSignature),
    {noreply, State#state{on_reconnect = OldOnReconnect}};

handle_cast({remove_reconn_callbk, OnReconnect}, State = #state{on_reconnect = OldOnReconnect}) ->
    {noreply, State#state{on_reconnect = remove_conn_callback(OnReconnect, OldOnReconnect)}};

handle_cast({add_disconn_callbk, OnDisconnect}, State = #state{on_disconnect = OldOnDisconnect}) ->
    {noreply, State#state{on_disconnect = add_conn_callback(OnDisconnect, OldOnDisconnect)}};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, Reason}, State = #state{opts = Opts, supervisees = SupPids}) ->
    case lists:member(Pid, SupPids) of
        true ->
            case proplists:get_value(auto_reconnect, Opts, false) of
                false -> {stop, {shutdown, Reason}, erase_client(Pid, State)};
                Secs -> reconnect(Secs, erase_client(Pid, State))
            end;
        false ->
            logger:debug("~p received unexpected exit:~0p from ~p. Supervisees: ~p",
                         [?MODULE, Reason, Pid, SupPids]),
            {noreply, State}
    end;

handle_info(reconnect, State = #state{opts = Opts, on_reconnect = OnReconnect}) ->
     case connect_internal(State) of
         {ok, NewState = #state{client = Client}}  ->
             _ = handle_reconnect(Client, OnReconnect),
             {noreply, NewState};
         {Err, _Reason} when Err =:= error orelse Err =:= 'EXIT'  ->
             reconnect(proplists:get_value(auto_reconnect, Opts), State)
     end;

handle_info(Info, State) ->
    logger:error("[PoolWorker] unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id,
                          client = Client,
                          supervisees = SupPids,
                          on_disconnect = Disconnect}) ->
    handle_disconnect(Client, Disconnect),
    %% Kill the supervisees as they may have hung.
    %% Note that the ecpool_worker_sup will shutdown this process after 5s, so we have to
    %% finish the termiation within 5s.
    %% We here wait 0.3s for each supervisee to exit.
    %% We use a short timeout value (0.3s) because the ecpool_worker_sup may have many
    %% workers to be killed, the total time spend by the ecpool_worker_sup will be
    %% (0.3 * NumOfSupervisees * NumOfWorkers) seconds.
    stop_supervisees(SupPids, 300),
    gproc_pool:disconnect_worker(ecpool:name(Pool), {Pool, Id}).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

connect(#state{mod = Mod, opts = Opts, id = Id}) ->
    Mod:connect([{ecpool_worker_id, Id} | connopts(Opts, [])]).

connopts([], Acc) ->
    Acc;
connopts([{Key, _}|Opts], Acc)
  when Key =:= pool_size;
       Key =:= pool_type;
       Key =:= auto_reconnect;
       Key =:= on_reconnect ->
    connopts(Opts, Acc);

connopts([Opt|Opts], Acc) ->
    connopts(Opts, [Opt|Acc]).

reconnect(Secs, State = #state{client = Client, on_disconnect = Disconnect, supervisees = SubPids}) ->
    [erlang:unlink(P) || P <- SubPids, is_pid(P)],
    handle_disconnect(Client, Disconnect),
    erlang:send_after(timer:seconds(Secs), self(), reconnect),
    ecpool_monitor:put_client_global(undefined),
    {noreply, State#state{client = undefined}}.

handle_reconnect(undefined, _) ->
    ok;
handle_reconnect(Client, OnReconnectList) when is_list(OnReconnectList) ->
    lists:foreach(fun(OnReconnectCallback) -> safe_exec(OnReconnectCallback, Client) end,
                  OnReconnectList).

handle_disconnect(undefined, _) ->
    ok;
handle_disconnect(Client, OnDisconnectList) ->
    lists:foreach(fun(OnDisconnectCallback) -> safe_exec(OnDisconnectCallback, Client) end,
                  OnDisconnectList).

connect_internal(State) ->
    try connect(State) of
        {ok, Client} when is_pid(Client) ->
            erlang:link(Client),
            ecpool_monitor:put_client_global(Client),
            {ok, State#state{client = Client, supervisees = [Client]}};
        {ok, Client, #{supervisees := SupPids} = _SupOpts} when is_list(SupPids) ->
            [erlang:link(P) || P <- SupPids],
            ecpool_monitor:put_client_global(Client),
            {ok, State#state{client = Client, supervisees = SupPids}};
        {error, Error} ->
            {error, Error}
    catch
        _C:Reason:ST -> {error, {Reason, ST}}
    end.

-spec reconnect_callback_signature({module(), atom(), list()}) -> {ok, term()} | error.
reconnect_callback_signature({M, _, A}) ->
    try
        {ok, M:get_reconnect_callback_signature(A)}
    catch
        _:_ ->
            error
    end.

is_reconnect_calback_signature_match(CB, Sig) ->
    case reconnect_callback_signature(CB) of
        {ok, Sig2} ->
            Sig2 =:= Sig;
        _ ->
            false
    end.

drop_reconnect_callbacks_by_signature(Callbacks, Signature) ->
    Pred = fun(CB) -> not is_reconnect_calback_signature_match(CB, Signature) end,
    lists:filter(Pred, Callbacks).

safe_exec({_M, _F, _A} = Action, MainArg) ->
    try exec(Action, MainArg)
    catch E:R:ST ->
        logger:error("[PoolWorker] safe_exec ~p, failed: ~0p", [Action, {E,R,ST}]),
        {error, {exec_failed, E, R}}
    end;

%% for backward compatibility upgrading from version =< 4.2.1
safe_exec(Action, MainArg) when is_function(Action) ->
    Action(MainArg).

exec({M, F, A}, MainArg) ->
    erlang:apply(M, F, [MainArg]++A).

ensure_callback(undefined) ->
    [];
ensure_callback({_,_,_} = Callback) ->
    [Callback];
ensure_callback(Callbacks) when is_list(Callbacks) ->
    %% assert
    lists:map(fun({_, _, _} = Callback) -> Callback end, Callbacks).

add_conn_callback(OnReconnect, OldOnReconnects) when is_list(OldOnReconnects) ->
    OldOnReconnects ++ [OnReconnect].

remove_conn_callback({Mod, Fn}, Callbacks) when is_list(Callbacks) ->
    lists:filter(fun({Mod0, Fn0, _Args}) -> {Mod0, Fn0} =/= {Mod, Fn} end, Callbacks).

erase_client(Pid, State = #state{client = Pid, supervisees = SupPids}) ->
    ecpool_monitor:put_client_global(undefined),
    State#state{client = undefined, supervisees = SupPids -- [Pid]};
erase_client(Pid, State = #state{supervisees = SupPids}) ->
    State#state{supervisees = SupPids -- [Pid]}.

stop_supervisees(SubPids, Time) ->
    lists:foreach(fun(Pid) ->
        case try_stop_supervisee(Pid, Time) of
            ok -> ok;
            {error, Reason} when Reason =:= normal; Reason =:= shutdown -> ok;
            {error, killed} ->
                logger:warning("[PoolWorker] supervisee ~p is force killed", [Pid]);
            {error, Reason} ->
                logger:warning("[PoolWorker] supervisee ~p terminated with error: ~p",
                    [Pid, Reason])
        end
    end, SubPids).

%% the 'try_stop_supervisee/2' is a copy of 'shutdown/2' in supervsior.erl
try_stop_supervisee(Pid, Time) ->
    case monitor_child(Pid) of
        ok ->
            exit(Pid, shutdown), %% Try to shutdown gracefully
            receive
                {'DOWN', _MRef, process, Pid, shutdown} ->
                    ok;
                {'DOWN', _MRef, process, Pid, OtherReason} ->
                    {error, OtherReason}
            after Time ->
                exit(Pid, kill), %% Force termination.
                receive
                    {'DOWN', _MRef, process, Pid, OtherReason} ->
                        {error, OtherReason}
                end
            end;
        {error, Reason} ->
            {error, Reason}
    end.

monitor_child(Pid) ->
    %% Do the monitor operation first so that if the child dies
    %% before the monitoring is done causing a 'DOWN'-message with
    %% reason noproc, we will get the real reason in the 'EXIT'-message
    %% unless a naughty child has already done unlink...
    erlang:monitor(process, Pid),
    unlink(Pid),

    receive
        %% If the child dies before the unlik we must empty
        %% the mail-box of the 'EXIT'-message and the 'DOWN'-message.
        {'EXIT', Pid, Reason} ->
            receive
                {'DOWN', _, process, Pid, _} ->
                    {error, Reason}
            end
        after 0 ->
            %% If a naughty child did unlink and the child dies before
            %% monitor the result will be that shutdown/2 receives a
            %% 'DOWN'-message with reason noproc.
            %% If the child should die after the unlink there
            %% will be a 'DOWN'-message with a correct reason
            %% that will be handled in shutdown/2.
            ok
    end.
