-module(mc_daemon).

-behaviour(gen_fsm).

%% API
-export([start_link/2]).

%% gen_fsm callbacks
-export([init/1, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).

%% My states
-export([processing/2, processing/3]).

%% Kind of ugly to export these, but modularity win.
-export([with_open_db/3, db_name/2, db_prefix/1]).

-define(SERVER, ?MODULE).

-include("couch_db.hrl").
-include("mc_constants.hrl").

-record(state, {mc_serv, db, json_mode, setqs=0}).

start_link(DbName, JsonMode) ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [DbName, JsonMode], []).

init([DbName, JsonMode]) ->
    ?LOG_INFO("MC daemon: starting: json_mode=~p.", [JsonMode]),
    {ok, S} = mc_tcp_listener:start_link(11213, self()),
    {ok, processing,
     #state{mc_serv=S, db=list_to_binary(DbName), json_mode=JsonMode}}.

db_name(VBucket, State)->
    iolist_to_binary([State#state.db, $/, integer_to_list(VBucket)]).

db_prefix(State) -> State#state.db.

with_open_db(F, VBucket, State) ->
    {ok, Db} = couch_db:open(db_name(VBucket, State), []),
    NewState = F(Db),
    couch_db:close(Db),
    NewState.

handle_get_call(Db, Key) ->
    case mc_couch_kv:get(Db, Key) of
        {ok, Flags, _Expiration, Cas, Data} ->
            FlagsBin = <<Flags:32>>,
            #mc_response{extra=FlagsBin, cas=Cas, body=Data};
        _ ->
            #mc_response{status=1, body="Does not exist"}
    end.

handle_set_call(Db, Key, Flags, Expiration, Value, JsonMode) ->
    NewCas = mc_couch_kv:set(Db,
                             Key, Flags,
                             Expiration, Value,
                             JsonMode),
    #mc_response{cas=NewCas}.

handle_setq_call(VBucket, Key, Flags, Expiration, Value, _CAS, Opaque, Socket, State) ->
    spawn_link(fun() ->
                      with_open_db(fun(Db) ->
                                           case catch(mc_couch_kv:set(Db, Key,
                                                                      Flags,
                                                                      Expiration,
                                                                      Value,
                                                                      State#state.json_mode)) of
                                               {ok, _} -> ok;
                                               _Error ->
                                                   %% TODO:  You heard the comment
                                                   do_something_about_this
                                           end,
                                           gen_server:cast(?MODULE, {setq_complete,
                                                                     Opaque, %% opaque
                                                                     Socket,
                                                                     0})
                                   end, VBucket, State)
              end),
    %% TODO:  Put the actual opaque here
    State#state{setqs=State#state.setqs + 1}.

handle_delete_call(Db, Key) ->
    case mc_couch_kv:delete(Db, Key) of
        ok -> #mc_response{};
        not_found -> #mc_response{status=1, body="Not found"}
    end.

delete_db(Key) ->
    lists:foreach(fun(N) ->
                          DbName = lists:flatten(io_lib:format("~s/~p",
                                                               [Key, N])),
                          couch_server:delete(list_to_binary(DbName), [])
                  end, lists:seq(0, 1023)).

processing({?GET, VBucket, <<>>, Key, <<>>, _CAS}, _From, State) ->
    with_open_db(fun(Db) -> {reply, handle_get_call(Db, Key), processing, State} end,
                 VBucket, State);
processing({?GET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?SET, VBucket, <<Flags:32, Expiration:32>>, Key, Value, _CAS},
           _From, State) ->
    with_open_db(fun(Db) -> {reply, handle_set_call(Db, Key, Flags,
                                                    Expiration, Value,
                                                    State#state.json_mode),
                             processing, State}
                 end, VBucket, State);
processing({?SET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?NOOP, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{}, processing, State};
processing({?DELETE, VBucket, <<>>, Key, <<>>, _CAS}, _From, State) ->
    with_open_db(fun(Db) -> {reply, handle_delete_call(Db, Key), processing, State} end,
                 VBucket, State);
processing({?DELETE, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?DELETE_BUCKET, _VBucket, <<>>, Key, <<>>, 0}, _From, State) ->
    delete_db(Key),
    {reply, #mc_response{body="Done!"}, processing, State};
processing({?DELETE_BUCKET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?SELECT_BUCKET, _VBucket, <<>>, Name, <<>>, 0}, _From, State) ->
    {reply, #mc_response{}, processing, State#state{db=Name}};
processing({?SELECT_BUCKET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?SET_VBUCKET_STATE, VBucket, <<VBState:32>>, <<>>, <<>>, 0}, _From, State) ->
    mc_couch_vbucket:handle_set_state(VBucket, VBState, State);
processing({?SET_VBUCKET_STATE, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({?DELETE_VBUCKET, VBucket, <<>>, <<>>, <<>>, 0}, _From, State) ->
    {reply, mc_couch_vbucket:handle_delete(VBucket, State), processing, State};
processing({?DELETE_VBUCKET, _, _, _, _, _}, _From, State) ->
    {reply, #mc_response{status=?EINVAL}, processing, State};
processing({OpCode, VBucket, Header, Key, Body, CAS}, _From, State) ->
    ?LOG_INFO("MC daemon: got unhandled call: ~p/~p/~p/~p/~p/~p.",
               [OpCode, VBucket, Header, Key, Body, CAS]),
    {reply, #mc_response{status=?UNKNOWN_COMMAND, body="WTF, mate?"}, processing, State}.

processing({?STAT, _Extra, <<"vbucket">>, _Body, _CAS, Socket, Opaque}, State) ->
    mc_couch_vbucket:handle_stfats(Socket, Opaque, State),
    {next_state, processing, State};
processing({?TAP_CONNECT, Extra, _Key, Body, _CAS, Socket, Opaque}, State) ->
    mc_tap:run(State#state.db, Opaque, Socket, Extra, Body),
    {next_state, processing, State};
processing({?STAT, _Extra, _Key, _Body, _CAS, Socket, Opaque}, State) ->
    mc_couch_stats:stats(Socket, Opaque),
    {next_state, processing, State};
processing({?SETQ, VBucket, <<Flags:32, Expiration:32>>, Key, Value,
             CAS, Socket, Opaque}, State) ->
    {next_state, processing, handle_setq_call(VBucket, Key, Flags, Expiration, Value,
                                              CAS, Opaque, Socket, State)};
%% non-protocol below
processing({setq_complete, Opaque, _Socket, _Status}, State) ->
    ?LOG_INFO("Completed a setq: ~p, now have ~p", [Opaque, State#state.setqs - 1]),
    {next_state, processing, State#state{setqs=State#state.setqs - 1}};
processing(_Msg, State) ->
    {noreply, processing, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
