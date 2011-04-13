-module(mc_daemon).

-behaviour(gen_server).

%% API
-export([start_link/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% Kind of ugly to export these, but modularity win.
-export([with_open_db/3, db_name/2, db_prefix/1]).

-define(SERVER, ?MODULE).

-include("couch_db.hrl").
-include("mc_constants.hrl").

-record(state, {mc_serv, db, json_mode}).

start_link(DbName, JsonMode) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [DbName, JsonMode],
                          []).

init([DbName, JsonMode]) ->
    ?LOG_INFO("MC daemon: starting: json_mode=~p.", [JsonMode]),
    {ok, S} = mc_tcp_listener:start_link(11213, self()),
    {ok, #state{mc_serv=S, db=list_to_binary(DbName), json_mode=JsonMode}}.

db_name(VBucket, State)->
    lists:flatten(io_lib:format("~s/~p", [State#state.db, VBucket])).

db_prefix(State) -> State#state.db.

with_open_db(F, VBucket, State) ->
    {ok, Db} = couch_db:open(list_to_binary(db_name(VBucket, State)), []),
    NewState = F(Db),
    couch_db:close(Db),
    NewState.

handle_get_call(Db, Key) ->
    case mc_couch_kv:get(Db, Key) of
        {ok, Flags, Cas, Data} ->
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

handle_call({?GET, VBucket, <<>>, Key, <<>>, _CAS}, _From, State) ->
    error_logger:info_msg("Got GET command for ~p.~n", [Key]),
    with_open_db(fun(Db) -> {reply, handle_get_call(Db, Key), State} end,
                 VBucket, State);
handle_call({?SET, VBucket, <<Flags:32, Expiration:32>>, Key, Value, _CAS},
            _From, State) ->
    with_open_db(fun(Db) -> {reply, handle_set_call(Db, Key, Flags,
                                                    Expiration, Value,
                                                    State#state.json_mode),
                             State}
                 end, VBucket, State);
handle_call({?DELETE, VBucket, <<>>, Key, <<>>, _CAS}, _From, State) ->
    with_open_db(fun(Db) -> {reply, handle_delete_call(Db, Key), State} end,
                 VBucket, State);
handle_call({?DELETE_BUCKET, 0, <<>>, Key, <<>>, 0}, _From, State) ->
    delete_db(Key),
    {reply, #mc_response{body="Done!"}, State};
handle_call({?SET_VBUCKET_STATE, VBucket, <<>>, <<>>, <<VBState:32>>, 0}, _From, State) ->
    mc_couch_vbucket:handle_set_state(VBucket, VBState, State);
handle_call({?DELETE_VBUCKET, VBucket, <<>>, <<>>, <<>>, 0}, _From, State) ->
    {reply, mc_couch_vbucket:handle_delete(VBucket, State), State};
handle_call({OpCode, VBucket, Header, Key, Body, CAS}, _From, State) ->
    ?LOG_INFO("MC daemon: got unhandled call: ~p/~p/~p/~p/~p/~p.",
               [OpCode, VBucket, Header, Key, Body, CAS]),
    {reply, #mc_response{status=?UNKNOWN_COMMAND, body="WTF, mate?"}, State};
handle_call(Request, _From, State) ->
    ?LOG_DEBUG("MC daemon: got call.", [Request]),
    Reply = ok,
    {reply, Reply, State}.

handle_cast({?STAT, _Extra, <<"vbucket">>, _Body, _CAS, Socket, Opaque}, State) ->
    mc_couch_vbucket:handle_stats(Socket, Opaque, State),
    {noreply, State};
handle_cast({?STAT, _Extra, _Key, _Body, _CAS, Socket, Opaque}, State) ->
    mc_couch_stats:stats(Socket, Opaque),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
