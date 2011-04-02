-module(mc_daemon).

-behaviour(gen_server).

%% API
-export([start_link/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-include("couch_db.hrl").
-include("mc_constants.hrl").

-record(state, {mc_serv, db}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ?LOG_INFO("MC daemon: starting.", []),
    {ok, S} = mc_tcp_listener:start_link(11213, self()),
    {ok, Db} = couch_db:open(<<"kv">>, []),
    {ok, #state{mc_serv=S, db=Db}}.

handle_call({?GET, <<>>, Key, <<>>, _CAS}, _From, State) ->
    error_logger:info_msg("Got GET command for ~p.~n", [Key]),
    case mc_couch_kv:get(State#state.db, Key) of
        {ok, Flags, Cas, Data} ->
            FlagsBin = <<Flags:32>>,
            {reply,
             #mc_response{extra=FlagsBin, cas=Cas, body=Data},
             State};
        _ ->
            {reply, #mc_response{status=1, body="Does not exist"}, State}
    end;
handle_call({?SET, <<Flags:32, Expiration:32>>, Key, Value, _CAS},
            _From, State) ->
    NewCas = mc_couch_kv:set(State#state.db, Key, Flags, Expiration, Value),
    {reply, #mc_response{cas=NewCas}, State};
handle_call({?DELETE, <<>>, Key, <<>>, _CAS}, _From, State) ->
    case mc_couch_kv:delete(State#state.db, Key) of
        ok ->
            {reply, #mc_response{}, State};
        not_found ->
            {reply, #mc_response{status=1, body="Not found"}, State}
    end;
handle_call({_OpCode, _Header, _Key, _Body, _CAS}, _From, State) ->
    {reply, #mc_response{status=?UNKNOWN_COMMAND, body="WTF, mate?"}, State};
handle_call(Request, _From, State) ->
    ?LOG_DEBUG("MC daemon: got call.", [Request]),
    Reply = ok,
    {reply, Reply, State}.

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
