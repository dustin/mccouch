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

-record(state, {mc_serv}).

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

init([]) ->
    ?LOG_INFO("MC daemon: starting.", []),
    {ok, S} = mc_tcp_listener:start_link(11213, self()),
    {ok, #state{mc_serv=S}}.

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
