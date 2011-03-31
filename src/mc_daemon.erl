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

mk_stat(K, V) -> #mc_response{key=K, body=V}.

round_value(Val) when not is_number(Val) ->
    Val;
round_value(Val) when Val == 0 ->
    integer_to_list(Val);
round_value(Val) when is_float(Val) ->
    lists:flatten(io_lib:format("~.6f", [Val]));
round_value(Val) ->
    lists:flatten(io_lib:format("~p", [Val])).

%%    float_to_list(erlang:round(Val * 1000.0) / 1000.0).

emit_stat_prop(_Socket, _Opaque, _Prefix, _Key, null) ->
    ok;
emit_stat_prop(Socket, Opaque, Prefix, Key, Value) ->
    mc_connection:respond(Socket, ?STAT, Opaque,
                          mk_stat(Prefix ++ atom_to_list(Key),
                                  round_value(Value))).

stats_section(Socket, Opaque, {Values}, Prefix, 0) ->
    lists:foreach(fun(StatKey) -> emit_stat_prop(Socket, Opaque, Prefix, StatKey,
                                                 proplists:get_value(StatKey, Values))
                  end, proplists:get_keys(Values));
stats_section(Socket, Opaque, {Values}, Prefix, N) ->
    lists:foreach(fun(StatKey) ->
                          stats_section(Socket, Opaque,
                                        proplists:get_value(StatKey, Values),
                                        Prefix ++ atom_to_list(StatKey) ++ ":", N - 1)
                  end, proplists:get_keys(Values)).


handle_cast({?STAT, _Extra, _Key, _Body, _CAS, Socket, Opaque}, State) ->
    {AllStats} = couch_stats_aggregator:all(),
    ?LOG_INFO("Got stats:  ~p~n", [proplists:get_keys(AllStats)]),
    stats_section(Socket, Opaque, {AllStats}, "", 2),
    mc_connection:respond(Socket, ?STAT, Opaque, mk_stat("", "")),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
     {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
