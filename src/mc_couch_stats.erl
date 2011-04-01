-module(mc_couch_stats).

-include("couch_db.hrl").
-include("mc_constants.hrl").

-export([stats/2]).

mk_stat(K, V) -> #mc_response{key=K, body=V}.

round_value(Val) when not is_number(Val) ->
    Val;
round_value(Val) when Val == 0 ->
    integer_to_list(Val);
round_value(Val) when is_float(Val) ->
    lists:flatten(io_lib:format("~.6f", [Val]));
round_value(Val) ->
    lists:flatten(io_lib:format("~p", [Val])).

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

stats(Socket, Opaque) ->
    {AllStats} = couch_stats_aggregator:all(),
    stats_section(Socket, Opaque, {AllStats}, "", 2),
    mc_connection:respond(Socket, ?STAT, Opaque, mk_stat("", "")).
