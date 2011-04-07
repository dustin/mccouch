-module(mc_couch_kv).

-include("couch_db.hrl").

-export([get/2, set/5, delete/2]).

%% ok, Flags, Cas, Data
-spec get(_, binary()) -> {ok, integer(), integer(), binary()}.
get(Db, Key) ->
    case couch_db:open_doc(Db, Key, []) of
        {ok, Doc} ->
            {EJson} = couch_doc:to_json_obj(Doc, []),
            ?LOG_INFO("Doc keys ~p.", [proplists:get_keys(EJson)]),
            Flags = proplists:get_value(<<"$flags">>, EJson, 0),
            Encoded = iolist_to_binary(couch_util:json_encode(
                                         {mc_couch_util:cleanup(EJson)})),
            ?LOG_INFO("MC daemon: got doc ~p:  ~p.", [Key, Encoded]),
            {ok, Flags, 0, Encoded};
        _ -> not_found
    end.

-spec set(_, binary(), integer(), integer(), binary()) -> integer().
set(Db, Key, Flags, Expiration, Value) ->
    {EJson} = couch_util:json_decode(Value),
    ?LOG_INFO("set ejson with keys: ~p.", [proplists:get_keys(EJson)]),
    ToStore = [{<<"_id">>, Key},
               {<<"$flags">>, Flags},
               {<<"$expiration">>, Expiration}
               | mc_couch_util:cleanup(EJson)],

    WithRev = mc_couch_util:addRev(Db, Key, ToStore),

    Doc = couch_doc:from_json_obj({WithRev}),

    couch_db:update_doc(Db, Doc, []),
    0.

delete(Db, Key) -> mc_couch_util:delete(Db, Key).
