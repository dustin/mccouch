-module(mc_couch_kv).

-include("couch_db.hrl").

-export([get/1, set/4, delete/1]).

cleanup(EJson, []) -> EJson;
cleanup(EJson, [Hd|Tl]) -> cleanup(proplists:delete(Hd, EJson), Tl).
cleanup(EJson) ->
    cleanup(EJson, [<<"_id">>, <<"_rev">>, <<"$flags">>, <<"$expiration">>]).

%% ok, Flags, Cas, Data
-spec get(binary()) -> {ok, integer(), integer(), binary()}.
get(Key) ->
    {ok, Db} = couch_db:open(<<"kv">>, []),
    case couch_db:open_doc(Db, Key, []) of
        {ok, Doc} ->
            {EJson} = couch_doc:to_json_obj(Doc, []),
            ?LOG_INFO("Doc keys ~p.", [proplists:get_keys(EJson)]),
            Flags = proplists:get_value(<<"$flags">>, EJson, 0),
            Encoded = iolist_to_binary(couch_util:json_encode({cleanup(EJson)})),
            ?LOG_INFO("MC daemon: got doc ~p:  ~p.", [Key, Encoded]),
            {ok, Flags, 0, Encoded};
        _ -> not_found
    end.

addRev(Db, Key, ToStore) ->
    case couch_db:open_doc(Db, Key, []) of
        {ok, Doc} ->
            {EJson} = couch_doc:to_json_obj(Doc, []),
            [{<<"_rev">>, proplists:get_value(<<"_rev">>, EJson)} | ToStore];
        _ ->
            ToStore
    end.

-spec set(binary(), integer(), integer(), binary()) -> integer().
set(Key, Flags, Expiration, Value) ->
    {EJson} = couch_util:json_decode(Value),
    ?LOG_INFO("set ejson with keys: ~p.", [proplists:get_keys(EJson)]),
    ToStore = [{<<"_id">>, Key},
               {<<"$flags">>, Flags},
               {<<"$expiration">>, Expiration}
               | cleanup(EJson)],

    {ok, Db} = couch_db:open(<<"kv">>, []),

    WithRev = addRev(Db, Key, ToStore),

    Doc = couch_doc:from_json_obj({WithRev}),

    couch_db:update_doc(Db, Doc, []),
    0.

delete(Key) ->
    {ok, Db} = couch_db:open(<<"kv">>, []),
    case couch_db:open_doc(Db, Key, []) of
        {ok, Doc} ->
            {EJson} = couch_doc:to_json_obj(Doc, []),
            DelMe = [{<<"_deleted">>, true},
                     {<<"_id">>, Key},
                     {<<"_rev">>, proplists:get_value(<<"_rev">>, EJson)}],
            couch_db:update_doc(Db,
                                couch_doc:from_json_obj({DelMe}), []),
            ok;
        _ ->
            not_found
    end.
