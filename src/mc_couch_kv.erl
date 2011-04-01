-module(mc_couch_kv).

-include("couch_db.hrl").

-export([get/1]).

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

